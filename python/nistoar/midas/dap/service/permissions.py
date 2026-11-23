"""Resolve DAP collaborators to users and synchronize their file permissions."""
from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils.prov import Agent
from ...dbio.base import (
    ACLs, ANONYMOUS, AUTOADMIN, GROUPS_COLL, PUBLIC_GROUP, ORG_FIELDS,
    organization_subjects,
)
from ...dbio.status import EDIT


def record_acls(record):
    """Return the record's standard ACLs as sets for permission comparisons."""
    # Take a snapshot of the record's four standard access lists.
    #
    # The result looks like:
    # {
    #     "read": {"mnm16", "grp0:owner:team"},
    #     "write": {"mnm16"},
    #     "admin": {"owner"},
    #     "delete": {"owner"},
    # }
    #
    # Sets remove duplicates and make comparisons straightforward.
    return {
        perm: set(record.acls.iter_perm_granted(perm))
        for perm in ACLs.OWN
    }


def review_file_role(record):
    """ Select the file-permission limit for a record with saved review ACLs."""
    # The review workflow saves the original ACLs under names such as "_write".
    # Their presence tells this helper to apply the review permission limit.
    saved = any(
        set(record.acls.iter_perm_granted("_" + perm))
        for perm in ACLs.OWN
    )

    if not saved:
        # There are no saved review ACLs, so apply the normal permission levels.
        return None

    # With saved review ACLs:
    # - an editing record allows up to the existing FM "Write" level;
    # - other states allow up to the existing FM "Read" level.
    return "Write" if record.status.state == EDIT else "Read"


class PermissionResolver:
    # Prepare group overrides and an empty directory cache for this resolver.
    def __init__(self, dbclient, group_overrides=None):
        self.db = dbclient

        # Use the pending member list for a group being edited.
        # Other groups are read from the database when needed.
        self.groups = dict(group_overrides or {})

        # Each request gets a fresh resolver and current directory data.
        self.people = None

    def members(self, subject, seen=frozenset()):
        """Expand a user or group subject into usernames, avoiding group cycles."""
        if subject in (ANONYMOUS, PUBLIC_GROUP):
            return set()

        # A subject without ":" is already an individual username.
        if ":" not in subject:
            return {subject}

        # Groups can contain other groups.
        # Stop following a branch if it leads back to a group already visited.
        #
        # For example:
        # Team A contains Team B, and Team B contains Team A.
        # Without this check, resolving their membership would never finish.
        if subject in seen:
            return set()

        prefix = subject.split(":", 1)[0]

        if prefix in ORG_FIELDS:
            # This is an organizational subject, such as "nistdiv:123".
            people_service = self.db.people_service
            if people_service is None:
                raise ConfigurationException(
                    "Organizational sharing requires NSD"
                )

            if self.people is None:
                # Read the directory once for this resolver.
                self.people = list(people_service.select_people({}))

                # Treat a completely empty directory as unavailable.
                # Otherwise an unavailable directory could look like every organizational group had lost all of its members.
                if not self.people:
                    raise ConfigurationException("NSD contains no people")

            users = set()

            for person in self.people:
                # Compare the complete organization subject.
                # "nistdiv:12" therefore does not match "nistdiv:123".
                if subject in organization_subjects(person):
                    username = person.get(people_service.EID_PROP)

                    # FM and Windows permissions need the person's username.
                    # Stop if a matching directory entry cannot provide one.
                    if not username:
                        raise ConfigurationException(
                            f"NSD member of {subject} has no username"
                        )

                    users.add(str(username))

            # An organization with no matching people produces an empty set.
            return users

        # Other namespaced subjects have no expansion in this proposal.
        if prefix != "grp0":
            return set()

        # This is a custom MIDAS group.
        # Read its stored members once while using this resolver.
        if subject not in self.groups:
            self.groups[subject] = self.db._get_from_coll(
                GROUPS_COLL, subject
            )

        group = self.groups[subject]

        # A deleted or deactivated group supplies no members.
        if not group or group.get("deactivated"):
            return set()

        users = set()

        for member in group.get("members", []):
            # Resolve each member in the same way:
            # an individual contributes one username;
            # another group contributes its resolved members.
            #
            # Add this group to the visited set before following its members.
            users.update(self.members(member, seen | {subject}))

        return users

    def permissions(self, acls, maximum=None):
        """Resolve each user's strongest ACL grant and apply an optional review limit."""
        # Build one final permission level for each individual.
        #
        # A person can appear directly in an ACL and through several groups.
        # We process the levels from weakest to strongest, so the strongest available grant becomes that person's final level.
        result = {}

        # Later entries replace weaker access received from another ACL or group.
        for perm, label in (
            (ACLs.READ, "Read"),
            (ACLs.WRITE, "Write"),
            (ACLs.DELETE, "Delete"),
            (ACLs.ADMIN, "All"),
        ):
            for subject in acls.get(perm, ()):
                for username in self.members(subject):
                    result[username] = label

        # Apply the review limit when one was supplied.
        if maximum == "Read":
            return {username: "Read" for username in result}

        if maximum == "Write":
            # Existing Read grants stay Read.
            # Stronger grants are limited to Write.
            return {
                username: "Read" if label == "Read" else "Write"
                for username, label in result.items()
            }

        return result

def synchronize_record(service, record, desired, previous=None, users=None):
    """
    Apply one record's desired FM permissions while preserving protected users.
    "desired" contains each person's required file permission.
    Record ACL updates supply their earlier permission map as "previous".
    "users" restricts the operation to particular people when supplied.
    """
    fm = service._fmcli

    # No FM client means file-manager integration is not configured.
    # An explicitly empty user set means there is nothing to update.
    if fm is None or users == set():
        return

    # Ask FM for the permissions it currently reports for its known users.
    current = fm.get_space_permissions(record.id)

    if not isinstance(current, dict):
        raise RuntimeError("FM did not return a permissions mapping")

    # Preserve the record owner and the original file-space creator.
    # The existing FM implementation protects the creator from removal.
    protected = {
        record.owner,
        (record.file_space or {}).get("creator"),
    }

    if users is None:
        # Include people who used to have access as well as people who should have access now.
        users = set(current) | set(desired) | set(previous or {})

    removals = {}
    grants = {}

    for username in sorted(users - protected):
        # Absence from the desired mapping means no remaining access.
        wanted = desired.get(username, "None")
        actual = current.get(username, "None")

        if (
            previous is not None
            and actual == wanted
            and previous.get(username, "None") == wanted
        ):
            continue

        if (
            wanted == "None"
            or actual != "None"
            or previous is None
            or previous.get(username, "None") != "None"
        ):
            removals[username] = "None"

        if wanted != "None":
            grants[username] = wanted

    for update in (removals, grants):
        if update:
            result = fm.set_space_permissions(record.id, update)

            # Verify that FM reports the requested permission labels.
            if not isinstance(result, dict) or any(
                result.get(username) != permission
                for username, permission in update.items()
            ):
                raise RuntimeError(
                    "FM did not apply the requested permissions"
                )

def synchronize_records(app, group_id=None, group_overrides=None):
    """Synchronize eligible DAP records, optionally limiting them to a changed group."""
    from .mds3 import DAPApp

    # Use the internal administrator identity to handle records across owners.
    who = Agent("file-permission-sync", Agent.USER, AUTOADMIN)
    # Several paths can mount the same subapp; process each instance only once.
    subapps = {
        id(subapp): subapp
        for subapp in app.subapps.values()
    }

    # Only DAP services participate in this file-permission synchronization.
    for subapp in subapps.values():
        if not isinstance(subapp, DAPApp):
            continue

        # Create an administrative service and skip it if FM is not configured.
        service = subapp.svcfact.create_service_for(who)
        if service._fmcli is None:
            continue

        # Apply any supplied pending group memberships when resolving access.
        resolver = PermissionResolver(service.dbcli, group_overrides)
        # Without a group filter, synchronize every eligible record.
        affected = None
        # Include parent groups so nested membership changes reach their records.
        if group_id is not None:
            affected = {group_id}
            affected.update(
                service.dbcli.groups.select_ids_for_user(group_id)
            )
            # Public membership does not identify records affected by this group edit.
            affected.discard(PUBLIC_GROUP)

        # Inspect the active records in this DAP service's project collection.
        records = service.dbcli._select_from_coll(service.dbcli.project)
        for data in records:
            # Records without an initialized file space have no FM access to update.
            if not (data.get("file_space") or {}).get("creator"):
                continue

            record = service.get_record(data["id"])
            acls = record_acls(record)
            # For a group edit, keep only records shared with it or a parent group.
            if affected is not None and not any(
                affected.intersection(subjects)
                for subjects in acls.values()
            ):
                continue

            # Combine direct and group grants, then apply the current review limit.
            desired = resolver.permissions(acls, review_file_role(record))
            # Reconcile existing FM users too, so obsolete access can be removed.
            synchronize_record(service, record, desired)
