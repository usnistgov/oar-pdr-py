"""
A web service interface to manage MIDAS Groups.

A group record is a special record that can be used to grant access to multiple users to other records.  Each
group is owned by a single user (the one who created the group), but group membership can be updated later.

The :py:class:`MIDASGroupApp` encapsulates the handling of requests to create and manipulate group
records.

This implementation uses the simple :py:mod:`nistoar-internal WSGI framework<nistoar.pdr.publish.service.wsgi>` to handle the specific web service endpoints.  The
:py:class:`MIDASGroupApp` is the router for the Group collection endpoint: it analyzes the relative
URL path and delegates the handling to a more specific handler class.  In particular, these endpoints
are handled accordingly:

``/`` -- :py:class:`GroupSelectionHandler`
    creates a new group (POST), returning the newly created record; the user must supply JSON including at least the "name".

``/<group_id>`` -- :py:class:`GroupHandler`
    returns the entire group record (GET)
    adds one or more user or group IDs to the membership (POST)
    overwrites the entire membership list (PUT)
    deletes the group (DELETE).

``/<group_id>/<user>`` -- :py:class:`GroupHandler`
    removes a single user (or group) from the membership list (DELETE).
"""
from collections.abc import Callable, Mapping
from logging import Logger

from .base import DBIOHandler
from nistoar.web.rest import ServiceApp, Handler, Agent
from nistoar.midas.dbio import NotAuthorized, ObjectNotFound, AlreadyExists, InvalidUpdate
from nistoar.midas.dbio import ACLs, DBClientFactory, ANONYMOUS
from nistoar.midas.dbio.base import Group, DBGroups

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class GroupService:
    """
    A service class that wraps basic group operations provided by the Group class
    """
    def __init__(self, dbclient, log: Logger = None):
        self._cli = dbclient
        self._groups: DBGroups = dbclient.groups
        self.log = log

    def create_group(self, name: str, foruser: str = None) -> Group:
        """
        Use DBGroups.create_group to create a new group.

        :param str name:     the name of the group to create
        :param str foruser:  the identifier of the user to create the group for.  This user will be set as
                             the group's owner/administrator.  If not given, the user attached to the
                             underlying :py:class:`DBClient` will be used.
        :raises AlreadyExists:  if the user has already defined a group with this name
        :raises NotAuthorized:  if the user is not authorized to create this group
        :return: the newly created Group object
        """
        return self._groups.create_group(name, foruser)

    def get_group(self, group_id: str) -> Group:
        """
        return the group by ID.

        :param group_id: the group ID to retrieve
        :return: a Group object
        :raises NotAuthorized, ObjectNotFound
        """
        grp = self._groups.get(group_id)
        if not grp:
            raise ObjectNotFound(group_id)
        return grp

    def delete_group(self, group_id: str) -> bool:
        """
        Delete an existing group. Requires DELETE permission.

        :return: True if the group was deleted, False otherwise.
        """
        return self._groups.delete_group(group_id)

    def add_members(self, group_id: str, member_ids: list[str]) -> list[str]:
        """
        Add one or more user (or group) IDs to this group's membership.

        :return: the updated membership list
        """
        grp = self.get_group(group_id)
        grp.add_member(*member_ids)
        grp.save()
        return grp._data["members"]

    def replace_members(self, group_id: str, member_ids: list[str]) -> list[str]:
        """
        Replace the group's entire membership with the given IDs.

        :return: the updated membership list
        """
        grp = self.get_group(group_id)
        # Clear the existing list:
        existing = list(grp._data["members"])
        for old in existing:
            if old in grp._data["members"]:
                grp._data["members"].remove(old)
        # Add the new ones
        grp.add_member(*member_ids)
        grp.save()
        return grp._data["members"]

    def remove_member(self, group_id: str, member_id: str) -> bool:
        """
        Remove a single user (or group) ID from this group's membership.

        :return: True if the user was removed from membership, False if no change
        """
        grp = self.get_group(group_id)
        if member_id not in grp._data["members"]:
            return False
        grp.remove_member(member_id)
        grp.save()
        return True


class GroupServiceFactory:
    """
    Factory to build GroupService for a specific user (via a DBClient).
    """

    def __init__(self, dbcli_factory: DBClientFactory, config: dict, log: Logger = None):
        self.dbcli_factory = dbcli_factory
        self.config = config
        self.log = log

    def create_service_for(self, who: Agent) -> GroupService:
        """
        Build a GroupService for the given user identity.
        """
        dbcli = self.dbcli_factory.create_client("groups", config=self.config, foruser=who.actor)
        return GroupService(dbcli, self.log)


class GroupSelectionHandler(DBIOHandler):
    """
    Handles collection-level requests (no specific group ID).
    """

    def __init__(self, svcapp: ServiceApp, svc: GroupService, wsgienv: dict, start_resp: Callable,
                 who: Agent, shoulder: str, config: dict = None, log: Logger = None):
        super().__init__(svcapp, None, wsgienv, start_resp, who, shoulder, config, log)
        self.svc = svc
        self._shoulder = shoulder

    def do_OPTIONS(self, path):
        return self.send_options(["POST"])

    def do_POST(self, path):
        """
        Create a new group. Expected JSON input:
        {
            "name": "<mnemonic name>",
            "foruser": "optional owner id"
        }
        """
        try:
            data = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if not isinstance(data, dict):
            return self.send_error_resp(400, "Invalid JSON",
                                        "Expected an object with at least 'name'")

        name = data.get("name", "").strip()
        if not name:
            return self.send_error_resp(400, "Missing name property",
                                        "Cannot create group without 'name'")

        foruser = data.get("foruser", None)
        try:
            grp = self.svc.create_group(name, foruser)
            return self.send_json(grp.to_dict(), "Group Created", 201)
        except AlreadyExists as ex:
            return self.send_error_resp(400, "Already Exists", str(ex))
        except NotAuthorized as ex:
            return self.send_unauthorized()


class GroupHandler(DBIOHandler):
    """
    Handles operations on a specific group ID, plus optional subpath. Endpoints:

    GET     /<shoulder>/<group_id>
    POST    /<shoulder>/<group_id>               -> add members
    PUT     /<shoulder>/<group_id>               -> replace membership
    DELETE  /<shoulder>/<group_id>               -> delete group
    DELETE  /<shoulder>/<group_id>/<member_id>   -> remove a single member
    """

    def __init__(self, svcapp: ServiceApp, svc: GroupService, wsgienv: dict, start_resp: Callable,
                 who: Agent, shoulder: str, path: str, config: dict = None, log: Logger = None):
        super().__init__(svcapp, None, wsgienv, start_resp, who, path, config, log)
        self.svc = svc
        self._shoulder = shoulder
        self._full_path = path.strip('/')
        parts = self._full_path.split('/', 1)
        self._group_id = parts[0] if parts else None
        self._extra = parts[1] if len(parts) > 1 else ""

        if not self._group_id:
            raise ValueError("Missing group_id from path")

    def do_OPTIONS(self, path):
        """
        If subpath is present => allow DELETE
        else => GET, POST, PUT, DELETE
        """
        if self._extra:
            return self.send_options(["DELETE"])
        return self.send_options(["GET", "POST", "PUT", "DELETE"])

    def do_GET(self, path, ashead=False):
        """
        GET /<shoulder>/<group_id> => fetch the entire group record
        """
        if self._extra:
            return self.send_error_resp(404, "Not Found",
                                        f"Unknown subresource: {self._extra}", ashead=ashead)
        try:
            grp = self.svc.get_group(self._group_id)
            return self.send_json(grp.to_dict(), ashead=ashead)
        except NotAuthorized:
            return self.send_unauthorized()
        except ObjectNotFound:
            return self.send_error_resp(404, "Group not found",
                                        f"No group found for id={self._group_id}",
                                        ashead=ashead)

    def do_POST(self, path):
        """
        POST /<shoulder>/<group_id> => add members to this group
        Body can be:
          ["member1", "member2"]   or   "member3"
        """
        if self._extra:
            return self.send_error_resp(400, "Cannot POST with subpath", self._extra)

        try:
            body = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        # accept a single string or a list of strings
        if isinstance(body, str):
            members_to_add = [body]
        elif isinstance(body, list):
            members_to_add = body
        else:
            return self.send_error_resp(400, "Invalid JSON input",
                                        "Expected a JSON list or a string for members")

        try:
            updated = self.svc.add_members(self._group_id, members_to_add)
            return self.send_json(updated, "Members added")
        except NotAuthorized:
            return self.send_unauthorized()
        except ObjectNotFound:
            return self.send_error_resp(404, "Group not found",
                                        f"No group found for id={self._group_id}")

    def do_PUT(self, path):
        """
        PUT /<shoulder>/<group_id> => replace the group's membership
        Body can be:
          ["member1","member2"] or a single string
        """
        if self._extra:
            return self.send_error_resp(400, "Cannot PUT with subpath", self._extra)

        try:
            body = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if isinstance(body, str):
            new_members = [body]
        elif isinstance(body, list):
            new_members = body
        else:
            return self.send_error_resp(400, "Invalid JSON input",
                                        "Expected a JSON list or a string for membership")

        try:
            replaced = self.svc.replace_members(self._group_id, new_members)
            return self.send_json(replaced, "Membership replaced")
        except NotAuthorized:
            return self.send_unauthorized()
        except ObjectNotFound:
            return self.send_error_resp(404, "Group not found",
                                        f"No group found for id={self._group_id}")

    def do_DELETE(self, path):
        """
        DELETE /<shoulder>/<group_id> => delete entire group
        DELETE /<shoulder>/<group_id>/<member> => remove one member from group
        """
        if not self._extra:
            # no subpath => delete the group
            try:
                deleted = self.svc.delete_group(self._group_id)
                if not deleted:
                    return self.send_error_resp(404, "Group not found",
                                                f"No group found for id={self._group_id}")
                return self.send_ok("Group deleted")
            except NotAuthorized:
                return self.send_unauthorized()
        else:
            # subpath => remove that one member
            member_id = self._extra
            try:
                removed = self.svc.remove_member(self._group_id, member_id)
                if removed:
                    return self.send_ok(f"Removed {member_id} from group {self._group_id}")
                else:
                    return self.send_ok(f"No change; {member_id} was not a member.")
            except NotAuthorized:
                return self.send_unauthorized()
            except ObjectNotFound:
                return self.send_error_resp(404, "Group not found",
                                            f"No group found for id={self._group_id}")


class MIDASGroupApp(ServiceApp):
    """
    a base web app for an interface handling group record.
    """

    def __init__(self, svc_factory: GroupServiceFactory, log: Logger, config: dict = None):
        super().__init__("midas-group", log, config)
        self.svc_factory = svc_factory

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        Route the request to the appropriate Handler based on path structure:
         - /<shoulder> only => GroupSelectionHandler
         - /<shoulder>/<group_id> => GroupHandler
        """
        svc = self.svc_factory.create_service_for(who)

        path = path.strip('/')
        if not path:
            return GroupSelectionHandler(
                self,  # parent ServiceApp
                svc,  # GroupService
                env, start_resp, who,
                "",  # shoulder (empty)
                self.cfg, self.log)

        parts = path.split('/', 1)
        shoulder = parts[0]
        subpath = parts[1] if len(parts) > 1 else ""

        logger.debug(f"subpath {str(subpath)}")

        if not subpath:
            # => /<shoulder> with no further subpath => create group
            return GroupSelectionHandler(self, svc, env, start_resp, who, shoulder,
                                         self.cfg, self.log)
        else:
            # => /<shoulder>/<group_id>...
            return GroupHandler(self, svc, env, start_resp, who, shoulder, subpath,
                                self.cfg, self.log)

    class _factory:
        """
        Callable class that the main WSGI can use as a factory.
        """
        def __init__(self):
            pass

        def __call__(self, dbcli_factory: DBClientFactory, log: Logger, config: dict = None,
                     project_coll: str = None):
            if config is None:
                config = {}
            svc_factory = GroupServiceFactory(dbcli_factory, config, log)
            return MIDASGroupApp(svc_factory, log, config)

    @classmethod
    def factory_for(cls):
        """
        Return a function that can create a MIDASGroupApp.
        """
        return cls._factory()

