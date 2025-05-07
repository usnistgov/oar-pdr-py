"""
The abstract interface for interacting with the database.  

This interface is based on the following model:

  *  Each service (DAP, DMPs, etc.) has its own collection that extends on a common base model
  *  Each *record* in the collection represents a "project" that a user is working on via the service
  *  A record can be expressed as a Python dictionary which can be exported into JSON

See the :py:mod:`DBIO package documentation <nistoar.midas.dbio>` for a fully description of the 
model and how to interact with the database.
"""
import time
import math
import logging
from abc import ABC, ABCMeta, abstractmethod, abstractproperty
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from collections import OrderedDict
from typing import Union, List, Sequence, AbstractSet, MutableSet, NewType, Iterator
from enum import Enum
from datetime import datetime

from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils.prov import Action
from .. import MIDASException
from .status import RecordStatus
from .notifier import Notifier
from nistoar.pdr.utils.prov import ANONYMOUS_USER
from nistoar.pdr.utils.validate import ValidationResults, ALL
from nistoar.nsd.service import PeopleService, create_people_service

DAP_PROJECTS = "dap"
DMP_PROJECTS = "dmp"
GROUPS_COLL = "groups"
PEOPLE_COLL = "people"     # this does not refer to the staff directory database
DRAFT_PROJECTS = "draft"   # this name is deprecated
PROV_ACT_LOG = "prov_action_log"
_AUTHDEL = "_authdel"

DEF_PEOPLE_SHOULDER = "ppl0"
DEF_GROUPS_SHOULDER = "grp0"

# all users are implicitly part of this group
PUBLIC_GROUP = DEF_GROUPS_SHOULDER + ":public"    # all users are implicitly part of this group
ANONYMOUS = ANONYMOUS_USER

__all__ = ["DBClient", "DBClientFactory", "ProjectRecord", "DBGroups", "Group", "ACLs", "PUBLIC_GROUP",
           "ANONYMOUS", "DAP_PROJECTS", "DMP_PROJECTS", "ObjectNotFound", "NotAuthorized", "AlreadyExists",
           "InvalidRecord", "InvalidUpdate" ]

Permissions = Union[str, Sequence[str], AbstractSet[str]]
CST = []

# forward declarations
ProtectedRecord = NewType("ProtectedRecord", object)
DBClient = NewType("DBClient", ABC)
DBPeople = NewType("DBPeople", object)


class ACLs:
    """
    a class for accessing and manipulating access control lists on a record
    """

    # Permissions
    READ = 'read'
    WRITE = 'write'
    READWRITE = WRITE
    ADMIN = 'admin'
    DELETE = 'delete'
    OWN = (READ, WRITE, ADMIN, DELETE,)

    def __init__(self, forrec: ProtectedRecord, acldata: MutableMapping = None):
        """
        intialize the object from raw ACL data 
        :param MutableMapping acldata:  the raw ACL data as returned from the record store as a dictionary
        :param ProjectRecord  projrec:  the record object that the ACLs apply to.  This will be used as 
                                          needed to interact with the backend record store
        """
        if not acldata:
            acldata = {}
        self._perms = acldata
        self._rec = forrec

    def iter_perm_granted(self, perm_name):
        """
        return an iterator to the list of identities that have been granted the given permission.  These
        will be either user names or group names.  If the given permission name is not a recognized 
        permission, then an iterator to an empty list is returned.
        """
        return iter(self._perms.get(perm_name, []))

    def grant_perm_to(self, perm_name, *ids):
        """
        add the user or group identities to the list having the given permission.  
        :param str perm_name:  the permission to be granted
        :param str ids:        the identities of the users the permission should be granted to 
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to grant this permission
        """
        if not self._rec.authorized(self.ADMIN):
            raise NotAuthorized(self._rec._cli.user_id, "grant permission")

        if perm_name not in self._perms:
            self._perms[perm_name] = []
        for id in ids:
            if id not in self._perms[perm_name]:
                self._perms[perm_name].append(id)

    def revoke_perm_from_all(self, perm_name, protect_owner: bool=True):
        """
        remove the given identities from the list having the given permission.  For each given identity 
        that does not currently have the permission, nothing is done.  
        :param str perm_name:  the permission to be revoked
        :param str ids:        the identities of the users the permission should be revoked from
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to grant this permission
        """
        if not self._rec.authorized(self.ADMIN):
            raise NotAuthorized(self._rec._cli.user_id, "revoke permission")

        empty = []
        if protect_owner and self._rec and perm_name in [ACLs.READ, ACLs.ADMIN] and \
           self._rec.owner in self._perms.get(perm_name,[]):
            # don't take away the owner's READ or ADMIN permissions
            empty = [self._rec.owner]

        if perm_name in self._perms:
            self._perms[perm_name] = empty

    def revoke_perm_from(self, perm_name, *ids, protect_owner: bool=True):
        """
        remove the given identities from the list having the given permission.  For each given identity 
        that does not currently have the permission, nothing is done.  Note that by default, read and 
        admin permissions cannot be revoked from the owner of the record unless ``protect_owner`` 
        is set to ``False``.  
        :param str perm_name:  the permission to be revoked
        :param str ids:        the identities of the users the permission should be revoked from
        :param bool protect_owner:  if True (default), do not revoke the owner's read and admin 
                               permissions even when the owner is one of the provided IDs.
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to grant this permission
        """
        if not self._rec.authorized(self.ADMIN):
            raise NotAuthorized(self._rec._cli.user_id, "revoke permission")

        if perm_name not in self._perms:
            return
        for id in ids:
            if protect_owner and self._rec and self._rec.owner == id and \
               perm_name in [ACLs.READ, ACLs.ADMIN]:
                # don't take away the owner's READ or ADMIN permissions
                continue
            if id in self._perms[perm_name]:
                self._perms[perm_name].remove(id)

    def _granted(self, perm_name, ids=[]):
        """
        return True if any of the given identities have the specified permission.  Normally, this will be 
        a list including a user identity and all the group identities that is user is a member of; however, 
        this is neither required nor checked by this implementation.

        This should be considered lowlevel; consider using :py:method:`authorized` instead which resolves 
        a users membership.  
        """
        return len(set(self._perms[perm_name]).intersection(ids)) > 0

    def __str__(self):
        return "<ACLs: {}>".format(str(self._perms))


class ProtectedRecord(ABC):
    """
    a base class for records that have ACLs attached to them.

    This record represents a local copy of the record that exists in the "remote" database.  The 
    client can make changes to this record; however, those changes are not persisted in the 
    database until the :py:meth:`save` method is called.
    """

    def __init__(self, servicetype: str, recdata: Mapping, dbclient: DBClient = None):
        """
        initialize the record with a dictionary retrieved from the underlying project collection.  
        The dictionary must include an `id` property with a valid ID value.
        """
        if not servicetype:
            raise ValueError(
                "ProtectedRecord(): must set service type (servicetype)")
        self._coll = servicetype
        self._cli = dbclient
        if not recdata.get('id'):
            raise ValueError("Record data is missing its 'id' property")
        self._data = self._initialize(recdata)
        self._acls = ACLs(self, self._data.get("acls", {}))
        self._status = RecordStatus(self.id, self._data['status'])
        self._authdel = _AuthDelegate(self) if self._coll != _AUTHDEL else None

    def _initialize(self, recdata: MutableMapping) -> MutableMapping:
        """
        initialize any missing data in the raw record data constituting the content of the record.  
        The implementation is allowed to update the input dictionary directly.  

        This default implimentation ensures that the record contains a minimal `acls` property

        :return: an combination of the given data and defaults
                 :rtype: MutableMapping
        """
        now = time.time()

        if not recdata.get('acls'):
            recdata['acls'] = {}
        if not recdata.get('owner'):
            recdata['owner'] = self._cli.user_id if self._cli else ""
        if 'deactivated' not in recdata:
            # Should be None or a date
            recdata['deactivated'] = None
        if 'status' not in recdata:
            recdata['status'] = RecordStatus(
                recdata['id'], {'created': -1}).to_dict(False)
        for perm in ACLs.OWN:
            if perm not in recdata['acls']:
                recdata['acls'][perm] = [
                    recdata['owner']] if recdata['owner'] else []
        return recdata

    @property
    def id(self):
        """
        the unique identifier for the record
        """
        return self._data.get('id')

    @property
    def owner(self):
        return self._data.get('owner', "")

    @owner.setter
    def owner(self, val):
        self.reassign(val)

    def reassign(self, who: str):
        """
        transfer ownership to the given user.  To transfer ownership, the calling user must have 
        "admin" permission on this record.  Note that this will not remove any permissions assigned 
        to the former owner.

        :param str who:   the identifier for the user to set as the owner of this record
        :raises NotAuthorized:  if the calling user is not authorized to change the owner.  
        :raises InvalidUpdate:  if the target user identifier is not recognized or not legal
        """
        if not self.authorized(ACLs.ADMIN):
            raise NotAuthorized(self._cli.user_id, "change owner")

        # make sure the target user is valid
        if not self._validate_user_id(who):
            raise InvalidUpdate("Unable to update owner: invalid user ID: "+str(who))

        self._data['owner'] = who
        for perm in ACLs.OWN:
            self.acls.grant_perm_to(perm, who)

    def _validate_user_id(self, who: str):
        if not bool(who) or not isinstance(who, str):
            # default test: ensure user is a non-empty string
            return False
        if self._cli and self._cli.people_service:
            return bool(self._cli.people_service.get_person_by_eid(who))
        return True

    @property
    def created(self) -> float:
        """
        the epoch timestamp indicating when this record was first corrected
        """
        return self.status.created

    @property
    def created_date(self) -> str:
        """
        the creation timestamp formatted as an ISO string
        """
        return self.status.created_date

    @property
    def modified(self) -> float:
        """
        the epoch timestamp indicating when this record was last updated
        """
        out = self.status.modified
        if out < 1:
            out = self.status.created
        return out

    @property
    def modified_date(self) -> str:
        """
        the timestamp for the last modification, formatted as an ISO string
        """
        return self.status.modified_date

    @property
    def deactivated(self) -> bool:
        """
        True if this record has been deactivated.  Record that are deactivated are generally
        skipped over when being accessed or used.  A deactivated record can only be retrieved 
        via its identifier.
        """
        return bool(self._data.get('deactivated'))

    @property
    def deactivated_date(self) -> str:
        """
        the timestamp when this record was deactivated, formatted as an ISO string.  An empty
        string is returned if the record is not currently deactivated.
        """
        if not self._data.get('deactivated'):
            return ""
        return datetime.fromtimestamp(math.floor(self._data.get('deactivated'))).isoformat()

    def deactivate(self) -> bool:
        """
        mark this record as "deactivated".  :py:meth:`deactivated` will now return True.
        The :py:meth:`save` method should be called to commit this change.
        :return:  False if the state was not changed for any reason, including because the record
                  was already deactivated.
                  :rtype: True
        """
        if self.deactivated:
            return False
        self._data['deactivated'] = time.time()
        return True

    def reactivate(self) -> bool:
        """
        reactivate this record; :py:meth:`deactivated` will now return False.
        The :py:meth:`save` method should be called to commit this change.
        :return:  False if the state was not changed for any reason, including because the record
                  was already activated.
                  :rtype: True
        """
        if not self.deactivated:
            return False
        self._data['deactivated'] = None
        return True

    @property
    def status(self) -> RecordStatus:
        """
        return the status object that indicates the current state of the record and the last 
        action applied to it.  
        """
        return self._status

    @property
    def acls(self) -> ACLs:
        """
        An object for accessing and updating the access control lists (ACLs) for this record
        """
        return self._acls

    def save(self):
        """
        save any updates to this record.  This implementation checks to make sure that the user 
        attached to the underlying client is authorized to make updates.

        :raises NotAuthorized:  if the user given by who is not authorized update the record
        """
        if not self.authorized(ACLs.WRITE):
            raise NotAuthorized(self._cli.user_id, "update record")
        olddates = (self.status.modified,
                    self.status.created, self.status.since)
        self.status.set_times()
        try:
            self._cli._upsert(self._coll, self._data)
        except Exception as ex:
            (self._data['modified'], self._data['created'],
             self._data['since']) = olddates
            raise

        self._authdel = _AuthDelegate(self) if self._coll != _AUTHDEL else None

    def authorized(self, perm: Permissions, who: str = None):
        """
        return True if the given user has the specified permission to commit an action on this record.
        The action is typically one of the base action permissions defined in this module, but it can 
        also be a custom permission suppported by this type of record.  This implementation will take 
        into account all of the groups the user is a member of.

        Note this implementation supports the notion of _superusers_ which implicitly hold all 
        premissions.  It will authorize the user if the user's id matches any of those in a list given 
        by the ``superusers`` configuration property.

        :param str|Sequence[str]|Set[str] perm:  a single permission or a list or set of permissions that 
                         the user must have to complete the requested action.  If a list of permissions 
                         is given, the user `who` must have all of the permissions.
        :param str who:  the identifier for the user attempting the action; if not given, the user id
                         attached to the DBClient is assumed.
        """
        if not who:
            who = self._cli.user_id

        if who in self._cli._cfg.get("superusers", []):
            return True

        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, list):
            perm = set(perm)

        idents = [who] + list(self._cli.all_groups_for(who))

        for p in perm:
            if not authdel.acls._granted(p, idents):
                return False
        return True


    def searched(self, cst: CST):
        """
        return True if the given records respect all the constraints in cst.
        :param is a dict of constraints for the records
        """
        # parse the query
        or_conditions = {}
        and_conditions = {}
        for condition in cst["$and"]:
            for key, value in condition.items():
                if key == "$or":
                    for or_condition in value:
                        for or_key, or_value in or_condition.items():
                            if or_key in or_conditions:
                                or_conditions[or_key].append(or_value)
                            else:
                                or_conditions[or_key]= [or_value]
                else:
                    and_conditions[key] = value

        #print("======== and_conditions =========")
        #for key, value in and_conditions.items():
        #    print(f"{key}: {value}")
        #print("======== or_conditions =========")
        #for key, values in or_conditions.items():
        #    for value in values:
        #        print(f"{key}: {value}")

        rec = self._data

        and_met = True
        for key, value in and_conditions.items():
            subdict = key.split(".")
            if len(subdict) > 1:
                if rec[subdict[0]].get(subdict[1]) == value:
                    continue
            else:
                if rec.get(key) == value:
                    continue
            and_met = False
            break

        or_met = False
        for key, values in or_conditions.items():
            subdict = key.split(".")
            for value in values:
                if len(subdict) > 1:
                    if rec.get(subdict[0], {}).get(subdict[1]) == value:
                        or_met = True
                        break
                else:
                    if rec.get(key) == value:
                        or_met = True
                        break
        #print(or_met)
        #print(and_met)
        if (not or_conditions or or_met) and and_met:
            return True
        return False

    def validate(self, errs=None, data=None) -> List[str]:
        """
        validate this record and return a list of error statements about its validity or an empty list
        if the record is valid.

        This implementation checks the `acls` property
        """
        if data is None:
            data = self._data
        if errs is None:
            errs = []

        if 'acls' not in data:
            errs.append("Missing 'acls' property")
        elif not isinstance(data['acls'], MutableMapping):
            errs.append("'acls' property not a dictionary")

        for perm in ACLs.OWN:
            if perm not in data['acls']:
                errs.append("ACLs: missing permmission: "+perm)
            elif not isinstance(data['acls'][perm], list):
                errs.append("ACL '{}': not a list".format(perm))

        return errs

    def to_dict(self):
        out = deepcopy(self._data)
        out['acls'] = self.acls._perms
        out['type'] = self._coll
        out['status']['createdDate'] = self.status.created_date
        out['status']['modifiedDate'] = self.status.modified_date
        out['status']['sinceDate'] = self.status.since_date
        return out

class _AuthDelegate(ProtectedRecord):
    # for internal use only; used to store original permissions
    def __init__(self, forrec: ProtectedRecord):
        usedata = {
            "id": forrec.id,
            "ownder": forrec.owner,
            "acls": deepcopy(forrec._data["acls"])
        }
        super(_AuthDelegate, self).__init__(_AUTHDEL, usedata, forrec._cli)

    def save(self):
        raise RuntimeException("Programming error: _AuthDelegate records should not be saved")

class Group(ProtectedRecord):
    """
    an updatable representation of a group.
    """

    def __init__(self, recdata: MutableMapping, dbclient: DBClient = None):
        """
        initialize the group record with a dictionary retrieved from the underlying group database
        collection.  The dictionary must include an `id` property with a valid ID value.
        """
        super(Group, self).__init__(GROUPS_COLL, recdata, dbclient)

    def _initialize(self, recdata: MutableMapping):
        out = super(Group, self)._initialize(recdata)
        if 'members' not in out:
            out['members'] = []
        return out

    @property
    def name(self):
        """
        the mnemonic name given to this group by its owner
        """
        return self._data['name']


    @name.setter
    def name(self, val):
        self.rename(val)

    def rename(self, newname):
        """
        assign the given name as the groups's mnemonic name.  If this record was pulled from
        the backend storage, then a check will be done to ensure that the name does not match
        that of any other group owned by the current user.

        :param str newname:  the new name to assign to the record
        :raises NotAuthorized:  if the calling user is not authorized to changed the name; for
                                non-superusers, ADMIN permission is required to rename a record.
        :raises AlreadyExists:  if the name has already been given to a record owned by the
                                current user.
        """
        if not self.authorized(ACLs.ADMIN):
            raise NotAuthorized(self._cli.user_id, "change name")
        if self._cli and self._cli.name_exists(newname):
            raise AlreadyExists(f"User {self_cli.user_id} has already defined a group with name={newname}")

        self._data['name'] = newname

    def validate(self, errs=None, data=None) -> List[str]:
        """
        validate this record and return a list of error statements about its validity or an empty list
        if the record is valid.
        """
        if not data:
            data = self._data

        # check the acls property
        errs = super(Group, self).validate(errs, data)

        for prop in "id name owner".split():
            if not data.get(prop):
                errs.append("'{}' property not set".format(prop))
            if not isinstance(data['id'], str):
                errs.append("'{}' property: not a str".format(prop))

        if not 'members' in data:
            errs.append("'members' property not found")
        if not isinstance(data['members'], list):
            errs.append("'members' property: not a list")

        return errs

    def iter_members(self):
        """
        iterate through the user IDs that constitute the members of this group
        """
        return iter(self._data['members'])

    def is_member(self, userid):
        """
        return True if the user for the given identifier is a member of this group
        """
        return userid in self._data['members']

    def add_member(self, *memids):
        """
        add members to this group (if they aren't already members)
        :param str memids:  the identities of the users to be added to the group
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not
                               authorized to add members
        """
        if not self.authorized(ACLs.WRITE):
            raise NotAuthorized(self._cli.user_id, "add member")

        for id in memids:
            if id not in self._data['members']:
                self._data['members'].append(id)

        return self

    def remove_member(self, *memids):
        """
        remove members from this group; any given ids that are not currently members are ignored.
        :param str memids:  the identities of the users to be removed from the group
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not
                               authorized to remove members
        """
        if not self.authorized(ACLs.WRITE):
            raise NotAuthorized(self._cli.user_id, "remove member")

        for id in memids:
            if id in self._data['members']:
                self._data['members'].remove(id)

        return self

    def __str__(self):
        return "<{} Group: {} ({}) owner={}>".format(self._coll.rstrip("s"), self.id,
                                                     self.name, self.owner)


class DBGroups(object):
    """
    an interface for creating and using user groups.  Each group has a unique identifier assigned to it
    and holds a list of user (and/or group) identities indicating the members of the groups.  In addition
    to its unique identifier, a group also has a mnemonic name given to it by the group's owner; the
    group name need not be globally unique, but it should be unique within the owner's namespace.
    """

    def __init__(self, dbclient: DBClient, idshoulder: str = DEF_GROUPS_SHOULDER):
        """
        initialize the interface with the groups collection
        :param DBClient dbclient:  the database client to use to interact with the database backend
        :param str    idshoulder:  the base shoulder to use for new group identifiers
        """
        self._cli = dbclient
        self._shldr = idshoulder

    @property
    def native(self):
        return self._cli._native

    def create_group(self, name: str, foruser: str = None):
        """
        create a new group for the given user.
        :param str name:     the name of the group to create
        :param str foruser:  the identifier of the user to create the group for.  This user will be set as
                             the group's owner/administrator.  If not given, the user attached to the
                             underlying :py:class:`DBClient` will be used.  Only a superuser (an identity
                             listed in the `superuser` config parameter) can create a group for another
                             user.
        :raises AlreadyExists:  if the user has already defined a group with this name
        :raises NotAuthorized:  if the user is not authorized to create this group
        """
        if not foruser:
            foruser = self._cli.user_id
        if not self._cli._authorized_group_create(self._shldr, foruser):
            raise NotAuthorized(self._cli.user_id, "create group")

        if self.name_exists(name, foruser):
            raise AlreadyExists(
                "User {} has already defined a group with name={}".format(foruser, name))

        out = Group({
            "id": self._mint_id(self._shldr, name, foruser),
            "name": name,
            "owner": foruser,
            "members": [foruser],
            "acls": {
                ACLs.ADMIN:  [foruser],
                ACLs.READ:   [foruser],
                ACLs.WRITE:  [foruser],
                ACLs.DELETE: [foruser]
            }
        }, self._cli)
        out.save()
        self._cli.recache_user_groups()
        return out

    def _mint_id(self, shoulder, name, owner):
        """
        create and register a new identifier that can be assigned to a new group
        :param str shoulder:   the shoulder to prefix to the identifier.  The value usually controls
                               how the identifier is formed.
        """
        return "{}:{}:{}".format(shoulder, owner, name)

    def exists(self, gid: str) -> bool:
        """
        return True if a group with the given ID exists.  READ permission on the identified
        record is not required to use this method.
        """
        return bool(self._cli._get_from_coll(GROUPS_COLL, gid))

    def name_exists(self, name: str, owner: str = None) -> bool:
        """
        return True if a group with the given name exists.  READ permission on the identified
        record is not required to use this method.
        :param str name:  the mnemonic name of the group given to it by its owner
        :param str owner: the ID of the user owning the group of interest; if not given, the
                          user ID attached to the `DBClient` is assumed.
        """
        if not owner:
            owner = self._cli.user_id
        it = self._cli._select_from_coll(GROUPS_COLL, incl_deact=True, name=name, owner=owner)
        try:
            return bool(next(it))
        except StopIteration:
            return False

    def get(self, gid: str) -> Group:
        """
        return the group by its given group identifier
        """
        m = self._cli._get_from_coll(GROUPS_COLL, gid)
        if not m:
            return None
        m = Group(m, self._cli)
        if m.authorized(ACLs.READ):
            return m
        raise NotAuthorized(id, "read")

    def __getitem__(self, id) -> Group:
        out = self.get(id)
        if not out:
            raise KeyError(id)
        return out

    def get_by_name(self, name: str, owner: str = None) -> Group:
        """
        return the group assigned the given name by its owner.  This assumes that the given owner
        has created only one group with the given name.
        """
        if not owner:
            owner = self._cli.user_id
        matches = self._cli._select_from_coll(GROUPS_COLL, incl_deact=True, name=name, owner=owner)
        for m in matches:
            m = Group(m, self._cli)
            if m.authorized(ACLs.READ):
                return m
        return None

    def select_ids_for_user(self, id: str) -> MutableSet:
        """
        return all the groups that a user (or a group) is a member of.  This implementation will
        resolve the groups that the user is indirectly a member of--i.e. a user's group itself is a
        member of another group.  Deactivated groups are not included.
        """
        checked = set()
        out = set(g['id'] for g in self._cli._select_prop_contains(GROUPS_COLL, 'members', id))

        follow = list(out)
        while len(follow) > 0:
            gg = follow.pop(0)
            if gg not in checked:
                add = set(g['id'] for g in self._cli._select_prop_contains(GROUPS_COLL, 'members', gg))
                out |= add
                checked.add(gg)
                follow.extend(add.difference(checked))

        out.add(PUBLIC_GROUP)

        return out

    def delete_group(self, gid: str) -> bool:
        """
        delete the specified group from the database.  The user attached to the underlying
        :py:class:`DBClient` must either be the owner of the record or have `DELETE` permission
        to carry out this option.
        :return:  True if the group was found and successfully deleted; False, otherwise
                  :rtype: bool
        """
        g = self.get(gid)
        if not g:
            return False
        if not g.authorized(ACLs.DELETE):
            raise NotAuthorized(gid, "delete group")

        self._cli._delete_from(GROUPS_COLL, gid)
        return True


class ProjectRecord(ProtectedRecord):
    """
    a single record from the project collection representing one project created by the user

    This record represents a local copy of the record that exists in the "remote" database.  The
    client can make changes to this record; however, those changes are not persisted in the
    database until the :py:meth:`save` method is called.
    """

    def __init__(self, projcoll: str, recdata: Mapping, dbclient: DBClient = None):
        """
        initialize the record with a dictionary retrieved from the underlying project collection.
        The dictionary must include an `id` property with a valid ID value.
        """
        super(ProjectRecord, self).__init__(projcoll, recdata, dbclient)

    def _initialize(self, rec: MutableMapping) -> MutableMapping:
        rec = super(ProjectRecord, self)._initialize(rec)

        if 'data' not in rec:
            rec['data'] = OrderedDict()
        if 'meta' not in rec:
            rec['meta'] = OrderedDict()
        if 'curators' not in rec:
            rec['curators'] = []

        self._initialize_data(rec)
        self._initialize_meta(rec)
        return rec

    def _initialize_data(self, recdata: MutableMapping):
        """
        add default data to the given dictionary of application-specific project data.
        """
        return recdata["data"]

    def _initialize_meta(self, recdata: MutableMapping):
        """
        add default data to the given dictionary of application-specific project metadata
        """
        return recdata["meta"]

    @property
    def name(self) -> str:
        """
        the mnemonic name given to this record by its creator
        """
        return self._data.get('name', "")

    @name.setter
    def name(self, val):
        self.rename(val)

    def rename(self, newname):
        """
        assign the given name as the record's mnemonic name.  If this record was pulled from
        the backend storage, then a check will be done to ensure that the name does not match
        that of any other record owned by the current user.

        :param str newname:  the new name to assign to the record
        :raises NotAuthorized:  if the calling user is not authorized to changed the name; for
                                non-superusers, ADMIN permission is required to rename a record.
        :raises AlreadyExists:  if the name has already been given to a record owned by the
                                current user.
        """
        if not self.authorized(ACLs.ADMIN):
            raise NotAuthorized(self._cli.user_id, "change name")
        if self._cli and self._cli.name_exists(newname):
            raise AlreadyExists(f"User {self_cli.user_id} has already defined a record with name={newname}")

        self._data['name'] = newname

    @property
    def data(self) -> MutableMapping:
        """
        the application-specific data for this record.  This dictionary contains data that is generally
        updateable directly by the user (e.g. via the GUI interface).  The expected properties for
        are determined by the application.
        """
        return self._data['data']

    @data.setter
    def data(self, data: Mapping):
        self._data['data'] = deepcopy(data)

    @property
    def meta(self) -> MutableMapping:
        """
        the application-specific metadata for this record.  This dictionary contains data that is generally
        not directly editable by the application, but which the application must track in order to manage
        the updating process.  The expected properties for this dictionary are determined by the application.
        """
        return self._data['meta']

    @meta.setter
    def meta(self, data: Mapping):
        self._data['meta'] = deepcopy(data)

    def __str__(self):
        return "<{} ProjectRecord: {} ({}) owner={}>".format(self._coll.rstrip("s"), self.id,
                                                             self.name, self.owner)


class DBClient(ABC):
    """
    a client connected to the database for a particular service (e.g. drafting, DMPs, etc.)

    As this class is abstract, implementations provide support for specific storage backends.
    All implementations support the following common set of configuration parameters:

    ``superusers``
         (List[str]) _optional_.  a list of strings giving the identifiers of users that
         should be considered superusers who will be afforded authorization for all operations
    ``allowed_project_shoulders``
         (List[str]) _optional_.  a list of strings representing the identifier prefixes--i.e.
         the _shoulders_--that can be used to create new project identifiers.  If not provided,
         the only allowed shoulder will be that given by ``default_shoulder``.
    ``default_shoulder``
         (str) _required_.  the identifier prefix--i.e. the _shoulder_--that should be used
         by default when not otherwise requested by the user when creating new project records.
    ``allowed_group_shoulders``
         (List[str]) _optional_.  a list of strings representing the identifier prefixes--i.e.
         the _shoulders_--that can be used to create new group identifiers.  If not provided,
         the only allowed shoulder will be the default, ``grp0``.
    """

    def __init__(self, config: Mapping, projcoll: str, nativeclient=None, foruser: str = ANONYMOUS,
                 peopsvc: PeopleService = None,websocket: str = 'ws://localhost:8765', key_websocket: str = '123456_secret_key'):
        """
        initialize the base client.
        :param dict  config:  the configuration data for the client
        :param str projcoll:  the type of project to connect with (i.e. the project collection name)
        :param nativeclient:  where applicable, the native client object to use to connect the back
                              end database.  The type and use of this client is implementation-specific
        :param str  foruser:  the user identity to connect as.  This will control what records are
                              accessible via this instance's methods.
        :param PeopleService peopsvc:  a PeopleService to incorporate into this client
        """
        self._cfg = config
        self._native = nativeclient
        self._projcoll = projcoll
        self._who = foruser
        self._whogrps = None

        self._dbgroups = DBGroups(self)
        self._peopsvc = peopsvc
        self.notifier = Notifier(uri=websocket,api_key=key_websocket)

    @property
    def project(self) -> str:
        """
        return the name of the project collection/type that this client handles records for
        """
        return self._projcoll

    @property
    def user_id(self) -> str:
        """
        the identifier of the user that this client is acting on behalf of
        """
        return self._who

    @property
    def user_groups(self) -> frozenset:
        """
        the set of identifiers for groups that the user given by :py:property:`user_id` belongs to.
        """
        if not self._whogrps:
            self.recache_user_groups()
        return self._whogrps

    def all_groups_for(self, who) -> frozenset:
        """
        Return the frozen set of all groups a user or group belongs to.
        """
        adhoc = self.groups.select_ids_for_user(who)
        virtual_groups = self._get_virtual_groups_for(who)
        all_groups = frozenset(adhoc.union(virtual_groups))

        return all_groups

    @property
    def people_service(self) -> PeopleService:
        """
        an attached PeopleService instance or None if such a service is not available.  This service
        encapsulates access to the organization's staff directory service.
        """
        return self._peopsvc

    def recache_user_groups(self):
        """
        the :py:property:`user_groups` contains a cached list of all the groups the user is
        a member of.  This function will recache this list (resulting in queries to the backend
        database).
        """
        adhoc_groups = self.groups.select_ids_for_user(self._who)
        virtual_groups = self._get_virtual_groups_for(self._who)
        self._whogrps = frozenset(adhoc_groups.union(virtual_groups))

    def _get_virtual_groups_for(self, user_id: str) -> List[str]:
        """
        Return the list of 'virtual groups' ids a user is part of, based on the staff directory
        (PeopleService). Returns an empty list if the PeopleService is not available or the
        user is not found.
        """
        if not self.people_service:
            return []
        person = self.people_service.get_person_by_eid(user_id)
        if not person:
            return []
        out = []
        if 'nistou' in person and person['nistou']:
            out.append(f"nistou:{person['nistou']}")
        if 'nistdiv' in person and person['nistdiv']:
            out.append(f"nistdiv:{person['nistdiv']}")
        if 'nistgrp' in person and person['nistgrp']:
            out.append(f"nistgrp:{person['nistgrp']}")
        return out

    def create_record(self, name: str, shoulder: str = None, foruser: str = None) -> ProjectRecord:
        """
        create (and save) and return a new project record.  A new unique identifier should be assigned
        to the record.

        :param str     name:  the mnemonic name (provided by the requesting user) to give to the 
                              record.
        :param str shoulder:  the identifier shoulder prefix to create the new ID with.  
                              (The implementation should ensure that the requested user is authorized 
                              to request the shoulder.)
        :param str  foruser:  the ID of the user that should be registered as the owner.  If not 
                              specified, the value of :py:property:`user_id` will be assumed.  In 
                              this implementation, only a superuser can create a record for someone 
                              else.
        """
        if not foruser:
            foruser = self.user_id
        if not shoulder:
            shoulder = self._default_shoulder()
        if not self._authorized_project_create(shoulder, foruser):
            raise NotAuthorized(self.user_id, "create record")
        if self.name_exists(name, foruser):
            raise AlreadyExists(
                "User {} has already defined a record with name={}".format(foruser, name))

        rec = self._new_record_data(self._mint_id(shoulder))
        rec['name'] = name
        rec = ProjectRecord(self._projcoll, rec, self)
        rec.save()
        message = f"proj-create,{self._projcoll},{name}"
        self.notifier.notify(message)
        return rec 

    def _default_shoulder(self):
        out = self._cfg.get("default_shoulder")
        if not out:
            raise ConfigurationException(
                "Missing required configuration parameter: default_shoulder")
        return out

    def _authorized_project_create(self, shoulder, who):
        shldrs = set(self._cfg.get("allowed_project_shoulders", []))
        defshldr = self._cfg.get("default_shoulder")
        if defshldr:
            shldrs.add(defshldr)
        return self._authorized_create(shoulder, shldrs, who)

    def _authorized_group_create(self, shoulder, who):
        shldrs = set(self._cfg.get("allowed_group_shoulders", []))
        defshldr = DEF_GROUPS_SHOULDER
        if defshldr:
            shldrs.add(defshldr)
        return self._authorized_create(shoulder, shldrs, who)

    def _authorized_create(self, shoulder, shoulders, who):
        if self._who and who != self._who and self._who not in self._cfg.get("superusers", []):
            return False
        return shoulder in shoulders

    def _mint_id(self, shoulder):
        """
        create and register a new identifier that can be attached to a new project record
        :param str shoulder:   the shoulder to prefix to the identifier.  The value usually controls
                               how the identifier is formed.  
        """
        return "{0}:{1:04}".format(shoulder, self._next_recnum(shoulder))

    def _parse_id(self, id):
        pair = id.rsplit(':', 1)
        if len(pair) != 2:
            return None, None
        try:
            return pair[0], int(pair[1])
        except ValueError:
            return None, None

    @abstractmethod
    def _next_recnum(self, shoulder):
        """
        return an unused record number that can be used to mint a new identifier.  This is called 
        by the default implementation of :py:method:`_mint_id`.  Typically, each shoulder has its 
        own unique sequence of numbers associated with it.  
        :param str shoulder:  the shoulder that the record number will be combined with
        """
        raise NotImplementedError()

    def _new_record_data(self, id):
        """
        return a dictionary containing data that will constitue a new ProjectRecord with the given 
        identifier assigned to it.  Generally, this record should not be committed yet.
        """
        return {"id": id, "status": {"created_by": self.user_id}}

    def exists(self, gid: str) -> bool:
        """
        return True if a group with the given ID exists.  READ permission on the identified 
        record is not required to use this method. 
        """
        return bool(self._get_from_coll(self._projcoll, gid))

    def name_exists(self, name: str, owner: str = None) -> bool:
        """
        return True if a project with the given name exists.  READ permission on the identified 
        record is not required to use this method.
        :param str name:  the mnemonic name of the group given to it by its owner
        :param str owner: the ID of the user owning the group of interest; if not given, the 
                          user ID attached to the `DBClient` is assumed.
        """
        if not owner:
            owner = self.user_id
        it = self._select_from_coll(self._projcoll, incl_deact=True, name=name, owner=owner)
        try:
            return bool(next(it))
        except StopIteration:
            return False

    def get_record_by_name(self, name: str, owner: str = None) -> Group:
        """
        return the group assigned the given name by its owner.  This assumes that the given owner 
        has created only one group with the given name.  
        """
        if not owner:
            owner = self.user_id
        matches = self._select_from_coll(self._projcoll, incl_deact=True, name=name, owner=owner)
        for m in matches:
            m = ProjectRecord(self._projcoll, m, self)
            if m.authorized(ACLs.READ):
                return m
        return None

    def get_record_for(self, id: str, perm: str = ACLs.READ) -> ProjectRecord:
        """
        return a single project record by its identifier.  The record is only 
        returned if the user this client is attached to is authorized to access the record with 
        the given permission.

        :param str   id:  the identifier for the record of interest
        :param str perm:  the permission type that the user must be authorized for in order for 
                          the record to be returned; if user is not authorized, an exception is raised.
                          Default: `ACLs.READ`
        :raises ObjectNotFound:  if the identifier does not exist
        :raises  NotAuthorized:  if the user does not have the permission given by ``perm``
        """
        out = self._get_from_coll(self._projcoll, id)
        if not out:
            raise ObjectNotFound(id)
        out = ProjectRecord(self._projcoll, out, self)
        if not out.authorized(perm):
            raise NotAuthorized(self.user_id, perm)
        return out

    @classmethod
    def check_query_structure(cls, query):
        if not isinstance(query, dict):
            return False

        valid_operators = ['$and', '$or', '$not', '$nor', '$eq', '$ne', '$gt', '$gte', '$lt',
                           '$lte', '$in', '$nin', '$exists', '$type', '$mod', '$regex', '$text',
                           '$all', '$elemMatch', '$size']

        for key in query.keys():
            if key not in valid_operators:
                return False

            if isinstance(query[key], dict):
                if not check_query_structure(query[key]):
                    return False

            return True

    @abstractmethod
    def select_records(self, perm: Permissions = ACLs.OWN, **constraints) -> Iterator[ProjectRecord]:
        """
        return an iterator of project records for which the given user has at least one of the given 
        permissions

        :param str       user:  the identity of the user that wants access to the records.  
        :param str|[str] perm:  the permissions the user requires for the selected record.  For
                                each record returned the user will have at least one of these
                                permissions.  The value can either be a single permission value
                                (a str) or a list/tuple of permissions
        """
        raise NotImplementedError()
    
    @abstractmethod
    def adv_select_records(self, filter: Mapping, perm: Permissions = ACLs.OWN) -> Iterator[ProjectRecord]:
        """
        return an iterator of project records for which the given user has at least one of the
        permissions and the records meet all the constraints given

        :param str       user:  the identity of the user that wants access to the records.  
        :param str|[str] perm:  the permissions the user requires for the selected record.  For
                                each record returned the user will have at least one of these
                                permissions.  The value can either be a single permission value
                                (a str) or a list/tuple of permissions
        :param str       **cst: a json that describes all the constraints the records should meet. 
                                the schema of this json is the query structure used by mongodb.
        """
        raise NotImplementedError()

    def is_connected(self) -> bool:
        """
        return True if this client is currently connected to its underlying database
        """
        return self._native is not None

    @property
    def native(self):
        """
        an instance of the native client specific for the underlying database being used.
        Accessing this property may implicitly cause the client to establish a connection.  (See 
        also :py:method:`is_connected`.)
        """
        return self._native

    @property
    def groups(self) -> DBGroups:
        """
        access to the management of groups
        """
        return self._dbgroups

    @property
    def people(self) -> DBPeople:
        """
        access to the people collection
        """
        return None

    @abstractmethod
    def _upsert(self, coll: str, recdata: Mapping) -> bool:
        """
        insert or update a data record into the specified collection.  
        :param str coll:  the name of the record collection to insert the record into.  
        :param Mapping recdata:  the record to update or insert.  This dictionary must include a an
                          "id" property.  If a record with the same value of "id" exists in the 
                          collection, that record will be replaced by this one; otherwise, this 
                          record will just be added.
        :return:  True if the record, based on its `id` property, was added for the first time.  
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_from_coll(self, collname, id) -> MutableMapping:
        """
        return a record with a given identifier from the specified collection
        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param str id:         the identifier for the record of interest
        """
        raise NotImplementedError()

    @abstractmethod
    def _select_from_coll(self, collname, incl_deact=False, **constraints) -> Iterator[MutableMapping]:
        """
        return an iterator to the records from a specified collection that match the set of 
        given constraints.

        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param dict constraints:  the constraints on properties in the record.  The returned records 
                               must all have properties matching the keys in the given constraint 
                               dictionary with corresponding matching values 
        """
        raise NotImplementedError()

    @abstractmethod
    def _select_prop_contains(self, collname, prop, target, incl_deact=False) -> Iterator[MutableMapping]:
        """
        return an iterator to the records from a specified collection in which the named list property
        contains a given target value.

        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param str prop:    the name of the property whose list value will be searched for the target value
        :param str target:  the value to search for in the list value of the specified property, `prop`.
        """
        raise NotImplementedError()

    @abstractmethod
    def _delete_from(self, collname, id):
        """
        delete a record with the given id from the named collection.  Nothing should happen if the record
        does not exist in the database collection.

        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param str id:  the identifier for the record to be deleted.
        """
        raise NotImplementedError()

    def delete_record(self, id: str) -> bool:
        """
        delete the specified group from the database.  The user attached to this 
        :py:class:`DBClient` must either be the owner of the record or have `DELETE` permission
        to carry out this option. 
        :return:  True if the group was found and successfully deleted; False, otherwise
                  :rtype: bool
        """
        try:
            g = self.get_record_for(id, ACLs.DELETE)
        except ObjectNotFound:
            return False
        if not g:
            return False

        self._delete_from(self._projcoll, id)
        return True

    def record_action(self, act: Action, coll: str = None):
        """
        save the given action record to the back-end store.  In order to save the action, the 
        action's subject must identify an existing record and the current user must have write 
        permission on that record.

        :param Action act:  the Action object to save
        :param str   coll:  the collection that the record with the action's subject ID can be 
                            found.  If not provided, the current project collection will be assumed.
        """
        if not act.subject:
            raise ValueError(
                "record_action(): action is missing a subject identifier")
        if act.type == Action.PROCESS and \
           (not isinstance(act.object, Mapping) or 'name' not in act.object):
            raise ValueError(
                "record_action(): action object is missing name property: "+str(act.object))

        # check existence and permission
        if not coll:
            coll = self._projcoll
        rec = self._get_from_coll(coll, act.subject)
        if not rec:
            raise ObjectNotFound(act.subject)
        rec = ProtectedRecord(coll, rec, self)
        if not rec.authorized(ACLs.WRITE):
            raise NotAuthorized(rec.id, "record action for id="+rec.id)

        self._save_action_data(act.to_dict())

    @abstractmethod
    def _save_action_data(self, actdata: Mapping):
        """
        save the given data to the action log collection
        """
        raise NotImplementedError()

    @abstractmethod
    def _select_actions_for(self, id: str) -> List[Mapping]:
        """
        retrieve all actions currently recorded for the record with the given identifier
        """
        raise NotImplementedError()

    @abstractmethod
    def _delete_actions_for(self, id: str):
        """
        purge all actions currently recorded for the record with the given identifier
        """
        raise NotImplementedError()

    def _close_actionlog_with(self, rec: ProtectedRecord, close_action: Action, extra=None,
                              cancel_if_empty=True):
        """
        archive all actions in the action log for a given ID, ending with the given action.
        All of the entries associated with the given record will be removed from the action 
        log and stored into an action archive document in JSON format.  

        :param ProtectedRecord rec:  the record whose action log is being closed
        :param Action close_action:  the action that is effectively closing the record.  This 
                                     is usually a PROCESS action or a DELETE action.
        :param dict extra:  additional data to include in the action archive document
        :raises NotAuthorized:  if the current user does not have write permission to the given
                                record.  
        """
        if not rec.authorized(ACLs.WRITE):
            raise NotAuthorized(
                self.user_id, "close record history for id="+rec.id)

        history = self._select_actions_for(rec.id)
        if len(history) == 0 and cancel_if_empty:
            return
        history.append(close_action.to_dict())

        # users with permission to read record can read the history, but only superusers
        # can update it or administer it.
        acls = OrderedDict([
            ("read", rec.acls._perms.get('read', []))
        ])

        if 'recid' in extra or 'close_action' in extra:
            extra = deepcopy(extra)
            if 'recid' in extra:
                del extra['recid']
            if 'close_action' in extra:
                del extra['close_action']

        archive = OrderedDict([
            ("recid", rec.id),
            ("close_action", close_action.type)
        ])
        if close_action.type == Action.PROCESS:
            archive['close_action'] += ":%s" % str(close_action.object)
        archive.update(extra)
        archive['acls'] = acls
        archive['history'] = history

        self._save_history(archive)
        self._delete_actions_for(rec.id)

    @abstractmethod
    def _save_history(self, histrec):
        """
        save the given history record to the history collection
        """
        raise NotImplementedError()


class DBClientFactory(ABC):
    """
    an abstract class for creating client connections to the database
    """

    def __init__(self, config, peopsvc: PeopleService = None):
        """
        initialize the factory with its configuration.  The configuration provided here serves as 
        the default parameters for the cient as these can be overridden by the configuration parameters
        provided via :py:method:`create_client`.  Generally, it is recommended that the parameters 
        the configure the backend storage be provided here, and that the non-storage parameters--namely,
        the ones that control authorization--be provided via :py:method:`create_client` as these can 
        depend on the type of project being access (e.g. "dmp" vs. "dap").

        :param dict          config:  the DBClient configuration
        :param PeoplService peopsvc:  a PeopleService to use to look up people in the organization.  If
                                      not provided, an attempt will be made to create one from the 
                                      configuration (via its ``people_service`` parameter). 
        """
        self._cfg = config
        self._peopsvc = peopsvc

    def create_people_service(self, config: Mapping = {}):
        """
        create a PeopleService that a DBClient can use.  The configuration data provided here is 
        typically value of the ``people_service`` parameter (when that value is a dictionary).
        """
        return create_people_service(config)

    @abstractmethod
    def create_client(self, servicetype: str, config: Mapping = {}, foruser: str = ANONYMOUS):
        """
        create a client connected to the database and the contents related to the given service

        .. code-block::
           :caption: Example

           # connect to the DMP collection
           client = dbio.MIDASDBClientFactory(configdata).create_client(dbio.DMP_PROJECTS, config, userid)

        :param str servicetype:  the service data desired.  The value should be one of ``DAP_PROJECTS``
                                 or ``DMP_PROJECTS``
        :param Mapping  config:  the configuration to pass into the client.  This will be merged into and 
                                 override the configuration provided to the factory at construction time. 
                                 Typically, the configuration provided here are the common parameters that 
                                 are independent of the type of backend storage.
        :param str     foruser:  The identifier of the user that DBIO requests will be made on behalf of.
        """
        raise NotImplementedError()


class DBIOException(MIDASException):
    """
    a general base Exception class for exceptions that occur while interacting with the DBIO framework
    """
    pass


class DBIORecordException(DBIOException):
    """
    a base Exception class for DBIO exceptions that are associated with a specific DBIO record.  This 
    class provides the record identifier via a ``record_id`` attribute.  
    """

    def __init__(self, recid, message, sys=None):
        super(DBIORecordException, self).__init__(message, sys=sys)
        self.record_id = recid


class InvalidRecord(DBIORecordException):
    """
    an exception indicating that record data is invalid and requires correction or completion.

    The determination of invalid data may result from detailed data validation which may uncover 
    multiple errors.  The ``errors`` property will contain a list of messages, each describing a
    validation error encounted.  The :py:meth:`format_errors` will format all these messages into 
    a single string for a (text-based) display. 
    """
    def __init__(self, message: str=None, recid: str=None, part: str=None,
                 errors: List[str]=None, sys=None):
        """
        initialize the exception
        :param str message:  a brief description of the problem with the user input
        :param str   recid:  the id of the record that data was provided for
        :param str    part:  the part of the record that was requested for update.  Do not provide 
                             this parameter if the entire record was provided.
        :param [str] errors: a listing of the individual errors uncovered in the data
        """
        if errors:
            if not message:
                if len(errors) == 1:
                    message = "Validation Error: " + errors[0]
                elif len(errors) == 0:
                    message = "Unknown validation errors encountered"
                else:
                    message = "Encountered %d validation errors, including: %s" % (len(errors), errors[0])
        elif message:
            errors = [message]
        else:
            message = "Unknown validation errors encountered while updating data"
            errors = []
        
        super(InvalidRecord, self).__init__(recid, message, sys)
        self.record_part = part
        self.errors = errors

    def __str__(self):
        out = ""
        if self.record_id:
            out += "%s: " % self.record_id
        if self.record_part:
            out += "%s: " % self.record_part
        return out + super().__str__()

    def format_errors(self):
        """
        format into a string the listing of the validation errors encountered that resulted in 
        this exception.  The returned string will have embedded newline characters for multi-line
        text-based display.
        """
        if not self.errors:
            return str(self)

        out = ""
        if self.record_id:
            out += "%s: " % self.record_id
        out += "Validation errors encountered"
        if self.record_part:
            out += " in data submitted to update %s" % self.record_part
        out += ":\n  * "
        out += "\n  * ".join([str(e) for e in self.errors])
        return out

class InvalidUpdate(InvalidRecord):
    """
    an exception indicating that the user-provided data is invalid or otherwise would result in 
    invalid data content for a record. 

    The determination of invalid data may result from detailed data validation which may uncover 
    multiple errors.  The ``errors`` property will contain a list of messages, each describing a
    validation error encounted.  The :py:meth:`format_errors` will format all these messages into 
    a single string for a (text-based) display. 
    """
    def __init__(self, message: str=None, recid=None, part=None, errors: List[str]=None, sys=None):
        """
        initialize the exception
        :param str message:  a brief description of the problem with the user input
        :param str   recid:  the id of the record that data was provided for
        :param str    part:  the part of the record that was requested for update.  Do not provide 
                             this parameter if the entire record was provided.
        :param [str] errors: a listing of the individual errors uncovered in the data
        """
        super(InvalidUpdate, self).__init__(message, recid, part, errors, sys)


class NotAuthorized(DBIOException):
    """
    an exception indicating that the user attempted an operation that they are not authorized to 
    """

    def __init__(self, who: str = None, op: str = None, message: str = None, sys=None):
        """
        create the exception
        :param str who:     the identifier of the user who requested the operation
        :param str op:      a brief phrase or term identifying the unauthorized operation. (A verb
                            or verb phrase is recommended.)
        :param str message: the message describing why the exception was raised; if not given,
                            a default message is constructed from `userid` and `op`.
        """
        self.user_id = who
        self.operation = op
        if not message:
            if not op:
                op = "effect an unspecified action"
            message = "User "
            if who:
                message += who + " "
            message += "is not authorized to {}".format(op)

        super(NotAuthorized, self).__init__(message)


class AlreadyExists(DBIOException):
    """
    an exception indicating a disallowed attempt to create a record that already exists (or includes 
    identifying data the corresponds to an already existing record).
    """
    pass


class ObjectNotFound(DBIORecordException):
    """
    an exception indicating that the requested record, or a requested part of a record, does not exist.
    """

    def __init__(self, recid, part=None, message=None, sys=None):
        """
        initialize this exception
        :param str   recid: the id of the record that was existed
        :param str    part: the part of the record that was requested.  Do not provide this parameter if 
                            the entire record does not exist.  
        :param str message: a brief description of the error (what object was not found)
        """
        self.record_part = part

        if not message:
            if part:
                message = "Requested portion of record (id=%s) does not exist: %s" % (
                    recid, part)
            else:
                message = "Requested record with id=%s does not exist" % recid
        super(ObjectNotFound, self).__init__(recid, message)