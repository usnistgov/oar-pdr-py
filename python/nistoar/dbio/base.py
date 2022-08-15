"""
The abstract interface for interacting with the database.  

This interface is based on the following model:

  *  Each service (drafting, DMPs, etc.) has its own collection that extends on a common base model
  *  Each *record* in the collection represents a "project" that a user is working on via the service
  *  A record can be expressed as a Python dictionary which can be exported into JSON

"""
import time
from abc import ABC, ABCMeta, abstractmethod, abstractproperty
from collections.abc import Mapping, MutableMapping, Sequence, Set
from collections import OrderedDict
from typing import Union, List
from enum import Enum
from datetime import datetime

from nistoar.base.config import ConfigurationException

DRAFT_PROJECTS = "draft"
DMP_PROJECTS   = "dmp"
GROUPS_COLL = "groups"
PEOPLE_COLL = "people"

DEF_PEOPLE_SHOULDER = "ppl0"
DEF_GROUPS_SHOULDER = "grp0"

PUBLIC_GROUP = DEF_GROUPS_SHOULDER + ":public"    # all users are implicitly part of this group
ANONYMOUS = PUBLIC_GROUP

# Permissions
READ      = 'read'
WRITE     = 'write'
READWRITE = WRITE
ADMIN     = 'admin'
DELETE    = 'delete'
OWN       = (READ, WRITE, ADMIN, DELETE,)

Permissions = Union[str, Sequence[str], Set[str]]

class DBClientFactory(ABC):
    """
    an abstract class for creating client connections to the database
    """

    def createClient(self, servicetype: str, foruser: str = ANONYMOUS):
        """
        create a client connected to the database and the contents related to the given service

        .. code-block::
           :caption: Example

           # connect to the DMP collection
           client = dbio.DBClienFactory(configdata).createClient(dbio.DMP_PROJECTS)

        :param str servicetype:  the service data desired.  The value should be one of ``DRAFT_SERVICE``
                                 or ``DMP_SERVICE``
        """
        pass

class DBClient(ABC):
    """
    a client connected to the database for a particular service (e.g. drafting, DMPs, etc.)
    """

    def __init__(self, config: Mapping, projcoll: str, nativeclient=None, foruser: str = ANONYMOUS):
        self._cfg = config
        self._native = nativeclient
        self._projcoll = projcoll
        self._who = foruser
        self._whogrps = None

        self._dbgroups = DBGroups(self)

    @property
    def user_id(self) => str:
        return self._who

    @property
    def user_groups(self) => frozenset:
        if self._whogrps:
            self.recache_user_groups()
        return self._whogrps

    def recache_user_groups(self):
        """
        the :py:property:`user_groups` contains a cached list of all the groups the user is 
        a member of.  This function will recache this list (resulting in queries to the backend 
        database).  
        """
        self._whogrps = frozenset(self.groups.select_ids_for_user(self._who) + PUBLIC_GROUP)

    def create_record(self, owner: str = None, shoulder: str=None) -> ProjectRecord:
        """
        create (and save) and return a new project record.  A new unique identifier should be assigned
        to the record.

        :param str    owner:  the ID of the user that should be registered as the owner.  If not 
                              specified, the value of :py:property:`user_id` will be assumed.  
        :param str shoulder:  the identifier shoulder prefix to create the new ID with.  
                              (The implementation should ensure that the requested shoulder is 
                              recognized and that requesting user is authorized to request 
                              the shoulder.)
        :raise ValueException:  if :py:property:`user_id` corresponds to an anonymous user and `owner`
                              is not specified.
        """
        if not owner:
            if self._who == ANONYMOUS:
                raise ValueException("create_record(): owner must be specified when client is anonymous")
            owner = self._who
        if not shoulder:
            shoulder = self._default_shoulder()
        if not self._authorized_create(shoulder, owner):
            raise NotAuthorized(owner, "create")

        rec = self._new_record(self._mint_id(shoulder))
        rec.save()
        return rec

    def _default_shoulder(self):
        out = self._cfg.get("default_shoulder")
        if not out:
            raise ConfigurationException("Missing required configuration parameter: default_shoulder")
        return out

    def _authorized_create(self, shoulder, who):
        shldrs = set(self._cfg.get("allowed_shoulders", []))
        defshldr = self._cfg.get("default_shoulder")
        if defshldr:
            shldrs.add(defshldr)
        return shoulder in shldrs

    def _mint_id(self, shoulder):
        """
        create and register a new identifier that can be attached to a new project record
        :param str shoulder:   the shoulder to prefix to the identifier.  The value usually controls
                               how the identifier is formed.  
        """
        return "{0}-{1:04}".format(shoulder, self._next_recnum(shoulder))

    @abstractmethod
    def _next_recnum(self, shoulder):
        """
        return an unused record number that can be used to mint a new identifier.  This is called 
        by the default implementation of :py:method:`_mint_id`.  Typically, each shoulder has its 
        own unique sequence of numbers associated with it.  
        :param str shoulder:  the shoulder that the record number will be combined with
        """
        raise NotImplementedError()

    @abstractmethod
    def _new_record(self, id):
        """
        return a new ProjectRecord instance with the given identifier assigned to it.  Generally, 
        this record should not be committed yet.
        """
        raise NotImplementedError()

    def record_for(self, id: str, perm: str=READ) -> ProjectRecord:
        """
        return a single project record by its identifier.  The record is only 
        returned if the user this client is attached to is authorized to access the record with 
        the given permission.
        
        :param str   id:  the identifier for the record of interest
        :param str perm:  the permission type that the user must be authorized for in order 
        """
        out = self._get_from_coll(self._projcoll, id)
        if not out:
            return None
        out = ProjectRecord(self._projcoll, out, self)
        if (not out.owner or self._who != out.owner) and not self.authorized(perm):
            raise NotAuthorized(self._who, perm)
        return out

    @abstractmethod
    def select_records(self, perm: Permissions=OWN) -> List[ProjectRecord]:
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

    @property
    def native(self):
        return self._native

    def groups(self) -> DBGroups:
        """
        access to the management of groups
        """
        return self._dbgroups

    def people(self) -> DBPeople:
        """
        access to the people collection
        """
        return None

    @abstractmethod
    def _upsert(self, coll: str, recdata: Mapping):
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
    def _get_from_coll(self, collname, id) => MutableMapping:
        """
        return a record with a given identifier from the specified collection
        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param str id:         the identifier for the record of interest
        """
        raise NotImplementedError()

    @abstractmethod
    def _select_from_coll(self, collname, **constraints) => Iterator[MutableMapping]:
        """
        return an iterator to the records from a specified collectino that match the set of 
        given constraints.

        :param str collname:   the logical name of the database collection (e.g. table, etc.) to pull 
                               the record from.  
        :param dict constraints:  the constraints on properties in the record.  The returned records 
                               must all have properties matching the keys in the given constraint 
                               dictionary with corresponding matching values 
        """
        raise NotImplementedError()
    
    @abstractmethod
    def _select_prop_contains(self, collname, prop, target) => Iterator[MutableMapping]:
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
        

class ProtectedRecord(ABC):
    """
    a base class for records that have ACLs attached to them
    """

    def __init__(self, servicetype: str, recdata: Mapping, dbclient: DBClient=None):
        """
        initialize the record with a dictionary retrieved from the underlying project collection.  
        The dictionary must include an `id` property with a valid ID value.
        """
        if not servicetype:
            raise ValueError("ProtectedRecord(): must set service type (servicetype)")
        self._coll = servicetype
        self._cli = dbclient
        if not recdata.get('id'):
            raise ValueError("Record data is missing its 'id' property")
        self._data = self._initialize(recdata)
        self._acls = ACLs(self.data.get("acls", {}))

    def _initialize(self, recdata: MutableMapping) => MutableMapping:
        """
        initialize any missing data in the raw record data constituting the content of the record.  
        The implementation is allowed to update the input dictionary directly.  

        This default implimentation ensures that the record contains a minimal `acls` property

        :return: an combination of the given data and defaults
                 :rtype: MutableMapping
        """
        if not recdata.get('acls'):
            recdata['acls'] = {}
        for perm in OWN:
            if perm not in recdata:
                recdata['acls'][perm] = []

    @property
    def id(self):
        """
        the unique identifier for the record
        """
        return self._data.get('id')

    @property
    def owner(self):
        return self._data.get('owner', "")


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
        if (not self.owner or self._cli._who != self.owner) and not self.authorized(WRITE):
            raise NotAuthorized(self._cli._who, "update")
        self._cli._upsert(self._coll, self.as_dict())

    def authorized(self, perm: Permissions, who: str = None):
        """
        return True if the given user has the specified permission to commit an action on this record.
        The action is typically one of the base action permissions defined in this module, but it can 
        also be a custom permission suppported by this type of record.  This implementation will take 
        into account all of the groups the user is a member of.

        :param str|Sequence[str]|Set[str] perm:  a single permission or a list or set of permissions that 
                         the user must have to complete the requested action.  If a list of permissions 
                         is given, the user `who` must have all of the permissions.
        :param str who:  the identifier for the user attempting the action; if not given, the user id
                         attached to the DBClient is assumed.
        """
        if not who:
            who = self._cli._who
        if who in self._cli._cfg.get("superusers", []):
            return True
            
        if isinstance(perm, str):
            perm = set(perm)
        elif isinstance(perm, list):
            perm = set(perm)

        idents = [self._cli._who] + list(self._cli.user_groups)
        for p in perm:
            if not self.acls._granted(p, idents):
                return False
        return True

    def validate(self, errs=None, data=None) => List[str]:
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

        for perm in OWN:
            if perm not in data['acls']:
                errs.append("ACLs: missing permmission: "+perm)
            elif not isinstance(data['acls'][perm], list):
                errs.append("ACL '{}': not a list".format(perm))

        return errs

    def to_dict(self):
        return dict(self._data)
        
        
class DBGroups(ABC):
    """
    an interface for creating and using user groups.  Each group has a unique identifier assigned to it
    and holds a list of user (and/or group) identities indicating the members of the groups.  In addition
    to its unique identifier, a group also has a mnumonic name given to it by the group's owner; the 
    group name need not be globally unique, but it should be unique within the owner's namespace.  
    """

    def __init__(self, dbclient: DBClient, idshoulder: str=DEF_GROUPS_SHOULDER):
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
                             underlying :py:class:`DBClient` will be used.  
        :raises AlreadyExists: if the user has already defined a group with this name
        """
        if not foruser:
            foruser = self._cli._who
        if not self._cli._authorized_create(self._shlder, foruser):
            raise NotAuthorized(foruser, "create group")

        if self.get_by_name(name, foruser):
            raise AlreadyExists("User {} has already defined a group with name={}".format(foruser, name))
        
        out = Group({
            "id": self._mint_id(self._shlder, name, foruser),
            "name": name,
            "owner": foruser,
            "members": [ foruser ],
            "acls": {
                ADMIN:  foruser,
                READ:   foruser,
                WRITE:  foruser,
                DELETE: foruser
            }
        }, self._cli)
        out.save()

    def _mint_id(self, shoulder, name, owner):
        """
        create and register a new identifier that can be assigned to a new group
        :param str shoulder:   the shoulder to prefix to the identifier.  The value usually controls
                               how the identifier is formed.  
        """
        return "{}:{}:{}".format(shoulder, name, owner)

    def get(self, gid: str) => Group:
        """
        return the group by its given group identifier
        """
        m = self._cli._get_from_coll(GROUPS_COLL, gid)
        if not m:
            return None
        m = Group(m)
        if (m.owner and self._cli._who == m.owner) or m.authorized(who, READ):
            return m
        raise NotAuthorized(id, "read")

    def __getitem__(self, id) => Group:
        out = self.get(id)
        if not out:
            raise KeyError(id)
        return out

    def get_by_name(self, name: str, owner: str) => Group:
        """
        return the group assigned the given name by its owner.  This assumes that the given owner 
        has only created one group with the given name.  
        """
        matches = self._cli._select_from_coll(GROUPS_COLL, name=name, owner=owner)
        for m in matches:
            if (m.owner and who == m.owner) or m.authorized(self._cli._who, READ):
                return m
        return None

    def select_ids_for_user(self, id):
        """
        return all the groups that a user (or a group) is a member of.  This implementation will 
        resolve the groups that the user is indirectly a member of--i.e. a user's group itself is a 
        member of another group.  
        """
        checked = set()
        out = set(self._cli._select_prop_contains(GROUPS_COLL, 'member', id))
        follow = list(out)
        while len(follow) > 0:
            g = follow.pop(0)
            if g not in checked:
                add = set(slef._cli._select_prop_contains(GROUPS_COLL, 'member', g))
                out |= add
                checked.add(g)
                follow |= add.difference(checked)

        return out

    def delete_group(self, gid: str) => bool:
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
        if (not g.owner or g.owner != self._cli._who) and not g.authorized(DELETE):
            raise NotAuthorized(gid, "delete group")

        self._cli._delete_from(GROUPS_COLL, gid)
        return True


class Group(ProtectedRecord):
    """
    an updatable representation of a group.
    """

    def __init__(self, recdata: MutableMapping, dbclient: DBClient=None):
        """
        initialize the group record with a dictionary retrieved from the underlying group database 
        collection.  The dictionary must include an `id` property with a valid ID value.
        """
        super(Group, self).__init__(GROUPS_COLL, recdata, dbclient)

    def _initialize(self, recdata: MutableMapping):
        out = super(Group, self)._initialize(recdata)
        if 'members' not in out:
            recdata['members'] = []
        
    def validate(self, errs=None, data=None) => List[str]:
        """
        validate this record and return a list of error statements about its validity or an empty list
        if the record is valid.
        """
        # check the acls property
        errs = super(Group, self).validate(data, errs)

        for prop in "id name owner members".split():
            if not data.get(prop):
                errs.append("'{}' property not set".format(prop))

        for prop in "id name owner".split():
            if not isinstance(data['id'], str):
                errs.append("'{}' property: not a str".format(prop))
        if not isinstance(data['members'], list):
            errs.append("'members' property: not a list")

        return errs

    def add_member(self, *memids):
        """
        add members to this group (if they aren't already members)
        :param str memids:  the identities of the users to be added to the group
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to add members
        """
        if (not self.owner or self._cli._who != self.owner) and not self.authorized(WRITE):
            raise NotAuthorized(self._cli._who, "add member")

        for id in memids:
            if id not in self._data['members']:
                self._data['members'].append(id)

    def remove_member(self, *memids):
        """
        remove members from this group; any given ids that are not currently members are ignored.
        :param str memids:  the identities of the users to be removed from the group
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to remove members
        """
        if (not self.owner or self._cli._who != self.owner) and not self.authorized(who, WRITE):
            raise NotAuthorized(self._cli._who, "remove member")

        for id in memids:
            if id in self._data['members']:
                self._data['members'].remove(id)



class ProjectRecord(ProtectedRecord):
    """
    a single record from the project collection representing one project created by the user
    """

    def __init__(self, projcoll: str, recdata: Mapping, dbclient: DBClient=None):
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
        if 'created' not in rec:
            rec['created'] = time.time()
        if 'deactivated' not in rec:
            # Should be None or a date
            rec['deactivated'] = None

        self._initialize_data(rec)
        self._initialize_meta(rec)
                
    def _initialize_data(self, recdata: MutableMapping):
        """
        add default data to the given dictionary of application-specific project data.  
        """
        return self._data["data"]

    def _initialize_meta(self, recmeta: MutableMapping):
        """
        add default data to the given dictionary of application-specific project metadata
        """
        return self._data["meta"]

    @property
    def created(self) -> float:
        """
        the epoch timestamp indicating when this record was first corrected
        """
        return self._data.get('created', 0)

    @property
    def created_date(self) -> str:
        """
        the creation timestamp formatted as an ISO string
        """
        return datetime.fromtimestamp(self.created).isoformat()

    @property
    def data(self) -> MutableMapping:
        """
        the application-specific data for this record.  This dictionary contains data that is generally 
        updateable directly by the user (e.g. via the GUI interface).  The expected properties for
        are determined by the application.
        """
        return self._data['data']

    @property
    def meta(self) -> MutableMapping:
        """
        the application-specific metadata for this record.  This dictionary contains data that is generally
        not directly editable by the application, but which the application must track in order to manage
        the updating process.  The expected properties for this dictionary are determined by the application.
        """
        return self._data['meta']

class ACLs:
    """
    a class for accessing and manipulating access control lists on a record
    """

    def __init__(self, acldata: MutableMapping=None, forrec: ProtectedRecord=None):
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
        :param str id:   the identities of the users the permission should be granted to 
        :raise NotAuthorized:  if the user attached to the underlying :py:class:`DBClient` is not 
                               authorized to grant this permission
        """
        if (not self._rec.owner or self._rec._cli._who != self._rec.owner) and \
           not self._rec.authorized(self._rec._cli._who, ADMIN):
            raise NotAuthorized(self._rec._cli._who, "grant permission")

        if perm_name not in self._perms:
            self._perms[perm_name] = []
        for id in ids:
            if id not in self._perms[perm_name]:
                self._perms[perm_name].append(id)

    def revoke_perm_from(self, who, perm_name, *ids):
        """
        remove the given identities from the list having the given permission.  For each given identity 
        that does not currently have the permission, nothing is done.  
        """
        if (not self._rec.owner or self._rec._cli._who != self._rec.owner) and \
           not self._rec.authorized(self._rec._cli._who, ADMIN):
            raise NotAuthorized(self._rec._cli._who, "revoke permission")

        if perm_name not in self._perms:
            return
        self._perms[perm_name] = list(set(self._perms[perm_name]).difference(ids))


    def _granted(self, perm_name, ids=[]):
        """
        return True if any of the given identities have the specified permission.  Normally, this will be 
        a list including a user identity and all the group identities that is user is a member of; however, 
        this is neither required nor checked by this implementation.

        This should be considered lowlevel; consider using :py:method:`authorized` instead which resolves 
        a users membership.  
        """
        return len(set(self._perms[perm_name]).intersection(ids)) > 0
        
