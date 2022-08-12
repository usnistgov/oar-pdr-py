"""
The abstract interface for interacting with the database.  

This interface is based on the following model:

  *  Each service (drafting, DMPs, etc.) has its own collection that extends on a common base model
  *  Each *record* in the collection represents a "project" that a user is working on via the service
  *  A record can be expressed as a Python dictionary which can be exported into JSON

"""
import time
from abc import ABC, ABCMeta, abstractmethod, abstractproperty
from collections.abc import Mapping, MutableMapping
from collections import OrderedDict
from typing import Union
from enum import Enum
from datetime import datetime

from nistoar.base.config import ConfigurationException

DRAFT_PROJECTS = "draft"
DMP_PROJECTS   = "dmp"

# Permissions
READ      = 'read'
WRITE     = 'write'
READWRITE = WRITE
ADMIN     = 'admin'
DELETE    = 'delete'
OWN       = (READ, WRITE, ADMIN, DELETE,)

Permissions = Union[str, Sequence[str]]

class DBClientFactory(ABC):
    """
    an abstract class for creating client connections to the database
    """

    def createClient(self, servicetype: str):
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

    def __init__(self, config: Mapping, nativeclient=None):
        self._cfg = config
        self._native = nativeclient

    @abstractmethod
    def create_record_for(self, owner: str, shoulder: str=None, byuser: str=None) -> ProjectRecord:
        """
        create (and save) and return a new project record.  A new unique identifier should be assigned
        to the record.

        :param str    owner:  the ID of the user that should be registered as the owner. 
        :param str shoulder:  the identifier shoulder prefix to create the new ID with.  
                              (The implementation should ensure that the requested shoulder is 
                               recognized and that requesting user is authorized to request 
                               the shoulder.)
        :param str   byuser:  The identifier of the user requesting creation whose authorizations
                              should be checked to see if the request is allowed.  If None, the 
                              owner is assumed to be the requesting user.
        """
        if not byuser:
            byuser = owner
        if not shoulder:
            shoulder = self._default_shoulder()
        if not self._authorized_create(byuser, shoulder):
            raise NotAuthorized(owner, "create")

        rec = self._new_record(self._mint_id(shoulder))
        rec.commit()
        return rec

    def _default_shoulder(self):
        out = self._cfg.get("default_shoulder")
        if not out:
            raise ConfigurationException("Missing required configuration parameter: default_shoulder")
        return out

    def _authorized_create(self, who, shoulder):
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

    @abstractmethod
    def record_for(self, id: str, user: str=None, perm=READ) -> ProjectRecord:
        """
        return a single project record by its identifier.  If ``user`` is provided, the record is only 
        returned if the named user is authorized to access the record with the given permission.
        
        :param str   id:  the identifier for the record of interest
        :param str user:  the identity of the user that wants access to the record.  If None, the 
                          record is always returned if it exists; no authorization check is made.
        :param str perm:  the permission type that the user must be authorized for in order 
        """
        raise NotImplementedError()

    @abstractmethod
    def select_records_for(self, user: str, perm: Permissions=OWN) -> List[ProjectRecord]:
        """
        return a list of project records for which the given user has at least one of the given 
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

    @abstractproperty
    def groups(self) -> DBGroups:
        """
        access to the management of groups
        """
        pass

    @abstractproperty
    def people(self) -> DBPeople:
        """
        access to the people collection
        """
        pass

class DBGroups(ABC):
    """
    an interface for creating and using user groups
    """

    def __init__(self, dbclient: DBClient):
        self._cli = dbclient

    @property
    def native(self):
        return self._cli._native

    def create_group(self, name: str, foruser: str):
        """
        create a new group for the given user.  
        :param str name:     the name of the group to create
        :param str foruser:  the user to create the group for.  This user will be set as the group's creator
                             and administrator
        """
        pass

    def get(self, id: str):
        """
        return the group by its given group identifier
        """
        pass

    def get_by_name(self, name: str, creator: str):
        """
        return the group assigned the given name by its creator.
        """
        pass


class ProjectRecord(ABC):
    """
    a single record from the project collection representing one project created by the user
    """

    def __init__(self, recdata: Mapping, dbclient: DBClient=None):
        """
        initialize the record with a dictionary retrieved from the underlying project collection.  
        The dictionary must include an `id` property with a valid ID value.
        """
        self._cli = dbclient
        if not recdata.get('id'):
            raise ValueError("Record data is missing its 'id' property")
        self._data = self._initialize(recdata)
        self._acls = ACLs(self.data.get("acls", {}))

    def _initialize(self, rec) -> Mapping:
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

        if 'acls' not in rec:
            rec['acls'] = {}
        for perm in OWN:
            if perm not in rec['acls']:
                rec['acls'][perm] = []
                if rec.get('owner') and rec.get('owner') not in rec['acls'][perm]:
                    rec['acls'][perm].append(owner)

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
    def id(self):
        """
        the unique identifier for the record
        """
        return self._data.get('id')

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

    @property
    def acls(self) -> ACLs:
        """
        An object for accessing and updating the access control lists (ACLs) for this record
        """
        return self._acls

class ACLs:
    """
    a class for accessing and manipulating access control lists on a record
    """

    def __init__(self, acldata: MutableMapping=None, projrec: ProjectRecord=None):
        """
        intialize the object from raw ACL data 
        :param MutableMapping acldata:  the raw ACL data as returned from the record store as a dictionary
        :param ProjectRecord  projrec:  the record object that the ACLs apply to.  This will be used as 
                                          needed to interact with the backend record store
        """
        if not acldata:
            acldata = {}
        self._perms = acldata
        self._rec = projrec

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
        """
        if perm_name not in self._perms:
            self._perms[perm_name] = []
        for id in ids:
            if id not in self._perms[perm_name]:
                self._perms[perm_name].append(id)

    def revoke_perm_from(self, perm_name, *ids):
        """
        remove the given identities from the list having the given permission.  For each given identity 
        that does not currently have the permission, nothing is done.  
        """
        if perm_name not in self._perms:
            return
        self._perms[perm_name] = list(set(self._perms[perm_name]).difference(ids))

    def granted(self, perm_name, ids=[]):
        """
        return True if any of the given identities have the specified permission.  Normally, this will be 
        a list including a user identity and all the group identities that is user is a member of; however, 
        this is neither required nor checked by this implementation.

        This should be considered lowlevel; consider using :py:method:`authorized` instead.  
        """
        return len(set(self._perms[perm_name]).intersection(ids)) > 0
        
