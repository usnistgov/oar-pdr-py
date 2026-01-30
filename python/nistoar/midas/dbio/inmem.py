"""
An implementation of the dbio interface based on a simple in-memory look-up.  

This is provided primarily for testing purposes
"""
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List,Sequence
from . import base
from .notifier import DBIOClientNotifier

from nistoar.base.config import merge_config
from nistoar.nsd.service import PeopleService, MongoPeopleService, create_people_service

SUPPORTED_CONSTRAINTS = set("name id owner status_state".split())

class InMemoryDBClient(base.DBClient):
    """
    an in-memory DBClient implementation 
    """

    def __init__(self, dbdata: Mapping, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS,
                 peopsvc: PeopleService = None, notifier: DBIOClientNotifier = None):
        """
        initialize this client.
        :param dict  dbdata:  the initial in-memory data store to use.  The structure of this dictionary
                              is specific to this implementation; thus except when provided as an empty
                              dictionary (representing an empty database), this should be a dictionary 
                              copied from another instance of this client.
        :param dict  config:  the configuration data for the client
        :param str projcoll:  the type of project to connect with (i.e. the project collection name)
        :param nativeclient:  where applicable, the native client object to use to connect the back
                              end database.  The type and use of this client is implementation-specific
        :param str  foruser:  the user identity to connect as.  This will control what records are 
                              accessible via this instance's methods.
        :param PeopleService peopsvc:  a PeopleService to incorporate into this client
        :param DBIOClientNotifier notifier:  a DBIOClientNotifier to use to alert DBIO clients about 
                              updates to the DBIO data.
        """
        self._db = dbdata
        super(InMemoryDBClient, self).__init__(config, projcoll, self._db, foruser, peopsvc, notifier)

    def reset(self, dbdata: Mapping = {}):
        """
        Reset the database to some initial state, characterized by the given dbdata.  This is intended 
        for use in unit tests that need to clear contents of the database after each test or suite.
        """
        self._db = dbdata

    def _next_recnum(self, shoulder):
        if shoulder not in self._db['nextnum']:
            self._db['nextnum'][shoulder] = 0
        self._db['nextnum'][shoulder] += 1
        return self._db['nextnum'][shoulder]

    def _try_push_recnum(self, shoulder, recnum):
        n = self._db['nextnum'].get(shoulder, -1)
        if n >= 0 and n == recnum:
            self._db['nextnum'][shoulder] -= 1

    def _get_from_coll(self, collname, id) -> MutableMapping:
        return deepcopy(self._db.get(collname, {}).get(id))

    def _select_from_coll(self, collname, incl_deact=False, **constraints) -> Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            if rec.get('deactivated') and not incl_deact:
                continue
            cancel = False
            for ck, cv in constraints.items():
                if rec.get(ck) != cv:
                    cancel = True
                    break
            if cancel:
                continue
            yield deepcopy(rec)

    def _select_prop_contains(self, collname, prop, target, incl_deact=False) -> Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            if rec.get('deactivated') and not incl_deact:
                continue
            if prop in rec and isinstance(rec[prop], (list, tuple)) and target in rec[prop]:
                yield deepcopy(rec)

    def _delete_from(self, collname, id):
        if collname in self._db and id in self._db[collname]:
            del self._db[collname][id]
            shldr, num = self._parse_id(id)
            if shldr:
                self._try_push_recnum(shldr, num)
            return True
        return False

    def _upsert(self, coll: str, recdata: Mapping) -> bool:
        if coll not in self._db:
            self._db[coll] = {}
        exists = bool(self._db[coll].get(recdata['id']))
        self._db[coll][recdata['id']] = deepcopy(recdata)
        return not exists

    def select_records(self, perm: base.Permissions=base.ACLs.OWN, **cnsts) -> Iterator[base.ProjectRecord]:
        """
        return an iterator of project records for which the given user has at least one of the given 
        permissions and matches additional optional search constraints

        :param str       user:  the identity of the user that wants access to the records.  
        :param str|[str] perm:  the permissions the user requires for the selected record.  For
                                each record returned the user will have at least one of these
                                permissions.  The value can either be a single permission value
                                (a str) or a list/tuple of permissions
        :param list _cnsts_:    an additional constraint that will match any record with a property
                                refered to by the constraint name if its value matches any of those 
                                given in the constraint's value list.  Supported _constraint_ names 
                                include ``name``, ``id``, ``status.state``, and ``owner``.  Particular 
                                implementations may support additional properties; any unsupported 
                                constraints will be ignored.  Note that multiple constraints are 
                                logically AND-ed together; that is, a matched record must match at least
                                one value from each constraint value list.
        """
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)

        for prop in cnsts:
            if cnsts.get(prop) and not isinstance(cnsts[prop], (list, tuple)):
                cnsts[prop] = [ cnsts[prop] ]

        # filter first on the raw record data (for ease); assumes records in storage are already
        # sufficiently initialized.
        for rec in self._db[self._projcoll].values():
            if cnsts:
                # filter out records not matched by cnsts
                matched = True
                for prop in SUPPORTED_CONSTRAINTS:
                    vals = cnsts.get(prop)
                    if not vals:
                        continue

                    if prop == "status_state":
                        if rec.get('status', {}).get('state') not in vals:
                            matched = False
                    elif rec.get(prop) not in vals:
                        matched = False
                if not matched:
                    continue
                
            rec = base.ProjectRecord(self._projcoll, rec, self)

            for p in perm:
                if rec.authorized(p):
                    yield deepcopy(rec)
                    break
    
    def adv_select_records(self, filter:dict,
                           perm: base.Permissions=base.ACLs.OWN,) -> Iterator[base.ProjectRecord]:
        if(base.DBClient.check_query_structure(filter) == True):
            try:
                if isinstance(perm, str):
                    perm = [perm]
                if isinstance(perm, (list, tuple)):
                    perm = set(perm)
                for rec in self._db[self._projcoll].values():
                    rec = base.ProjectRecord(self._projcoll, rec, self)
                    for p in perm:
                        if(rec.authorized(p)):
                            if (rec.searched(filter) == True):
                                yield deepcopy(rec)
                                break
            except Exception as ex:
                raise base.DBIOException(
                    "Failed while selecting records: " + str(ex), cause=ex)
        else:
            raise SyntaxError('Wrong query format')

    def _save_action_data(self, actdata: Mapping):
        if 'subject' not in actdata:
            raise ValueError("_save_action_data(): Missing subject property in action data")
        id = actdata['subject']
        if base.PROV_ACT_LOG not in self._db:
            self._db[base.PROV_ACT_LOG] = {}
        if id not in self._db[base.PROV_ACT_LOG]:
            self._db[base.PROV_ACT_LOG][id] = []
        self._db[base.PROV_ACT_LOG][id].append(actdata)
                
    def _select_actions_for(self, id: str) -> List[Mapping]:
        if base.PROV_ACT_LOG not in self._db or id not in self._db[base.PROV_ACT_LOG]:
            return []
        return deepcopy(self._db[base.PROV_ACT_LOG][id])

    def _delete_actions_for(self, id):
        if base.PROV_ACT_LOG not in self._db or id not in self._db[base.PROV_ACT_LOG]:
            return
        del self._db[base.PROV_ACT_LOG][id]

    def _save_history(self, histrec):
        if 'recid' not in histrec:
            raise ValueError("_save_history(): Missing recid property in history data")
        if 'history' not in self._db:
            self._db['history'] = {}
        if histrec['recid'] not in self._db['history']:
            self._db['history'][histrec['recid']] = []
        self._db['history'][histrec['recid']].append(histrec)

    def client_for(self, projcoll: str, foruser: str = None):
        """
        create a new DBClient using the same backend as this one but attached to a different collection
        (and possibly user).
        :param str projcol:  the project collection name
        :param str foruser:  the user this should be used on behalf of.  This controls what records the 
                             client has access to.
        """
        if not foruser:
            foruser = self.user_id
        return self.__class__(self._db, self._cfg, projcoll, foruser)


class InMemoryDBClientFactory(base.DBClientFactory):
    """
    a DBClientFactory that creates InMemoryDBClient instances in which records are stored in data
    structures kept in memory.  Records remain in memory for the life of the factory and all the 
    clients it creates.  
    """

    def __init__(self, config: Mapping, _dbdata = None):
        """
        Create the factory with the given configuration.

        :param dict  config:  the configuration parameters used to configure clients
        :param dict _dbdata:  the initial data for the database.  (Note: internal knowledge of 
                              of the in-memory data structure required to use this input.)  If 
                              not provided, an empty database is created.
        """
        super(InMemoryDBClientFactory, self).__init__(config)
        self._init_data = _dbdata
        self.init_db_data()

    def init_db_data(self, dbdata = None):
        """
        reset the internal database to a particular state.
        """
        self._db = {
            base.DAP_PROJECTS: {},
            base.DMP_PROJECTS: {},
            base.GROUPS_COLL: {},
            base.PEOPLE_COLL: {},
            "nextnum": {}
        }
        if not dbdata:
            dbdata = self._init_data
        if dbdata:
            self._db.update(deepcopy(dbdata))

    def reset(self):
        """
        reset the internal database to its original state at construction time
        """
        self.init_db_data()

    def create_client(self, servicetype: str, config: Mapping={}, foruser: str = base.ANONYMOUS):
        cfg = merge_config(config, deepcopy(self._cfg))
        if servicetype not in self._db:
            self._db[servicetype] = {}

        peopsvc = self._peopsvc
        if not peopsvc:
            peopsvc = self.create_people_service(cfg.get("people_service", {}))
        notifier = self._notifier
        if not notifier:
            notifier = self._create_notifier_from_config(cfg)

        return InMemoryDBClient(self._db, cfg, servicetype, foruser, peopsvc, notifier)
        
