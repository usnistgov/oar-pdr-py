"""
An implementation of the dbio interface based on a simple in-memory look-up.  

This is provided primarily for testing purposes
"""
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base
from .notifier import Notifier

from nistoar.base.config import merge_config

class InMemoryDBClient(base.DBClient):
    """
    an in-memory DBClient implementation 
    """

    def __init__(self, dbdata: Mapping, config: Mapping, projcoll: str ,foruser: str = base.ANONYMOUS, notification_server: Notifier = None):
        self._db = dbdata
        super(InMemoryDBClient, self).__init__(config, projcoll, self._db, foruser,notification_server)

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
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)
        for rec in self._db[self._projcoll].values():
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

                
class InMemoryDBClientFactory(base.DBClientFactory):
    """
    a DBClientFactory that creates InMemoryDBClient instances in which records are stored in data
    structures kept in memory.  Records remain in memory for the life of the factory and all the 
    clients it creates.  
    """

    def __init__(self, config: Mapping, _dbdata = None, notification_server: Notifier = None):
        """
        Create the factory with the given configuration.

        :param dict  config:  the configuration parameters used to configure clients
        :param dict _dbdata:  the initial data for the database.  (Note: internal knowledge of 
                              of the in-memory data structure required to use this input.)  If 
                              not provided, an empty database is created.
        """
        super(InMemoryDBClientFactory, self).__init__(config, notification_server)
        self.notification_server = notification_server
        self._db = {
            base.DAP_PROJECTS: {},
            base.DMP_PROJECTS: {},
            base.GROUPS_COLL: {},
            base.PEOPLE_COLL: {},
            "nextnum": {}
        }
        if _dbdata:
            self._db.update(deepcopy(_dbdata))
            

    def create_client(self, servicetype: str, config: Mapping={}, foruser: str = base.ANONYMOUS):
        
        cfg = merge_config(config, deepcopy(self._cfg))
        if servicetype not in self._db:
            self._db[servicetype] = {}
        return InMemoryDBClient(self._db, cfg, servicetype, foruser, self.notification_server)
        
