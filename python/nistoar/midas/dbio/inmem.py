"""
An implementation of the dbio interface based on a simple in-memory look-up.  

This is provided primarily for testing purposes
"""
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base

from nistoar.base.config import merge_config

class InMemoryDBClient(base.DBClient):
    """
    an in-memory DBClient implementation 
    """

    def __init__(self, dbdata: Mapping, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS):
        self._db = dbdata
        super(InMemoryDBClient, self).__init__(config, projcoll, self._db, foruser)

    def _next_recnum(self, shoulder):
        if shoulder not in self._db['nextnum']:
            self._db['nextnum'][shoulder] = 0
        self._db['nextnum'][shoulder] += 1
        return self._db['nextnum'][shoulder]

    def _get_from_coll(self, collname, id) -> MutableMapping:
        return deepcopy(self._db.get(collname, {}).get(id))

    def _select_from_coll(self, collname, **constraints) -> Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            cancel = False
            for ck, cv in constraints.items():
                if rec.get(ck) != cv:
                    cancel = True
                    break
            if cancel:
                continue
            yield deepcopy(rec)

    def _select_prop_contains(self, collname, prop, target) -> Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            if prop in rec and isinstance(rec[prop], (list, tuple)) and target in rec[prop]:
                yield deepcopy(rec)

    def _delete_from(self, collname, id):
        if collname in self._db and id in self._db[collname]:
            del self._db[collname][id]
            return True
        return False

    def _upsert(self, coll: str, recdata: Mapping) -> bool:
        if coll not in self._db:
            self._db[coll] = {}
        exists = bool(self._db[coll].get(recdata['id']))
        self._db[coll][recdata['id']] = deepcopy(recdata)
        return not exists

    def select_records(self, perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
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
        self._db = {
            base.DRAFT_PROJECTS: {},
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
        return InMemoryDBClient(self._db, cfg, servicetype, foruser)
        
