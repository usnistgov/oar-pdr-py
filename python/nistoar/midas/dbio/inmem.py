"""
An implementation of the dbio interface based on a simple in-memory look-up.  

This is provided primarily for testing purposes
"""
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base

class InMemoryDBClient(base.DBClient):
    """
    an in-memory DBClient implementation 
    """

    def __init__(self, dbdata: Mapping, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS):
        super(InMemoryDBClient, self).__init__(config, projcoll, None, foruser)
        self._db = dbdata

    def _next_recnum(self, shoulder):
        if shoulder not in self._db['nextnum']:
            self._db['nextnum'][shoulder] = 0
        self._db['nextnum'][shoulder] += 1
        return self._db['nextnum'][shoulder]

    def _new_record(self, id):
        return base.ProjectRecord(self._projcoll, {"id": id}, self)

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

    def _upsert(self, coll: str, recdata: Mapping):
        if coll not in self._db:
            self._db[coll] = {}
        exists = bool(self._db[coll].get(recdata['id']))
        self._db[coll][recdata['id']] = recdata
        return not exists

    def select_records(self, perm: base.Permissions=base.OWN) -> Iterator[base.ProjectRecord]:
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
    a DBClientFactory the creates InMemoryDBClient 
    """

    def __init__(self, config, dbdata = None):
        super(InMemoryDBClientFactory, self).__init__(config)
        self._db = {
            base.DRAFT_PROJECTS: {},
            base.DMP_PROJECTS: {},
            base.GROUPS_COLL: {},
            base.PEOPLE_COLL: {},
            "nextnum": {}
        }
        if dbdata:
            self._db.update(deepcopy(dbdata))
            

    def create_client(self, servicetype: str, foruser: str = base.ANONYMOUS):
        if servicetype not in self._db:
            self._db[servicetype] = {}
        return InMemoryDBClient(self._db, self._cfg, servicetype, foruser)
        
