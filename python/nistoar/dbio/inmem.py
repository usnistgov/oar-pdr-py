"""
An implementation of the dbio interface based on a simple in-memory look-up.  

This is provided primarily for testing purposes
"""
from . import base

class InMemoryDBClient(base.DBClient):
    """
    an in-memory DBClient implementation 
    """

    def __init__(self, projcoll, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS):
        super(InMemoryDBClient, self).__init__(config, projcoll, None, foruser)
        self._db = {
            self._projcoll: {},
            base.GROUPS_COLL: {},
            base.PEOPLE_COLL: {},
            "nextnum": {}
        }

    def _next_recnum(self, shoulder):
        if shoulder not in self._db['nextnum']:
            self._db['nextnum'][shoulder] = 0
        self._db['nextnum'][shoulder] += 1
        return self._db['nextnum'][shoulder]

    def _new_record(self, id):
        return ProjectRecord(self._projcoll, {"id": id}, self)

    def _get_from_coll(self, collname, id) => MutableMapping:
        return self._db.get(collname, {}).get(id)

    def _select_from_coll(self, collname, **constraints) => Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            cancel = False
            for ck, cv in constraints.items():
                if rec.get(ck) != cv:
                    cancel = True
                    break
            if cancel:
                continue
            yield rec

    def _select_prop_contains(self, collname, prop, target) => Iterator[MutableMapping]:
        for rec in self._db.get(collname, {}).values():
            if prop in rec and isinstance(rec[prop], (list, tuple)) and target in rec[prop]:
                yield rec

    def _delete_from(self, collname, id):
        if id in self._db[collname]:
            del self._db[collname][id]
            return True
        return False

    def _upsert(self, coll: str, recdata: Mapping):
        self._db[coll][recdata['id']] = recdata

    def select_records(self, perm: Permissions=OWN) -> List[ProjectRecord]:
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)
        for rec in self._db[self._projcoll]:
            rec = ProjectRecord(self._projcoll, rec, self)
            for p in perm:
                if rec.authorized(p):
                    yield rec
                    break
                
                

        
