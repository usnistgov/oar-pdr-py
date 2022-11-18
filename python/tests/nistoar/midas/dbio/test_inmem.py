import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base

class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.cfg = { "goob": "gurn" }
        self.fact = inmem.InMemoryDBClientFactory(self.cfg, { "nextnum": { "hank": 2 }})

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertTrue(self.fact._db)
        self.assertEqual(self.fact._db.get(base.DRAFT_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.DMP_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.GROUPS_COLL), {})
        self.assertEqual(self.fact._db.get(base.PEOPLE_COLL), {})
        self.assertEqual(self.fact._db.get("nextnum"), {"hank": 2})

    def test_create_client(self):
        cli = self.fact.create_client(base.DMP_PROJECTS, {}, "ava1")
        self.assertEqual(cli._db, self.fact._db)
        self.assertEqual(cli._cfg, self.fact._cfg)
        self.assertEqual(cli._projcoll, base.DMP_PROJECTS)
        self.assertEqual(cli._who, "ava1")
        self.assertIsNone(cli._whogrps)
        self.assertIs(cli._native, self.fact._db)
        self.assertIsNotNone(cli._dbgroups)
        

class TestInMemoryDBClient(test.TestCase):

    def setUp(self):
        self.cfg = {}
        self.user = "nist0:ava1"
        self.cli = inmem.InMemoryDBClientFactory({}).create_client(base.DMP_PROJECTS, {}, self.user)

    def test_next_recnum(self):
        self.assertEqual(self.cli._next_recnum("goob"), 1)
        self.assertEqual(self.cli._next_recnum("goob"), 2)
        self.assertEqual(self.cli._next_recnum("goob"), 3)
        self.assertEqual(self.cli._next_recnum("gary"), 1)
        self.assertEqual(self.cli._next_recnum("goober"), 1)
        self.assertEqual(self.cli._next_recnum("gary"), 2)

    def test_get_from_coll(self):
        # test query on non-existent collection
        self.assertIsNone(self.cli._get_from_coll("alice", "p:bob"))

        # test query on existing but empty collection
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {"id": "p:bob", "owner": "alice"}
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})
        
        self.cli._db[base.GROUPS_COLL]["p:mine"] = {"id": "p:mine", "owner": "p:bob"}
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"),
                         {"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

        self.assertIsNone(self.cli._get_from_coll("nextnum", "goob"))
        self.cli._db["nextnum"] = {"goob": 0}
        self.assertEqual(self.cli._get_from_coll("nextnum", "goob"), 0)

    def test_select_from_coll(self):
        # test query on non-existent collection
        it = self.cli._select_from_coll("alice", owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on existing but empty collection
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {"id": "p:bob", "owner": "alice", "hobby": "whittling"}
        it = self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})

        self.cli._db[base.GROUPS_COLL]["p:mine"] = {"id": "p:mine", "owner": "p:bob", "hobby": "whittling"}
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, hobby="whittling"))
        self.assertEqual(len(recs), 2)

    def test_select_prop_contains(self):
        # test query on non-existent collection
        it = self.cli._select_prop_contains("alice", "hobbies", "whittling")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on existing but empty collection
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {"id": "p:bob", "members": ["p:bob"]}
        self.cli._db[base.GROUPS_COLL]["stars"] = {"id": "stars", "members": ["p:tom", "p:bob"]}
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        self.cli._db[base.GROUPS_COLL]["p:bob"]["members"].append("alice")
        it = self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "members": ["p:bob", "alice"]})
        
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        self.assertEqual(set([r.get('id') for r in recs]), set("p:bob stars".split()))

    def test_delete_from(self):
        # test query on non-existent collection
        self.assertFalse(self.cli._delete_from("alice", "p:bob"))

        # test query on existing but empty collection
        self.assertFalse(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))

        self.cli._db[base.GROUPS_COLL]["p:bob"] = {"id": "p:bob", "members": ["p:bob"]}
        self.cli._db[base.GROUPS_COLL]["stars"] = {"id": "stars", "members": ["p:tom", "p:bob"]}
        self.assertTrue(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))
        self.assertNotIn("p:bob", self.cli._db[base.GROUPS_COLL])
        self.assertIn("stars", self.cli._db[base.GROUPS_COLL])

    def test_upsert(self):
        # test on a non-existent collection
        self.assertIsNone(self.cli._get_from_coll("about", "p:bob"))
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))

        self.assertTrue(self.cli._upsert("about",
                                         {"id": "p:bob", "owner": "alice", "hobby": "whittling"}))   # insert
        self.assertEqual(self.cli._get_from_coll("about", "p:bob"), 
                         {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))
                         
        self.assertFalse(self.cli._upsert("about",
                                          {"id": "p:bob", "owner": "alice", "hobby": "knitting"}))   # update
        self.assertEqual(self.cli._get_from_coll("about", "p:bob"), 
                         {"id": "p:bob", "owner": "alice", "hobby": "knitting"})
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))

        # test on an existing collection
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "g:friends"))
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "stars"))

        self.assertTrue(self.cli._upsert(base.GROUPS_COLL, {"id": "p:bob", "members": ["p:bob"]}))
        rec = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec, {"id": "p:bob", "members": ["p:bob"]})
        rec['members'].append("alice")
        self.cli._upsert(base.GROUPS_COLL, rec)
        rec2 = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec2, {"id": "p:bob", "members": ["p:bob", "alice"]})

    def test_select_records(self):
        # test query on existing but empty collection
        it = self.cli.select_records(base.ACLs.READ)
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # inject some data into the database
        id = "pdr0:0002"
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": id}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": "goob", "owner": "alice"}, self.cli)
        self.cli._db[base.DMP_PROJECTS]["goob"] = rec.to_dict()

        recs = list(self.cli.select_records(base.ACLs.READ))
        self.assertEqual(len(recs), 1)
        self.assertTrue(isinstance(recs[0], base.ProjectRecord))
        self.assertEqual(recs[0].id, id)

        
        
                         
if __name__ == '__main__':
    test.main()
        

        
