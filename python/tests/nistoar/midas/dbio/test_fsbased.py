import os, json, pdb, logging, tempfile
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import fsbased, base

class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_dbclient.", dir=".")
        self.cfg = { "goob": "gurn" }
        self.fact = fsbased.FSBasedDBClientFactory(self.cfg, self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertEqual(self.fact._dbroot, str(self.outdir.name))
        self.assertEqual(len([f for f in os.listdir(self.fact._dbroot) if not f.startswith(".")]), 0)

    def test_create_client(self):
        cli = self.fact.create_client(base.DMP_PROJECTS, {}, "ava1")
        self.assertEqual(cli._cfg, self.fact._cfg)
        self.assertEqual(cli._projcoll, base.DMP_PROJECTS)
        self.assertEqual(cli.user_id, "ava1")
        self.assertIsNone(cli._whogrps)
        self.assertEqual(os.path.normpath(str(cli._native)), self.outdir.name.lstrip("./"))
        self.assertIsNotNone(cli._dbgroups)

class TestFSBasedDBClient(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_dbclient.", dir=".")
        self.cfg = {}
        self.user = "nist0:ava1"
        self.cli = fsbased.FSBasedDBClient(self.outdir.name, self.cfg, base.DMP_PROJECTS, self.user)
    
    def tearDown(self):
        self.outdir.cleanup()

    def test_ensure_collectin(self):
        self.assertTrue(not os.path.exists(os.path.join(self.outdir.name, "goob")))
        self.cli._ensure_collection("goob")
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, "goob")))
        self.cli._ensure_collection("goob")

        self.assertTrue(not os.path.exists(os.path.join(self.outdir.name, "nextnum")))
        self.cli._ensure_collection("nextnum")
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, "nextnum")))

    def test_read_rec(self):
        self.assertIsNone(self.cli._read_rec("goob", "gurn.json"))
        self.cli._ensure_collection("goob")
        self.assertIsNone(self.cli._read_rec("goob", "gurn.json"))
        with open(os.path.join(self.outdir.name, "goob", "gurn.json"), 'w') as fd:
            fd.write("%d\n" % 3)
        rec = self.cli._read_rec("goob", "gurn")
        self.assertEqual(rec, 3)

    def test_write_rec(self):
        self.assertTrue(not os.path.exists(os.path.join(self.outdir.name, "nextnum")))
        self.cli._write_rec("nextnum", "goob", 4)
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, "nextnum", "goob.json")))
        with open(os.path.join(self.outdir.name, "nextnum", "goob.json")) as fd:
            self.assertEqual(fd.read(), "4")

        self.cli._write_rec("nextnum", "goob", 8)
        self.assertEqual(self.cli._read_rec("nextnum", "goob"), 8)

        rec = { "id": "pdr0:1", "data": { "a": [ 0 ]}}
        self.cli._write_rec(base.DMP_PROJECTS, rec["id"], rec)
        self.assertEqual(self.cli._read_rec(base.DMP_PROJECTS, rec["id"]), rec)

    def test_next_recnum(self):
        self.assertEqual(self.cli._next_recnum("goob"), 1)
        self.assertEqual(self.cli._next_recnum("goob"), 2)
        self.assertEqual(self.cli._next_recnum("goob"), 3)
        self.assertEqual(self.cli._next_recnum("gary"), 1)
        self.assertEqual(self.cli._next_recnum("goober"), 1)
        self.assertEqual(self.cli._next_recnum("gary"), 2)

        recpath = self.cli._root / "nextnum" / ("goob.json")
        self.assertTrue(recpath.is_file())
        self.assertEqual(self.cli._read_rec("nextnum", "goob"), 3)
        self.cli._try_push_recnum("goob", 2)
        self.assertEqual(self.cli._read_rec("nextnum", "goob"), 3)

        recpath = self.cli._root / "nextnum" / ("hank.json")
        self.assertTrue(not recpath.exists())
        self.cli._try_push_recnum("hank", 2)
        self.assertTrue(not recpath.exists())

        self.cli._try_push_recnum("goob", 3)
        self.assertEqual(self.cli._read_rec("nextnum", "goob"), 2)

    def test_get_from_coll(self):
        # test query on non-existent collection
        self.assertIsNone(self.cli._get_from_coll("alice", "p:bob"))

        # test query on existing but empty collection
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))

        # put in some test data into the underlying database
        self.cli._write_rec(base.GROUPS_COLL, "p:bob", {"id": "p:bob", "owner": "alice"})
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})
        
        self.cli._write_rec(base.GROUPS_COLL, "p:mine", {"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"),
                         {"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

        self.assertIsNone(self.cli._get_from_coll("nextnum", "goob"))
        self.cli._write_rec("nextnum", "goob", 0)
        self.assertEqual(self.cli._get_from_coll("nextnum", "goob"), 0)

    def test_select_from_coll(self):
        # test query on non-existent collection
        it = self.cli._select_from_coll("alice", owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on existing but empty collection
        self.cli._ensure_collection(base.GROUPS_COLL)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._write_rec(base.GROUPS_COLL, "p:bob", {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        it = self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})

        self.cli._write_rec(base.GROUPS_COLL, "p:mine",
                            {"id": "p:mine", "owner": "p:bob", "hobby": "whittling"})
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
        self.cli._ensure_collection(base.GROUPS_COLL)
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._write_rec(base.GROUPS_COLL, "p:bob", {"id": "p:bob", "members": ["p:bob"]})
        self.cli._write_rec(base.GROUPS_COLL, "stars", {"id": "stars", "members": ["p:tom", "p:bob"]})
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        self.cli._write_rec(base.GROUPS_COLL, "p:bob", {"id": "p:bob", "members": ["p:bob", "alice"]})
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
        self.cli._ensure_collection(base.GROUPS_COLL)
        self.assertFalse(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))

        self.cli._write_rec(base.GROUPS_COLL, "p:bob", {"id": "p:bob", "members": ["p:bob"]})
        self.cli._write_rec(base.GROUPS_COLL, "stars", {"id": "stars", "members": ["p:tom", "p:bob"]})
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, base.GROUPS_COLL, "p:bob.json")))
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, base.GROUPS_COLL, "stars.json")))

        self.assertTrue(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))
        self.assertTrue(not os.path.exists(os.path.join(self.outdir.name, base.GROUPS_COLL, "p:bob.json")))
        self.assertTrue(os.path.exists(os.path.join(self.outdir.name, base.GROUPS_COLL, "stars.json")))

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
        self.cli._ensure_collection(base.GROUPS_COLL)
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
        self.cli._write_rec(base.DMP_PROJECTS, id, rec.to_dict())
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": "goob", "owner": "alice"}, self.cli)
        self.cli._write_rec(base.DMP_PROJECTS, "goob", rec.to_dict())

        recs = list(self.cli.select_records(base.ACLs.READ))
        self.assertEqual(len(recs), 1)
        self.assertTrue(isinstance(recs[0], base.ProjectRecord))
        self.assertEqual(recs[0].id, id)

    def test_action_log_io(self):
        with self.assertRaises(ValueError):
            self.cli._save_action_data({'goob': 'gurn'})

        recpath = self.cli._root / "prov_action_log" / "goob:gurn.lis"
        self.assertTrue(not recpath.exists())
        self.cli._save_action_data({'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertTrue(recpath.exists())
        with open(recpath) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0]), {'subject': 'goob:gurn', 'foo': 'bar'})

        self.cli._save_action_data({'subject': 'goob:gurn', 'bob': 'alice'})
        with open(recpath) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(json.loads(lines[1]), {'subject': 'goob:gurn', 'bob': 'alice'})
        
        recpath = self.cli._root / "prov_action_log" / "grp0001.lis"
        self.assertTrue(not recpath.exists())
        self.cli._save_action_data({'subject': 'grp0001', 'dylan': 'bob'})
        self.assertTrue(recpath.exists())
        with open(recpath) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0]), {'subject': 'grp0001', 'dylan': 'bob'})

        acts = self.cli._select_actions_for("goob:gurn")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(acts[1], {'subject': 'goob:gurn', 'bob': 'alice'})
        acts = self.cli._select_actions_for("grp0001")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("grp0001")
        self.assertTrue(not recpath.exists())
        recpath = self.cli._root / "prov_action_log" / "goob:gurn.lis"
        self.assertTrue(recpath.exists())
        self.cli._delete_actions_for("goob:gurn")
        self.assertTrue(not recpath.exists())

        self.assertEqual(self.cli._select_actions_for("goob:gurn"), [])
        self.assertEqual(self.cli._select_actions_for("grp0001"), [])

    def test_save_history(self):
        with self.assertRaises(ValueError):
            self.cli._save_history({'goob': 'gurn'})

        recpath = self.cli._root / "history" / "goob:gurn.json"
        self.assertFalse(recpath.exists())
        self.cli._save_history({'recid': 'goob:gurn', 'foo': 'bar'})
        self.cli._save_history({'recid': 'goob:gurn', 'alice': 'bob'})

        self.assertTrue(recpath.is_file(), "history not saved to file")
        with open(recpath) as fd:
            data = json.load(fd)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], {'recid': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(data[1], {'recid': 'goob:gurn', 'alice': 'bob'})



                         
if __name__ == '__main__':
    test.main()




        
        
        
