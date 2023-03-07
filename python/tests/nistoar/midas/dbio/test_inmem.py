import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.pdr.publish.prov import Action, PubAgent

testuser = PubAgent("test", PubAgent.AUTO, "tester")

class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.cfg = { "goob": "gurn" }
        self.fact = inmem.InMemoryDBClientFactory(self.cfg, { "nextnum": { "hank": 2 }})

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertTrue(self.fact._db)
        self.assertEqual(self.fact._db.get(base.DAP_PROJECTS), {})
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

        self.assertEqual(self.cli._db["nextnum"]["goob"], 3)
        self.cli._try_push_recnum("goob", 2)
        self.assertEqual(self.cli._db["nextnum"]["goob"], 3)
        self.assertNotIn("hank", self.cli._db["nextnum"])
        self.cli._try_push_recnum("hank", 2)
        self.assertNotIn("hank", self.cli._db["nextnum"])
        self.cli._try_push_recnum("goob", 3)
        self.assertEqual(self.cli._db["nextnum"]["goob"], 2)

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

        # test deactivated filter
        self.cli._db[base.GROUPS_COLL]["p:gang"] = {"id": "p:gang", "owner": "p:bob", "deactivated": 1.2 }
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob"))
        self.assertEqual(len(recs), 1)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, incl_deact=True, owner="p:bob"))
        self.assertEqual(len(recs), 2)
        self.cli._db[base.GROUPS_COLL]["p:gang"]["deactivated"] = None
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob"))
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

        # test deactivated filter
        self.cli._db[base.GROUPS_COLL]["p:gang"] = {"id": "p:gang", "members": ["p:bob"], "deactivated": 1.2}
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob", incl_deact=True))
        self.assertEqual(len(recs), 3)
        self.cli._db[base.GROUPS_COLL]["p:gang"]["deactivated"] = None
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 3)

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

    def test_action_log_io(self):
        with self.assertRaises(ValueError):
            self.cli._save_action_data({'goob': 'gurn'})

        self.cli._save_action_data({'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['goob:gurn']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][0], 
                         {'subject': 'goob:gurn', 'foo': 'bar'})

        self.cli._save_action_data({'subject': 'goob:gurn', 'bob': 'alice'})
        self.assertEqual(len(self.cli._db['prov_action_log']['goob:gurn']), 2)
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][0], 
                         {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][1], 
                         {'subject': 'goob:gurn', 'bob': 'alice'})
        
        self.cli._save_action_data({'subject': 'grp0001', 'dylan': 'bob'})
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('grp0001' in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['grp0001']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['grp0001'][0], 
                         {'subject': 'grp0001', 'dylan': 'bob'})

        acts = self.cli._select_actions_for("goob:gurn")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(acts[1], {'subject': 'goob:gurn', 'bob': 'alice'})
        acts = self.cli._select_actions_for("grp0001")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("goob:gurn")
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' not in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['grp0001']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['grp0001'][0], 
                         {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("grp0001")
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' not in self.cli._db['prov_action_log'])
        self.assertTrue('grp0001' not in self.cli._db['prov_action_log'])

        self.assertEqual(self.cli._select_actions_for("goob:gurn"), [])
        self.assertEqual(self.cli._select_actions_for("grp0001"), [])

    def test_save_history(self):
        with self.assertRaises(ValueError):
            self.cli._save_history({'goob': 'gurn'})

        self.cli._save_history({'recid': 'goob:gurn', 'foo': 'bar'})
        self.cli._save_history({'recid': 'goob:gurn', 'alice': 'bob'})

        self.assertTrue('history' in self.cli._db)
        self.assertTrue('goob:gurn' in self.cli._db['history'])
        self.assertEqual(len(self.cli._db['history']['goob:gurn']), 2)
        self.assertEqual(self.cli._db['history']['goob:gurn'][0],
                         {'recid': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(self.cli._db['history']['goob:gurn'][1],
                         {'recid': 'goob:gurn', 'alice': 'bob'})

    def test_record_action(self):
        self.cli.record_action(Action(Action.CREATE, "mds3:0008", testuser, "created"))
        self.cli.record_action(Action(Action.COMMENT, "mds3:0008", testuser, "i'm hungry"))
        acts = self.cli._select_actions_for("mds3:0008")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0]['type'], Action.CREATE)
        self.assertEqual(acts[1]['type'], Action.COMMENT)
        
    def test_close_actionlog_with(self):
        prec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                  {"id": "pdr0:2222", "name": "brains", "owner": "nist0:ava1"}, self.cli)
        finalact = Action(Action.PROCESS, "pdr0:2222", testuser, "done!", "submit")
        self.assertNotIn('prov_action_log', self.cli._db)
        self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook"})

        # no history should have been written
        self.assertNotIn('history', self.cli._db)
                         
        self.cli.record_action(Action(Action.CREATE, "pdr0:2222", testuser, "created"))
        self.cli.record_action(Action(Action.COMMENT, "pdr0:2222", testuser, "i'm hungry"))
        self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook", "recid": "goob"})
        self.assertIn('history', self.cli._db)
        self.assertEqual(len(self.cli._db['history']["pdr0:2222"]), 1)
        self.assertEqual(len(self.cli._db['history']["pdr0:2222"][0]['history']), 3)
        self.assertEqual(self.cli._db['history']["pdr0:2222"][0]['recid'], "pdr0:2222")
        self.assertEqual(self.cli._db['history']["pdr0:2222"][0]['published_as'], "comicbook")
        self.assertEqual(self.cli._db['history']["pdr0:2222"][0]['close_action'], "PROCESS:submit")
        self.assertEqual(self.cli._db['history']["pdr0:2222"][0]['acls'],
                         {"read": prec.acls._perms['read']})
        self.assertEqual(self.cli._db['history']["pdr0:2222"][0]['history'][-1]['message'], "done!")
        self.assertEqual(self.cli._select_actions_for("pdr0:2222"), [])
        
        self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook"})
        self.assertEqual(self.cli._select_actions_for("pdr0:2222"), [])
        self.assertEqual(len(self.cli._db['history']["pdr0:2222"]), 1)
        
        self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook"}, False)
        self.assertEqual(self.cli._select_actions_for("pdr0:2222"), [])
        self.assertEqual(len(self.cli._db['history']["pdr0:2222"]), 2)
        self.assertEqual(self.cli._db['history']["pdr0:2222"][1]['history'][-1]['message'], "done!")
        self.assertEqual(len(self.cli._db['history']["pdr0:2222"][1]['history']), 1)

        

if __name__ == '__main__':
    test.main()
        

        
