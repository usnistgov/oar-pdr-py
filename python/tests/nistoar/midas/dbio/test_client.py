import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.pdr.utils.prov import Action, Agent

class TestDBClient(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0", "allowed_project_shoulders": ["mds3"] }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, self.user)

    def test_ctor(self):
        self.assertEqual(self.cli._cfg, self.cfg)
        self.assertEqual(self.cli.user_id, self.user)
        self.assertIsNone(self.cli._whogrps)
        self.assertTrue(isinstance(self.cli.groups, base.DBGroups))
        self.assertEqual(self.cli._projcoll, base.DRAFT_PROJECTS)

    def test_user_groups(self):
        self.assertIsNone(self.cli._whogrps)
        self.assertEqual(self.cli.user_groups, {'grp0:public'})
        self.assertEqual(self.cli._whogrps, {'grp0:public'})

        self.cli.groups.create_group("goobers").add_member("bob").save()
        self.assertEqual(self.cli.user_groups, {'grp0:public', 'grp0:nist0:ava1:goobers'})

    def test_mint_id(self):
        self.assertEqual(self.cli._mint_id("go0"), "go0:0001")
        self.assertEqual(self.cli._mint_id("go0"), "go0:0002")
        self.assertEqual(self.cli._mint_id("go0"), "go0:0003")
        self.cli._db["nextnum"]["go0"] = 22
        self.assertEqual(self.cli._mint_id("go0"), "go0:0023")
        self.cli._db["nextnum"]["go0"] = 22222

        # testing unminting
        id = self.cli._mint_id("go0")
        self.assertEqual(id, "go0:22223")
        self.assertEqual(self.cli._parse_id(id), ("go0", 22223))
        self.cli._try_push_recnum("go0", 22223)   # unmints 22223
        self.assertEqual(self.cli._mint_id("go0"), "go0:22223")
        self.cli._try_push_recnum("go0", 22222)   # has no effect
        self.assertEqual(self.cli._mint_id("go0"), "go0:22224")

        self.assertEqual(self.cli._mint_id("ncnr5"), "ncnr5:0001")
    
    def test_create_record(self):
        self.assertTrue(not self.cli.exists("pdr0:0001"))
        self.assertTrue(not self.cli.name_exists("goob"))
        
        rec = self.cli.create_record("goob")
        self.assertEqual(rec.id, "pdr0:0001")
        self.assertEqual(rec.owner, self.user)
        self.assertEqual(rec.name, "goob")
        self.assertEqual(rec.acls._perms[base.ACLs.READ], [self.user])
        self.assertEqual(rec.data, {})
        self.assertEqual(rec.meta, {})
        self.assertGreater(rec.created, 0)

        self.assertTrue(self.cli.exists("pdr0:0001"))
        self.assertTrue(self.cli.name_exists("goob"))
        with self.assertRaises(base.AlreadyExists):
            self.cli.create_record("goob")
        with self.assertRaises(base.AlreadyExists):
            self.cli.create_record("goob", "mds3")

        with self.assertRaises(base.NotAuthorized):
            self.cli.create_record("hers", foruser="alice")
        rec = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice").create_record("goob", foruser="alice")
        self.assertEqual(rec.id, "pdr0:0002")
        self.assertEqual(rec.owner, "alice")
        self.assertEqual(rec.name, "goob")
        self.assertTrue(self.cli.exists("pdr0:0002"))
        self.assertTrue(self.cli.name_exists("goob", "alice"))

        rec = self.cli.create_record("test", "mds3")
        self.assertEqual(rec.id, "mds3:0001")
        self.assertEqual(rec.owner, self.user)
        self.assertEqual(rec.name, "test")
        self.assertTrue(self.cli.exists("mds3:0001"))
        self.assertTrue(self.cli.name_exists("test", self.user))

        self.assertTrue(self.cli.delete_record("mds3:0001"))
        self.assertTrue(not self.cli.exists("mds3:0001"))
        rec = self.cli.create_record("test", "mds3")
        self.assertEqual(rec.id, "mds3:0001")
        rec = self.cli.create_record("test2", "mds3")
        self.assertEqual(rec.id, "mds3:0002")
        self.assertTrue(self.cli.exists("mds3:0001"))
        self.assertTrue(self.cli.delete_record("mds3:0001"))
        self.assertTrue(not self.cli.exists("mds3:0001"))
        rec = self.cli.create_record("test", "mds3")
        self.assertEqual(rec.id, "mds3:0003")

    def test_get_record(self):
        with self.assertRaises(base.ObjectNotFound):
            self.cli.get_record_for("pdr0:0001")

        self.cli.create_record("test1")
        self.cli.create_record("test2")
        rec = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice").create_record("goob")

        rec = self.cli.get_record_for("pdr0:0001")
        self.assertEqual(rec.name, "test1")
        rec = self.cli.get_record_for("pdr0:0002")
        self.assertEqual(rec.name, "test2")
        self.assertTrue(self.cli.exists("pdr0:0003"))
        self.assertTrue(self.cli.name_exists("goob", "alice"))
        with self.assertRaises(base.NotAuthorized):
            self.cli.get_record_for("pdr0:0003")
    
        goob = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice").get_record_for("pdr0:0003")
        self.assertEqual(goob.name, "goob")
        
        goob.acls.grant_perm_to(base.ACLs.READ, self.user)
        goob.save()
        rec = self.cli.get_record_for("pdr0:0003")
        self.assertEqual(goob.name, "goob")

        with self.assertRaises(base.NotAuthorized):
            self.cli.get_record_for("pdr0:0003", base.ACLs.WRITE)
        goob.acls.grant_perm_to(base.ACLs.WRITE, self.user)
        goob.save()
        rec = self.cli.get_record_for("pdr0:0003", base.ACLs.WRITE)
        self.assertEqual(goob.name, "goob")
        
        with self.assertRaises(base.NotAuthorized):
            self.cli.get_record_for("pdr0:0003", base.ACLs.OWN)

    def test_get_record_by_name(self):
        self.cli.create_record("test1")
        self.cli.create_record("test2")
        rec = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice").create_record("goob")

        rec = self.cli.get_record_by_name("test1")
        self.assertEqual(rec.name, "test1")
        self.assertEqual(rec.id, "pdr0:0001")
        rec = self.cli.get_record_by_name("test2")
        self.assertEqual(rec.name, "test2")
        self.assertEqual(rec.id, "pdr0:0002")
        self.assertIsNone(self.cli.get_record_by_name("goob"))
        self.assertIsNone(self.cli.get_record_by_name("goob", "alice"))

        goob = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice").get_record_by_name("goob")
        self.assertEqual(goob.name, "goob")
        self.assertEqual(goob.id, "pdr0:0003")

        goob.acls.grant_perm_to(base.ACLs.READ, self.user)
        goob.save()
        self.assertIsNone(self.cli.get_record_by_name("goob"))
        rec = self.cli.get_record_by_name("goob", "alice")
        self.assertEqual(rec.name, "goob")
        self.assertEqual(rec.id, "pdr0:0003")

    def test_select_records(self):
        rec = self.cli.create_record("mine1")
        rec = self.cli.create_record("mine2")
        rec.acls.grant_perm_to(rec.acls.READ, "alice")
        rec.save()
        rec = self.cli.create_record("mine3")
        rec.acls.grant_perm_to(rec.acls.READ, "alice")
        rec.acls.grant_perm_to(rec.acls.WRITE, "alice")
        rec.save()

        cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, "alice")
        rec = cli.create_record("test1")
        rec = cli.create_record("test2")

        recs = cli.select_records(rec.acls.READ)
        names = [r.name for r in recs]
        self.assertIn("test1", names)
        self.assertIn("test2", names)
        self.assertIn("mine2", names)
        self.assertIn("mine3", names)
        self.assertEqual(len(names), 4)

        recs = cli.select_records(rec.acls.WRITE)
        names = [r.name for r in recs]
        self.assertIn("test1", names)
        self.assertIn("test2", names)
        self.assertIn("mine3", names)
        self.assertEqual(len(names), 3)

        recs = cli.select_records()
        names = [r.name for r in recs]
        self.assertIn("test1", names)
        self.assertIn("test2", names)
        self.assertIn("mine2", names)
        self.assertIn("mine3", names)
        self.assertEqual(len(names), 4)

    def test_record_action(self):
        testuser = Agent("dbio", Agent.USER, self.user, "nist")
        
        rec = self.cli.create_record("mine1")
        rec = self.cli.create_record("mine2")
        self.assertTrue(self.cli._upsert(base.DRAFT_PROJECTS,
                                         {"id": "mds3:0001", "owner": self.user, "hobby": "whittling"})) 
        self.assertTrue(self.cli._upsert(base.DRAFT_PROJECTS,
                                         {"id": "mds3:0002", "owner": self.user, "hobby": "whittling"})) 
        self.cli.record_action(Action(Action.CREATE, "mds3:0001", testuser, "created"))
        self.cli.record_action(Action(Action.COMMENT, "mds3:0001", testuser, "i'm hungry"))
        self.cli.record_action(Action(Action.COMMENT, "mds3:0002", testuser, "i'm hungry"))
        acts = self.cli._select_actions_for("mds3:0001")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0]['type'], Action.CREATE)
        self.assertEqual(acts[1]['type'], Action.COMMENT)

        with self.assertRaises(base.ObjectNotFound):
            self.cli.record_action(Action(Action.CREATE, "bob", testuser, "created"))

        cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, "nist:alice")
        with self.assertRaises(base.NotAuthorized):
            cli.record_action(Action(Action.COMMENT, "mds3:0001", testuser, "crikey"))
            
    def test_close_actionlog_with(self):
        testuser = Agent("dbio", Agent.USER, self.user, "nist")
        
        prec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                  {"id": "pdr0:2222", "name": "brains", "owner": "doc0:sec"}, self.cli)
        finalact = Action(Action.PROCESS, "pdr0:2222", testuser, "done!", "submit")
        self.assertNotIn('prov_action_log', self.cli._db)
        with self.assertRaises(base.NotAuthorized):
            self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook"})
        self.assertNotIn('prov_action_log', self.cli._db)

        prec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                  {"id": "pdr0:2222", "name": "brains", "owner": self.cli.user_id}, self.cli)
        self.cli._close_actionlog_with(prec, finalact, {"published_as": "comicbook"})

        # no history should have been written
        self.assertNotIn('history', self.cli._db)

        self.cli._db[self.cli._projcoll]["pdr0:2222"] = prec.to_dict()
        
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

