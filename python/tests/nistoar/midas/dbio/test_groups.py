import os, json, pdb, logging, time
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base

class TestGroup(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DMP_PROJECTS, {}, self.user)
        self.rec = base.Group({"id": "g:ava1:friends", "name": "friends", "owner": self.user}, self.cli)

    def test_ctor(self):
        self.assertIs(self.rec._cli, self.cli)
        self.assertEqual(self.rec.id, "g:ava1:friends")
        self.assertEqual(self.rec.owner, self.user)
        self.assertTrue(self.rec.acls, base.ACLs)
        self.assertIs(self.rec._data['acls'], self.rec.acls._perms)
        self.assertEqual(self.rec.acls._perms, {
            base.ACLs.READ:   [self.rec.owner],
            base.ACLs.WRITE:  [self.rec.owner],
            base.ACLs.ADMIN:  [self.rec.owner],
            base.ACLs.DELETE: [self.rec.owner]
        })

        self.assertEqual(self.rec.validate(), [])

    def test_add_remove(self):
        self.assertEqual(list(self.rec.iter_members()), [])
        self.assertTrue(not self.rec.is_member("bob"))
        self.assertTrue(not self.rec.is_member("tony"))
        self.assertTrue(not self.rec.is_member("alice"))
        self.assertFalse(self.rec.is_member("wart"))

        self.rec.add_member("alice", "bob", "tony")
        self.assertEqual(list(self.rec.iter_members()), ["alice", "bob", "tony"])

        self.assertTrue(self.rec.is_member("bob"))
        self.assertTrue(self.rec.is_member("tony"))
        self.assertTrue(self.rec.is_member("alice"))
        self.assertFalse(self.rec.is_member("wart"))

        self.rec.remove_member("tony", "alice")
        self.assertEqual(list(self.rec.iter_members()), ["bob"])

    def test_save(self):
        self.assertEqual(list(self.rec.iter_members()), [])
        self.rec.add_member("alice", "bob", "tony")
        self.assertEqual(list(self.rec.iter_members()), ["alice", "bob", "tony"])

        # make sure the database has the old membership (group doesn't exist yet)
        self.assertNotIn("g:ava1:friends", self.fact._db[base.GROUPS_COLL])

        self.rec.save()
        self.assertEqual(self.fact._db[base.GROUPS_COLL]["g:ava1:friends"]['members'],
                         ["alice", "bob", "tony"])
        
        self.rec.remove_member("alice")
        self.assertEqual(list(self.rec.iter_members()), ["bob", "tony"])
        self.assertEqual(self.fact._db[base.GROUPS_COLL]["g:ava1:friends"]['members'],
                         ["alice", "bob", "tony"])

        self.rec.save()
        self.assertEqual(self.fact._db[base.GROUPS_COLL]["g:ava1:friends"]['members'],
                         ["bob", "tony"])

    def test_authorized(self):
        self.assertTrue(self.rec.authorized(base.ACLs.READ))
        self.assertTrue(self.rec.authorized(base.ACLs.WRITE))
        self.assertTrue(self.rec.authorized(base.ACLs.ADMIN))
        self.assertTrue(self.rec.authorized(base.ACLs.DELETE))
        self.assertTrue(self.rec.authorized([base.ACLs.READ, base.ACLs.WRITE]))
        self.assertTrue(self.rec.authorized(base.ACLs.OWN))
        self.assertFalse(self.rec.authorized(base.ACLs.ADMIN, "gary"))
        self.assertFalse(self.rec.authorized(base.ACLs.DELETE, "gary"))
        self.assertFalse(self.rec.authorized([base.ACLs.READ, base.ACLs.WRITE], "gary"))
        
class TestDBGroups(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DMP_PROJECTS, {}, self.user)
        self.dbg = self.cli.groups

    def test_ctor(self):
        self.assertTrue(isinstance(self.dbg, base.DBGroups))
        self.assertEqual(self.dbg._cli, self.cli)
        self.assertEqual(self.dbg._shldr, base.DEF_GROUPS_SHOULDER)

    def test_mint_id(self):
        self.assertEqual(self.dbg._mint_id("a", "b", "c"), "a:c:b")

    def test_create_group(self):
        # group does not exist yet
        id = "grp0:nist0:ava1:enemies"
        self.assertNotIn(id, self.fact._db[base.GROUPS_COLL])
        
        grp = self.dbg.create_group("enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, self.user)
        self.assertEqual(grp.id, id)
        self.assertTrue(grp.is_member(self.user))
        self.assertGreater(grp.created, 0)
        self.assertLess(grp.created, time.time())
        self.assertEqual(grp.modified, grp.created)

        self.assertTrue(grp.authorized(base.ACLs.OWN))

        # group record was saved to db
        self.assertIn(id, self.fact._db[base.GROUPS_COLL])

        with self.assertRaises(base.AlreadyExists):
            grp = self.dbg.create_group("enemies")

        with self.assertRaises(base.NotAuthorized):
            grp = self.dbg.create_group("friends", "alice")

        self.cli._cfg['superusers'] = [self.user]
        grp = self.dbg.create_group("friends", "alice")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "alice")
        self.assertEqual(grp.id, "grp0:alice:friends")
        self.assertTrue(grp.is_member("alice"))
        self.assertTrue(not grp.is_member(self.user))

    def test_get(self):
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        with self.assertRaises(KeyError):
            self.dbg["grp0:nist0:ava1:friends"]
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))
        
        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))

        grp = self.dbg.get("grp0:nist0:ava1:friends")
        self.assertEqual(grp.id, "grp0:nist0:ava1:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg.get("grp0:nist0:ava1:enemies")
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg["grp0:nist0:ava1:enemies"]
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")

    def test_get_by_name(self):
        self.assertIsNone(self.dbg.get_by_name("friends"))
        self.assertIsNone(self.dbg.get_by_name("enemies"))
        
        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")

        grp = self.dbg.get_by_name("friends")
        self.assertEqual(grp.id, "grp0:nist0:ava1:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg.get_by_name("enemies", self.user)
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")
        self.assertGreater(grp.created, 0)
        self.assertGreaterEqual(grp.modified, grp.created)

        self.assertIsNone(self.dbg.get_by_name("friends", "alice"))

        self.cli._cfg['superusers'] = [self.user]
        grp = self.dbg.create_group("friends", "alice")
        grp = self.dbg.get_by_name("friends", "alice")
        self.assertEqual(grp.id, "grp0:alice:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "alice")

    def test_delete(self):
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        with self.assertRaises(KeyError):
            self.dbg["grp0:nist0:ava1:friends"]
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))
        
        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")

        self.assertIsNotNone(self.dbg.get("grp0:nist0:ava1:friends"))
        grp = self.dbg.get("grp0:nist0:ava1:enemies")
        self.assertIsNotNone(grp)
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))

        self.dbg.delete_group(grp.id)
        self.assertIsNotNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        
        grp.save()
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertGreater(grp.modified, grp.created)

    def test_select_ids_for_user(self):
        for s in "abcdefghijklmnopqrstuvwxyz":
            self.dbg.create_group("group-"+s)

        ns = "grp0:nist0:ava1:group-"
        for s in "rlp":
            grp = self.dbg.get(ns+s)
            grp.add_member("ray")
            grp.save()
        for s in "abtp":
            grp = self.dbg.get(ns+s)
            grp.add_member("alice")
            grp.save()

        self.assertTrue(self.dbg.get(ns+"r").is_member("ray"))
        self.assertTrue(self.dbg.get(ns+"b").is_member("alice"))
        self.assertTrue(self.dbg.get(ns+"p").is_member("ray"))
        self.assertTrue(self.dbg.get(ns+"p").is_member("alice"))

        # no recursive resolving needed
        matches = list(self.dbg.select_ids_for_user("ray"))
        self.assertIn(ns+"r", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 4)

        matches = list(self.dbg.select_ids_for_user("alice"))
        self.assertIn(ns+"a", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        # one-level recursive resolving
        grp = self.dbg.get(ns+"b")
        grp.add_member(ns+"l")
        grp.save()
        matches = list(self.dbg.select_ids_for_user("ray"))
        self.assertIn(ns+"r", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        matches = list(self.dbg.select_ids_for_user("alice"))
        self.assertIn(ns+"a", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        # two-level recursive resolving
        grp = self.dbg.get(ns+"l")
        grp.add_member(ns+"t")
        grp.save()
        grp = self.dbg.get(ns+"t")
        grp.add_member("gary")
        grp.save()
        matches = list(self.dbg.select_ids_for_user("gary"))
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 4)


                         
if __name__ == '__main__':
    test.main()

