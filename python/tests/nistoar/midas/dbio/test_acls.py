import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.base import ACLs
from nistoar.pdr.utils.prov import Agent

class TestACLs(test.TestCase):

    def setUp(self):
        self.cfg = {
            "project_id_minting": {
                "default_shoulder": {
                    "public": "pdr0"
                }
            }
        }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DMP_PROJECTS, {}, self.user)
        self.rec = self.cli.create_record("test")
        self.acls = self.rec.acls

    def test_ctor(self):
        self.assertIs(self.acls._rec, self.rec)
        self.assertIn(ACLs.READ, self.acls._perms)
        self.assertIn(ACLs.WRITE, self.acls._perms)
        self.assertIn(ACLs.ADMIN, self.acls._perms)
        self.assertIn(ACLs.DELETE, self.acls._perms)
        self.assertEqual(self.acls._perms[ACLs.READ], [self.user])
        self.assertEqual(self.acls._perms[ACLs.WRITE], [self.user])
        self.assertEqual(self.acls._perms[ACLs.ADMIN], [self.user])
        self.assertEqual(self.acls._perms[ACLs.DELETE], [self.user])

    def test_grant_revoke_perm_from(self):
        self.acls.grant_perm_to(ACLs.READ, "alice")
        self.acls.grant_perm_to(ACLs.READ, "bob")
        self.acls.grant_perm_to(ACLs.WRITE, "alice")

        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [self.user, "alice", "bob"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.WRITE)), [self.user, "alice"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.DELETE)), [self.user])

        self.acls.revoke_perm_from(ACLs.READ, "alice")
        
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [self.user, "bob"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.WRITE)), [self.user, "alice"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.DELETE)), [self.user])

        self.acls.revoke_perm_from(ACLs.WRITE, "alice", self.user)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [self.user, "bob"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.WRITE)), [])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.DELETE)), [self.user])

        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.ADMIN)), [self.user])
        self.acls.grant_perm_to(ACLs.ADMIN, "alice")
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.ADMIN)), [self.user, "alice"])
        self.acls.revoke_perm_from(ACLs.ADMIN, "alice", self.user)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.ADMIN)), [self.user])
        self.acls.revoke_perm_from(ACLs.ADMIN, "alice", self.user, protect_owner=False)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.ADMIN)), [])

    def test_grant_revoke_perm_from_all(self):
        self.acls.grant_perm_to(ACLs.READ, "alice")
        self.acls.grant_perm_to(ACLs.READ, "bob")
        self.acls.grant_perm_to(ACLs.WRITE, "alice")
        self.acls.grant_perm_to(ACLs.ADMIN, "alice")

        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [self.user, "alice", "bob"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.WRITE)), [self.user, "alice"])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.DELETE)), [self.user])
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.ADMIN)), [self.user, "alice"])

        self.acls.revoke_perm_from_all(ACLs.READ)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [self.user])
        self.acls.revoke_perm_from_all(ACLs.WRITE)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.WRITE)), [])
        self.acls.revoke_perm_from_all(ACLs.READ, protect_owner=False)
        self.assertEqual(list(self.acls.iter_perm_granted(ACLs.READ)), [])
        
        

    def test_iter_perm_granted(self):
        self.acls._perms[ACLs.READ] = "alice bob".split()
        it = self.acls.iter_perm_granted(ACLs.DELETE)
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        self.assertEqual(len(list(it)), 1)
        it = self.acls.iter_perm_granted(ACLs.READ)
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        self.assertEqual(len(list(it)), 2)

    def test_unauthorized(self):
        self.cli._who = Agent("test", Agent.USER, "gary")
        with self.assertRaises(base.NotAuthorized):
            self.acls.grant_perm_to(ACLs.WRITE, "alice")


        
        
                         
if __name__ == '__main__':
    test.main()
        

        
    
