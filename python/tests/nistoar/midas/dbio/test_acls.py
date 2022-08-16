import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base

class TestACLs(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DMP_PROJECTS, self.user)
        self.rec = self.cli.create_record(self.user)
        self.acls = self.rec.acls

    def test_ctor(self):
        self.assertIs(self.acls._rec, self.rec)
        self.assertIn(base.READ, self.acls._perms)
        self.assertIn(base.WRITE, self.acls._perms)
        self.assertIn(base.ADMIN, self.acls._perms)
        self.assertIn(base.DELETE, self.acls._perms)
        self.assertEqual(self.acls._perms[base.READ], [])
        self.assertEqual(self.acls._perms[base.WRITE], [])
        self.assertEqual(self.acls._perms[base.ADMIN], [])
        self.assertEqual(self.acls._perms[base.DELETE], [])

    def test_grant_revoke_perm(self):
        self.acls.grant_perm_to(base.READ, "alice")
        self.acls.grant_perm_to(base.READ, "bob")
        self.acls.grant_perm_to(base.WRITE, "alice")

        self.assertEqual(list(self.acls.iter_perm_granted(base.READ)), "alice bob".split())
        self.assertEqual(list(self.acls.iter_perm_granted(base.WRITE)), ["alice"])
        self.assertEqual(list(self.acls.iter_perm_granted(base.DELETE)), [])

        self.acls.revoke_perm_from(base.READ, "alice")
        
        self.assertEqual(list(self.acls.iter_perm_granted(base.READ)), ["bob"])
        self.assertEqual(list(self.acls.iter_perm_granted(base.WRITE)), ["alice"])
        self.assertEqual(list(self.acls.iter_perm_granted(base.DELETE)), [])


    def test_iter_perm_granted(self):
        self.acls._perms[base.READ] = "alice bob".split()
        it = self.acls.iter_perm_granted(base.DELETE)
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        self.assertEqual(len(list(it)), 0)
        it = self.acls.iter_perm_granted(base.READ)
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        self.assertEqual(len(list(it)), 2)
        
        
                         
if __name__ == '__main__':
    test.main()
        

        
    
