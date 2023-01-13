import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base

class TestProjectRecord(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DRAFT_PROJECTS, self.user)
        self.rec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                      {"id": "pdr0:2222", "name": "brains", "owner": self.user}, self.cli)

    def test_ctor(self):
        self.assertIs(self.rec._cli, self.cli)
        self.assertEqual(self.rec.id, "pdr0:2222")
        self.assertEqual(self.rec.name, "brains")
        self.assertEqual(self.rec.owner, self.user)
        self.assertGreater(self.rec.created, 0)
        self.assertTrue(self.rec.created_date.startswith("20"))
        self.assertNotIn('.', self.rec.created_date)
        self.assertEqual(self.rec.data, {})
        self.assertEqual(self.rec.meta, {})
        # self.assertEqual(self.rec.curators, [])
        self.assertEqual(self.rec._data.get('curators'), [])
        self.assertTrue(self.rec.acls, base.ACLs)
        self.assertIs(self.rec._data['acls'], self.rec.acls._perms)
        self.assertEqual(self.rec.acls._perms, {
            base.ACLs.READ:   [self.rec.owner],
            base.ACLs.WRITE:  [self.rec.owner],
            base.ACLs.ADMIN:  [self.rec.owner],
            base.ACLs.DELETE: [self.rec.owner]
        })

        self.assertEqual(self.rec.validate(), [])

    def test_save(self):
        self.assertEqual(self.rec.data, {})
        self.assertEqual(self.rec.meta, {})
        self.assertNotIn("pdr0:2222", self.cli._db[base.DRAFT_PROJECTS])
        
        self.rec.save()
        self.assertIn("pdr0:2222", self.cli._db[base.DRAFT_PROJECTS])
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['name'], "brains")
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['data'], {})
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['meta'], {})
        
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['acls'][base.ACLs.READ],
                         [self.user])
        self.rec.meta['type'] = 'software'
        self.rec.acls.grant_perm_to(base.ACLs.READ, "alice")
        self.rec.save()
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['meta'], {"type": "software"})
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['acls'][base.ACLs.READ],
                         [self.user, "alice"])

        rec = self.cli.get_record_for("pdr0:2222")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.meta,  {"type": "software"})
        self.assertTrue(rec.authorized(base.ACLs.READ, "alice"))

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



                         
if __name__ == '__main__':
    test.main()

