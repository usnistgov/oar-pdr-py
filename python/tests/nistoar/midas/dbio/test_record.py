import os, json, pdb, logging
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import inmem, base

class TestProjectRecord(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.user = "nist0:ava1"
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, self.user)
        self.rec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                      {"id": "pdr0:2222", "name": "brains", "owner": self.user}, self.cli)

    def test_ctor(self):
        self.rec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                      {"id": "pdr0:2222", "name": "brains", "owner": self.user}, self.cli)
        self.assertIs(self.rec._cli, self.cli)
        self.assertEqual(self.rec.id, "pdr0:2222")
        self.assertEqual(self.rec.name, "brains")
        self.assertEqual(self.rec.owner, self.user)
        self.assertGreater(self.rec.created, 0)
        self.assertEqual(self.rec.modified, self.rec.created)
        self.assertTrue(self.rec.created_date.startswith("20"))
        self.assertNotIn('.', self.rec.created_date)
        self.assertFalse(self.rec.deactivated)
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
        self.assertGreaterEqual(self.rec.modified, self.rec.created)
        oldmod = self.rec.modified
        self.assertIn("pdr0:2222", self.cli._db[base.DRAFT_PROJECTS])
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['name'], "brains")
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['data'], {})
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['meta'], {})
        
        self.assertEqual(self.cli._db[base.DRAFT_PROJECTS]["pdr0:2222"]['acls'][base.ACLs.READ],
                         [self.user])
        self.rec.meta['type'] = 'software'
        self.rec.acls.grant_perm_to(base.ACLs.READ, "alice")
        self.rec.save()
        self.assertGreater(self.rec.modified, oldmod)
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

    def test_deactivate(self):
        self.assertFalse(self.rec.deactivated)
        self.rec.save()
        self.assertTrue(self.cli.name_exists("brains"))
        self.assertTrue(self.rec.deactivate())
        self.assertFalse(self.rec.deactivate())
        self.rec.save()
        self.assertFalse(not self.rec.deactivated)
        self.assertTrue(self.cli.name_exists("brains"))
        self.assertTrue(self.rec.reactivate())
        self.assertFalse(self.rec.reactivate())
        self.assertFalse(self.rec.deactivated)
        self.rec.save()
        self.assertTrue(self.cli.name_exists("brains"))
    def test_reassign(self):
        # test_validate_user_id()
        self.assertTrue(self.rec._validate_user_id("henry"))
        self.assertFalse(self.rec._validate_user_id(["henry"]))

        self.assertEqual(self.rec.owner, "nist0:ava1")
        self.rec.reassign("nist0:gob")
        self.assertEqual(self.rec.owner, "nist0:gob")
        with self.assertRaises(base.InvalidUpdate):
            self.rec.reassign(["goob"])

    @test.skipIf(not os.environ.get('MONGO_PEOPLE_URL'), "test mongodb people database not available")
    def test_reassign_useps(self):
        self.cfg["people_service"] = { "factory": "mongo", "db_url": os.environ.get('MONGO_PEOPLE_URL') }
        self.fact = inmem.InMemoryDBClientFactory(self.cfg)
        self.cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, self.user)
        self.rec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                      {"id": "pdr0:2222", "name": "brains", "owner": self.user}, self.cli)
        self.assertTrue(self.cli.people_service)

        # test_validate_user_id()
        self.assertTrue(self.rec._validate_user_id("pgp1"))
        self.assertFalse(self.rec._validate_user_id("henry"))

        self.assertEqual(self.rec.owner, "nist0:ava1")
        self.rec.reassign("pgp1")
        self.assertEqual(self.rec.owner, "pgp1")
        with self.assertRaises(base.InvalidUpdate):
            self.rec.reassign("nist0:gob")



                         
if __name__ == '__main__':
    test.main()

