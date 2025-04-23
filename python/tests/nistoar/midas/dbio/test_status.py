import os, pdb, sys, json
import unittest as test
from copy import deepcopy

from nistoar.midas.dbio import status
from nistoar.pdr.utils.prov import Action

class TestRecordStatus(test.TestCase):

    def test_ctor(self):
        stat = status.RecordStatus("goob", {})
        self.assertEqual(stat.id, "goob")
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, status.ACTION_CREATE)
        self.assertEqual(stat.message, "")
        self.assertEqual(stat.since, 0)
        self.assertEqual(stat.modified, 0)
        self.assertEqual(stat.created, 0)
        self.assertEqual(stat.since_date, "pending")
        self.assertEqual(stat.modified_date, "pending")
        self.assertEqual(stat.created_date, "pending")

    def test_act(self):
        stat = status.RecordStatus("goob", {"state": status.EDIT, "since": -1})
        self.assertEqual(stat.id, "goob")
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, status.ACTION_CREATE)
        self.assertIsNone(stat.by_who)
        self.assertEqual(stat.message, "")
        self.assertGreater(stat.since, 0)
        self.assertGreater(stat.modified, 0)
        self.assertGreater(stat.created, 0)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")
        self.assertNotEqual(stat.created_date, "pending")

        stat.act(Action.PATCH, "made updates")
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, Action.PATCH)
        self.assertIsNone(stat.by_who)
        self.assertEqual(stat.message, "made updates")
        self.assertEqual(stat.modified, 0)
        self.assertGreater(stat.created, 0)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertEqual(stat.modified_date, "pending")
        self.assertNotEqual(stat.created_date, "pending")
        
        stat.act(Action.PUT)
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, Action.PUT)
        self.assertIsNone(stat.by_who)
        self.assertEqual(stat.message, "")
        self.assertEqual(stat.modified, 0)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertEqual(stat.modified_date, "pending")

        stat.act(Action.COMMENT, "Whoa", when=-1)
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, Action.COMMENT)
        self.assertIsNone(stat.by_who)
        self.assertEqual(stat.message, "Whoa")
        self.assertGreater(stat.modified, stat.since)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")

        stat.act(Action.COMMENT, "Whoa", "nstr1", -1)
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, Action.COMMENT)
        self.assertEqual(stat.by_who, "nstr1")
        self.assertEqual(stat.message, "Whoa")
        self.assertGreater(stat.modified, stat.since)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")
        
    def test_set_state(self):
        stat = status.RecordStatus("goob", {"state": status.EDIT, "since": -1})
        self.assertEqual(stat.id, "goob")
        self.assertEqual(stat.state, status.EDIT)
        self.assertEqual(stat.action, status.ACTION_CREATE)
        self.assertEqual(stat.message, "")
        self.assertGreater(stat.since, 0)
        self.assertGreater(stat.modified, 0)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")

        then = stat.since
        stat.set_state(status.PROCESSING)
        self.assertEqual(stat.id, "goob")
        self.assertEqual(stat.state, status.PROCESSING)
        self.assertEqual(stat.action, status.ACTION_CREATE)
        self.assertEqual(stat.message, "")
        self.assertGreater(stat.since, then)
        self.assertLess(stat.modified, stat.since)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")

    def test_set_times(self):
        stat = status.RecordStatus("goob", {})
        self.assertEqual(stat.since, 0)
        self.assertEqual(stat.modified, 0)
        self.assertEqual(stat.created, 0)
        self.assertEqual(stat.since_date, "pending")
        self.assertEqual(stat.modified_date, "pending")
        self.assertEqual(stat.created_date, "pending")
        
        stat.set_times()
        self.assertGreater(stat.since, 0)
        self.assertGreater(stat.modified, 0)
        self.assertGreater(stat.created, 0)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")
        self.assertNotEqual(stat.created_date, "pending")
        

if __name__ == '__main__':
    test.main()
