import os, pdb, sys, json, time
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
        self.assertIsNone(stat.published_as)
        self.assertIsNone(stat.last_version)
        self.assertIsNone(stat.archived_at)

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
        self.assertEqual(stat.submitted, -1)
        self.assertEqual(stat.published, -1)
        self.assertEqual(stat.since_date, "pending")
        self.assertEqual(stat.modified_date, "pending")
        self.assertEqual(stat.created_date, "pending")
        self.assertEqual(stat.submitted_date, "(not yet submitted)")
        self.assertEqual(stat.published_date, "(not yet published)")
        
        stat.set_times()
        self.assertGreater(stat.since, 0)
        self.assertGreater(stat.modified, 0)
        self.assertGreater(stat.created, 0)
        self.assertEqual(stat.submitted, -1)
        self.assertEqual(stat.published, -1)
        self.assertNotEqual(stat.since_date, "pending")
        self.assertNotEqual(stat.modified_date, "pending")
        self.assertNotEqual(stat.created_date, "pending")
        self.assertEqual(stat.submitted_date, "(not yet submitted)")
        self.assertEqual(stat.published_date, "(not yet published)")

        stat._data[status._submitted_p] = 0
        stat.set_times()
        self.assertGreater(stat.submitted, 0)
        self.assertEqual(stat.published, -1)
        st = stat.submitted
        stat._data[status._published_p] = 0
        stat.set_times()
        self.assertEqual(stat.submitted, st)
        self.assertGreater(stat.published, st)

    def test_publish(self):
        stat = status.RecordStatus("goob", {})
        self.assertIsNone(stat.published_as)
        self.assertIsNone(stat.last_version)
        self.assertIsNone(stat.archived_at)
        self.assertLess(stat.published, 0)
        self.assertLess(stat.submitted, 0)

        stat.publish("ark:/88888/goob", "1.0.0")
        self.assertEqual(stat.published_as, "ark:/88888/goob")
        self.assertEqual(stat.last_version, "1.0.0")
        self.assertEqual(stat.state, status.PUBLISHED)
        self.assertIsNone(stat.archived_at)
        self.assertGreater(stat.published, 0)
        self.assertGreater(stat.submitted, 0)
        st = stat.submitted
        stat.set_state(status.EDIT)

        stat.publish("ark:/88888/goob", "1.2.0", "arch:goob")
        self.assertEqual(stat.state, status.PUBLISHED)
        self.assertEqual(stat.published_as, "ark:/88888/goob")
        self.assertEqual(stat.last_version, "1.2.0")
        self.assertEqual(stat.archived_at, "arch:goob")
        self.assertEqual(stat.submitted, st)
        self.assertGreater(stat.published, st)
        
        stat.set_state(status.SUBMITTED)
        self.assertGreater(stat.submitted, st)

    def test_pubreview(self):
        stat = status.RecordStatus("goob", {})
        sdata = stat.to_dict()
        self.assertNotIn("external_review", sdata)

        stat.pubreview("nps", "group-review", "goob", "/od/id/goob")
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "group-review")
        self.assertNotIn("feedback", rdata)

        fb = {
            "reviewer": "jerry",
            "type": "warn",
            "description": "this looks like dangerous gnostic data"
        }
        stat.pubreview("nps", "div-review", feedback=[fb], gurn="cranston")
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "div-review")
        self.assertEqual(rdata["gurn"], "cranston")
        self.assertIn("feedback", rdata)
        self.assertEqual(len(rdata["feedback"]), 1)
        self.assertEqual(rdata["feedback"][0]["reviewer"], "jerry")
        self.assertEqual(rdata["feedback"][0]["type"], "warn")
        self.assertTrue(rdata["feedback"][0]["description"])

        fb = {
            "reviewer": "gary",
            "type": "req",
            "description": "please pick a different color"
        }
        stat.pubreview("nps", "div-review", feedback=[fb], fbreplace=False, gurn="goober")
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "div-review")
        self.assertEqual(rdata["gurn"], "goober")
        self.assertIn("feedback", rdata)
        self.assertEqual(len(rdata["feedback"]), 2)
        self.assertEqual(rdata["feedback"][0]["reviewer"], "jerry")
        self.assertEqual(rdata["feedback"][0]["type"], "warn")
        self.assertTrue(rdata["feedback"][0]["description"])
        self.assertEqual(rdata["feedback"][1]["reviewer"], "gary")
        self.assertEqual(rdata["feedback"][1]["type"], "req")
        self.assertTrue(rdata["feedback"][1]["description"].startswith("please"))

        fb["description"] = "you must pick a different color"
        fb["when"] = "now"
        stat.pubreview("nps", "div-review", feedback=[fb])
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "div-review")
        self.assertEqual(rdata["gurn"], "goober")
        self.assertIn("feedback", rdata)
        self.assertEqual(len(rdata["feedback"]), 1)
        self.assertEqual(rdata["feedback"][0]["reviewer"], "gary")
        self.assertEqual(rdata["feedback"][0]["type"], "req")
        self.assertEqual(rdata["feedback"][0]["when"], "now")
        self.assertTrue(rdata["feedback"][0]["description"].startswith("you must"))
        
        stat.pubreview("nps", "div-review", feedback=[])
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "div-review")
        self.assertEqual(rdata["gurn"], "goober")
        self.assertIn("feedback", rdata)
        self.assertEqual(len(rdata["feedback"]), 0)

        stat.pubreview("elrs", "tech")
        sdata = stat.to_dict()
        self.assertIn("external_review", sdata)
        self.assertIn("nps", sdata['external_review'])
        rdata = sdata['external_review']['nps']
        self.assertEqual(rdata["@id"], "goob")
        self.assertEqual(rdata["info_at"], "/od/id/goob")
        self.assertEqual(rdata["phase"], "div-review")
        self.assertEqual(rdata["gurn"], "goober")
        self.assertIn("feedback", rdata)
        self.assertEqual(len(rdata["feedback"]), 0)

        self.assertIn("elrs", sdata['external_review'])
        rdata = sdata['external_review']['elrs']
        self.assertNotIn("@id", rdata)
        self.assertNotIn("info_at", rdata)
        self.assertEqual(rdata["phase"], "tech")
        self.assertNotIn("gurn", rdata)
        self.assertNotIn("feedback", rdata)

        stat.publish("gurn", "1.2.0")
        sdata = stat.to_dict()
        self.assertNotIn("external_review", sdata)
        
        

if __name__ == '__main__':
    test.main()
