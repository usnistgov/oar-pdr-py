import os, sys, pdb, json
import unittest as test
from pathlib import Path

import nistoar.midas.dap.review.nerdm as rev
import nistoar.pdr.utils.validate as base
from nistoar.pdr.utils.io import read_nerd

testdir = Path(__file__).parent
datadir = testdir.parent / "data"
sipdir = datadir / "mdssip"/"mdst:1491"

class TestDAPNERDmValidator(test.TestCase):

    def setUp(self):
        self.nerd = read_nerd(sipdir/"nerdm.json")
        self.val = rev.DAPNERDmValidator()

    def test_test_simple_props(self):
        res = self.val.test_simple_props(self.nerd)
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_passed(), 3)
        self.assertTrue(res.passed()[0].label.endswith("#title"))
        self.assertTrue(res.passed()[1].label.endswith("#description"))
        self.assertTrue(res.passed()[2].label.endswith("#keyword"))
        self.assertEqual(res.passed()[0].comments[0], "Add a title")
        self.assertEqual(res.passed()[1].comments[0], "Add a description")
        self.assertEqual(res.passed()[2].comments[0], "Add some keywords")

        self.nerd["description"] = ""
        del self.nerd["keyword"]

        res = self.val.test_simple_props(self.nerd)
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_passed(), 1)
        self.assertIn("#title", res.passed()[0].label)
        self.assertEqual(res.count_failed(), 2)
        self.assertTrue(res.failed()[0].label.endswith("#description"))
        self.assertTrue(res.failed()[1].label.endswith("#keyword"))
        self.assertEqual(res.failed()[0].comments[0], "Add a description")
        self.assertEqual(res.failed()[1].comments[0], "Add some keywords")

        





if __name__ == '__main__':
    test.main()



