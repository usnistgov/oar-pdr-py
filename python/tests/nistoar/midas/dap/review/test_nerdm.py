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

        self.nerd["description"] = ""
        del self.nerd["keyword"]

        res = self.val.test_simple_props(self.nerd)
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_passed(), 1)
        self.assertIn("#title", res.passed()[0].label)
        self.assertEqual(res.count_failed(), 2)


        





if __name__ == '__main__':
    test.main()



