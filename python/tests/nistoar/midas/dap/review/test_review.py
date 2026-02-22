import os, sys, pdb, json, tempfile, logging
import unittest as test
from pathlib import Path

import nistoar.midas.dap.review as rev
from nistoar.midas.dap.review.nerdm import DAPNERDmValidator
from nistoar.midas.dbio import ProjectRecord, DAP_PROJECTS
from nistoar.midas.dbio.wsgi.project import ProjectStatusHandler
import nistoar.pdr.utils.validate as base
from nistoar.pdr.utils.io import read_nerd
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage

testdir = Path(__file__).parent
datadir = testdir.parent / "data"
sipdir = datadir / "mdssip"/"mdst:1491"

class TestDAPReviewer(test.TestCase):

    def setUp(self):
        nerd = read_nerd(sipdir/"nerdm.json")
        self.store = InMemoryResourceStorage()
        self.store.load_from(nerd, "mdst:1491")
        self.prec = ProjectRecord(DAP_PROJECTS, { "id": "mdst:1491", "data": {}, "meta": {} })
        self.reviewer = rev.DAPReviewer.create_reviewer(self.store)


    def test_ctor(self):
        self.assertIs(self.reviewer.store, self.store)
        self.assertEqual(len(self.reviewer.nrdvals), 1)
        self.assertTrue(isinstance(self.reviewer.nrdvals[0], DAPNERDmValidator))
        self.assertEqual(len(self.reviewer.dapvals), 0)

    def test_validate(self):
        res = self.reviewer.validate(self.prec)
        self.assertEqual(res.target, "mdst:1491")
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_passed(), 3)
        
    def test_export(self):
        self.store.load_from({"@id": "mdst-1492"}, "mdst:1492")
        self.prec = ProjectRecord(DAP_PROJECTS, { "id": "mdst:1492", "data": {}, "meta": {} })
        self.reviewer = rev.DAPReviewer.create_reviewer(self.store)

        res = self.reviewer.validate(self.prec)
        self.assertEqual(res.target, "mdst:1492")
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_failed(), 3)

        todo = ProjectStatusHandler.export_review(res)
        self.assertEqual(len(todo.get("req", [])), 3)
        self.assertEqual(len(todo.get("warn", [1])), 0)
        self.assertEqual(len(todo.get("rec", [1])), 0)
        
        self.assertEqual(todo["req"][0].get("subject"), "title")
        self.assertEqual(todo["req"][0].get("summary"), "Add a title")
        self.assertIn("title", todo["req"][0].get("details", [""])[0])


if __name__ == '__main__':
    test.main()



