"""
test extrev.sim module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.midas.dap.extrev import sim, create_external_review_client
from nistoar.midas.dbio.project import NotSubmitable

tmpdir = tempfile.TemporaryDirectory(prefix="_test_extrev.")
testdir = Path(__file__).parents[0]

class TestSimulatedExternalReviewClient(test.TestCase):

    def setUp(self):
        self.cfg = {
            "name": "simulated"
        }

    def test_ctor(self):
        revcli = sim.SimulatedExternalReviewClient(self.cfg, False)
        self.assertFalse(revcli.autoapp)
        self.assertIsNone(revcli.projsvc)
        self.assertEqual(revcli.projs, {})

        revcli = sim.SimulatedExternalReviewClient(self.cfg)
        self.assertTrue(revcli.autoapp)
        self.assertIsNone(revcli.projsvc)
        self.assertEqual(revcli.projs, {})

    def test_factory(self):
        revcli = create_external_review_client(self.cfg)
        self.assertTrue(revcli)
        self.assertTrue(isinstance(revcli, sim.SimulatedExternalReviewClient))

    def test_submit(self):
        id = "pdr0:0001"
        revcli = sim.SimulatedExternalReviewClient(self.cfg, False)
        revcli.submit(id, "adm", instructions="keep it")
        self.assertIn(id, revcli.projs)
        self.assertEqual(revcli.projs[id]['submitter'], 'adm')
        self.assertEqual(revcli.projs[id]['phase'], 'requested')
        self.assertEqual(revcli.projs[id]['options']['instructions'], 'keep it')

        with self.assertRaises(sim.ExternalReviewException):
            revcli.submit(id, "goober")

        revcli = sim.SimulatedExternalReviewClient(self.cfg)
        revcli.submit(id, "adm", "1.0.1", instructions="keep it")
        self.assertIn(id, revcli.projs)
        self.assertEqual(revcli.projs[id]['submitter'], 'adm')
        self.assertEqual(revcli.projs[id]['phase'], 'approved')
        self.assertEqual(revcli.projs[id]['options']['instructions'], 'keep it')

        revcli.submit("pdr0-0002", "goober", "1.0.1")
        self.assertIn(id, revcli.projs)
        self.assertIn("pdr0-0002", revcli.projs)

        
    def test_update_approve(self):
        id = "pdr0:0001"
        revcli = sim.SimulatedExternalReviewClient(self.cfg, False)
        revcli.submit(id, "adm", instructions="keep it")
        self.assertEqual(revcli.projs[id]['phase'], 'requested')

        revcli.update(id, "group", None, ["nice!"])
        self.assertEqual(revcli.projs[id]['submitter'], 'adm')
        self.assertEqual(revcli.projs[id]['phase'], 'group')
        self.assertEqual(revcli.projs[id]['options']['feedback'], ['nice!'])

        revcli.approve(id, True)
        self.assertEqual(revcli.projs[id]['submitter'], 'adm')
        self.assertEqual(revcli.projs[id]['phase'], 'approved')
        self.assertEqual(revcli.projs[id]['options']['feedback'], ['nice!'])
        


if __name__ == '__main__':
    test.main()




        
