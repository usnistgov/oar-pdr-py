import os, json, pdb, logging, time
import unittest as test

from nistoar.pdr.publish.idmint import minter, registry
from nistoar.pdr.exceptions import StateException, ConfigurationException

from nistoar.testing import *
from nistoar.pdr.describe import rmm

class TestPDPMinter(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.cachedir = self.tf.mkdir("idreg")
        self.cfg = {
            'id_shoulder': 'pdp1.',
            'naan': '88888',
            'store_dir': self.cachedir,
            'sequence_start': 10,
            'registry': {
                "foo": "bar"
            }
        }
        self.cachefile = os.path.join(self.cachedir, "pdp1-issued-ids.tsv")

    def tearDown(self):
        self.tf.clean()

    def test_ctor(self):
        mntr = minter.PDPMinter(self.cfg)
        self.assertEqual(mntr.shldr, "pdp1.")
        self.assertEqual(mntr.naan, '88888')
        self.assertIsNotNone(mntr._seqminter)
        self.assertEqual(mntr._seqminter.nextn, 10)
        self.assertFalse(mntr.baseondata)

        self.assertIsNotNone(mntr.registry)
        self.assertEqual(mntr.registry.store, self.cachefile)
        self.assertIsNone(mntr.registry.initloader)
        self.assertTrue(mntr.registry.cache_immediately)
        self.assertEqual(len(mntr.registry.data), 0)

        self.cfg['based_on_sipid'] = True
        reg = registry.CachingIDRegistry(self.cachedir, self.cfg['registry'], name="bob")
        mntr = minter.PDPMinter(self.cfg, "cnr0-", 25, reg)
        self.assertEqual(mntr.shldr, "cnr0-")
        self.assertEqual(mntr.naan, '88888')
        self.assertIsNotNone(mntr._seqminter)
        self.assertEqual(mntr._seqminter.nextn, 25)
        self.assertTrue(mntr.baseondata)

        self.assertIs(mntr.registry, reg)
        self.assertEqual(mntr.registry.store, os.path.join(self.cachedir, "bob-issued-ids.tsv"))
        self.assertFalse(hasattr(mntr.registry, 'initloader'))
        self.assertTrue(mntr.registry.cache_immediately)
        self.assertEqual(len(mntr.registry.data), 0)

    def test_mint(self):
        mntr = minter.PDPMinter(self.cfg)
        id = mntr.mint({"sipid": "goob"})
        self.assertEqual(id, "ark:/88888/pdp1.0010s0")
        id = mntr.mint({"sipid": "goob"})
        self.assertEqual(id, "ark:/88888/pdp1.0011sh")
        id = mntr.mint()
        self.assertEqual(id, "ark:/88888/pdp1.0012s1")
        self.assertTrue(os.path.exists(mntr.registry.store))
        with open(mntr.registry.store) as fd:
            self.assertEqual(len([line[0] for line in fd]), 3)

        self.cfg['based_on_sipid'] = True
        mntr = minter.PDPMinter(self.cfg)
        self.assertTrue(mntr.issued("ark:/88888/pdp1.0011sh"))
        self.assertFalse(mntr.issued("ark:/88888/pdp1.0011s0"))
        self.assertEqual(mntr.datafor("ark:/88888/pdp1.0011sh"), {"sipid": "goob"})
        self.assertIsNone(mntr.datafor("ark:/88888/pdp1.0011s0"))
        
        id = mntr.mint({"sipid": "goob"})
        self.assertEqual(id, "ark:/88888/pdp1.sgoob")
        id = mntr.mint({"sipid": "971-376"})
        self.assertEqual(id, "ark:/88888/pdp1.s971-376")
        id = mntr.mint()
        self.assertEqual(id, "ark:/88888/pdp1.0013sj")
        id = mntr.mint({"ediid": "goob"})
        self.assertEqual(id, "ark:/88888/pdp1.0014s2")
        with self.assertRaises(StateException):
            id = mntr.mint({"sipid": "goob"})
        with open(mntr.registry.store) as fd:
            self.assertEqual(len([line[0] for line in fd]), 7)
            
        matches = mntr.search({"sipid": "goob"})
        self.assertIn("ark:/88888/pdp1.sgoob", matches)
        self.assertIn("ark:/88888/pdp1.0010s0", matches)
        self.assertIn("ark:/88888/pdp1.0011sh", matches)
        self.assertEqual(len(matches), 3)

    def test_id_for_sipid(self):
        mntr = minter.PDPMinter(self.cfg)
        self.assertIsNone(mntr.id_for_sipid("goob"))

        self.cfg['based_on_sipid'] = True
        mntr = minter.PDPMinter(self.cfg)
        self.assertEqual(mntr.id_for_sipid("goob"), "ark:/88888/pdp1.sgoob")
        self.assertEqual(mntr.id_for_sipid("foo"), "ark:/88888/pdp1.sfoo")
        self.assertEqual(mntr.id_for_sipid("ark:/20/foo"), "ark:/88888/pdp1.sfoo")
        self.assertEqual(mntr.id_for_sipid("pdp1.foo"), "ark:/88888/pdp1.sfoo")
        self.assertEqual(mntr.id_for_sipid("ark:/20/pdp1.foo"), "ark:/88888/pdp1.sfoo")
                         
if __name__ == '__main__':
    test.main()
        
        
