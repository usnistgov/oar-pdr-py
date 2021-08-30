import os, sys, logging, argparse, pdb, imp, time, json, shutil, tempfile
import unittest as test

from nistoar.pdr import cli
from nistoar.pdr.cli.md.trans import latest
from nistoar.pdr.exceptions import PDRException, ConfigurationException
import nistoar.pdr.config as cfgmod
from nistoar.pdr.describe import RMMServerError, IDNotFound
from nistoar.pdr.distrib import DistribServerError, DistribResourceNotFound
from nistoar.pdr.utils import read_nerd
from nistoar.nerdm import constants as const

basedir = __file__
for i in range(8):
    basedir = os.path.dirname(basedir)
schemadir = os.path.join(basedir, 'metadata', 'model')
datadir1 = os.path.join(schemadir, "examples")
datadir2 = os.path.join(basedir, "jq", "tests", "data")
hitsc = os.path.join(datadir1, "hitsc-0.2.json")
simplenerd = os.path.join(datadir2, "simple-nerdm.json")

# assert(os.path.exists(schemadir))
# NERDM_SCH_ID_BASE = const.core_schema_base

tmparch = None
def setUpModule():
    global tmparch
    tmparch = tempfile.TemporaryDirectory(prefix="_test_latest.")

def tearDownModule():
    tmparch.cleanup()

class TestLatestCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_get.", dir=tmparch.name)
        self.cmd = cli.PDRCLI()
        self.cmd.load_subcommand(latest)

        self.config = {
#            "pdr_dist_base": self.distep,
#            "pdr_rmm_base":  self.mdep
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_cmd(self):
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        cmdline = "-q latest -o %s %s" % (outf, hitsc)
        self.cmd.execute(cmdline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(hitsc)
        ltst = read_nerd(outf)
        self.assertIn("@id", ltst)
        self.assertIn("title", ltst)

        self.assertEqual(ltst['@id'], nerdm['@id'])
        self.assertEqual(ltst['_schema'], const.CORE_SCHEMA_URI+"#")
        self.assertEqual(ltst['version'], "1.0")
        self.assertIn('version', ltst)
        self.assertNotIn('versionHistory', ltst)
        self.assertIn('releaseHistory', ltst)
        self.assertEqual(len(ltst['releaseHistory']['hasRelease']), 1)
        
        
        

if __name__ == '__main__':
    test.main()

