import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test

from nistoar.pdr import cli
from nistoar.pdr.cli.md.trans import rmm
from nistoar.pdr.exceptions import PDRException, ConfigurationException
from nistoar.pdr import config as cfgmod
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

class TestRmmCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_rmm.", dir=tmparch.name)
        self.cmd = cli.PDRCLI()
        self.cmd.load_subcommand(rmm)

        self.config = {
#            "pdr_dist_base": self.distep,
#            "pdr_rmm_base":  self.mdep
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_cmd(self):
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        cmdline = "-q rmm -o %s -F %s" % (outf, hitsc)
        self.cmd.execute(cmdline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(hitsc)
        ing = read_nerd(outf)

        for prop in "record version releaseSet".split():
            self.assertIn(prop, ing)

        self.assertEqual(ing['record']['@id'], nerdm['@id'])
        self.assertEqual(ing['record']['title'], nerdm['title'])
        self.assertEqual(ing['record']['_schema'], const.CORE_SCHEMA_URI+"#")
        self.assertEqual(ing['record']['version'], "1.0")
        self.assertNotIn('versionHistory', ing['record'])
        self.assertIn('releaseHistory', ing['record'])
        self.assertEqual(len(ing['record']['releaseHistory']['hasRelease']), 1)
        self.assertEqual(ing['version']['@id'], nerdm['@id']+"/pdr:v/1.0")
        self.assertEqual(ing['version']['releaseHistory']['hasRelease'][0]['version'], "1.0")
        self.assertEqual(ing['version']['releaseHistory']['hasRelease'][0]['description'], "initial release")

        self.assertEqual(ing['releaseSet']['@id'], nerdm['@id']+"/pdr:v")
        self.assertEqual(ing['releaseSet']['title'], nerdm['title'])
        self.assertEqual(ing['releaseSet']['_schema'], const.CORE_SCHEMA_URI+"#")
        self.assertEqual(ing['releaseSet']['version'], "1.0")
        self.assertIn('version', ing['releaseSet'])
        self.assertNotIn('versionHistory', ing['releaseSet'])
        self.assertNotIn('releaseHistory', ing['releaseSet'])
        self.assertIn('hasRelease', ing['releaseSet'])
        self.assertEqual(len(ing['releaseSet']['hasRelease']), 1)
        self.assertEqual(ing['releaseSet']['hasRelease'][0]['version'], "1.0")
        self.assertEqual(ing['releaseSet']['hasRelease'][0]['description'], "initial release")
        
        
        
        

if __name__ == '__main__':
    test.main()

