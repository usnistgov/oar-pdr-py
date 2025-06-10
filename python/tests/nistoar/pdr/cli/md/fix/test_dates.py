import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test

from nistoar.pdr.utils import cli
from nistoar.pdr.cli.md.fix import dates
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
    tmparch = tempfile.TemporaryDirectory(prefix="_test_fix.")

def tearDownModule():
    tmparch.cleanup()

class TestDatesCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_dates.", dir=tmparch.name)
        self.cmd = cli.CLISuite("test")
        self.cmd.load_subcommand(dates)

        self.config = {
#            "pdr_dist_base": self.distep,
#            "pdr_rmm_base":  self.mdep
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_normalize_date_str(self):
        for ds in "1998 2005-10 2020-01-12 1955-12-25T12:10 1901-12-25T12:10:55 1901-12-25T12:10:55.1351".split():
            try:
                self.assertEqual(dates.normalize_date_str(ds), ds)
            except ValueError as ex:
                self.fail("Legal date doesn't normalize: "+ds)
            self.assertEqual(dates.normalize_date_str("1955-12-25T12"), "1955-12-25T12:00")

        for ds in "1998T12:10 2020-15-31 2020-07-32 1975-04-22T28 1975-04-22T28 1975-04-22T20:62".split():
            with self.assertRaises(ValueError):
                dates.normalize_date_str(ds)

    def test_parse_date(self):
        self.assertEqual(dates._parse_date(["1998"], {}, "g", None), "1998")
        self.assertEqual(dates._parse_date(["1998-12"], {}, "g", None), "1998-12")

        self.assertEqual(dates._parse_date(["1998-12-09"], {}, "g", None), "1998-12-09")
        self.assertEqual(dates._parse_date(["1998-12-09T14"], {}, "g", None), "1998-12-09T14:00")
        self.assertEqual(dates._parse_date(["1998-12-09T14:50:05.0"], {}, "g", None), "1998-12-09T14:50:05.0")

        for ds in "1998T12:10 2020-15-31 2020-07-32 1975-04-22T28 1975-04-22T28 1975-04-22T20:62".split():
            with self.assertRaises(cli.CommandFailure):
                dates._parse_date([ds], {}, "g", None)
        
    def test_cmd(self):
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        cmdline = "-q dates -o %s -F %s -fimra 1998" % (outf, hitsc)
        self.cmd.execute(cmdline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(hitsc)
        fixd = read_nerd(outf)
        self.assertIn("@id", fixd)
        self.assertIn("title", fixd)
        self.assertEqual(fixd['@id'], nerdm['@id'])
        for prop in "firstIssued revised annotated".split():
            self.assertNotIn(prop, nerdm, "Unexpectedly found new date in old rec: %s: %s" %
                             (prop, nerdm.get(prop)))

        for prop in "firstIssued revised annotated issued modified".split():
            self.assertEqual(fixd[prop], "1998")

    def test_cmd_missing_prop(self):
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        cmdline = "-q dates -o %s -F %s 1998-01-01" % (outf, hitsc)
        try:
            self.cmd.execute(cmdline.split(), {})
            self.fail("Failed to detect missing date type options")
        except cli.CommandFailure as ex:
            self.assertIn("One of ", str(ex), "Unexpected error detected: "+str(ex))
        
        
        

if __name__ == '__main__':
    test.main()

