"""
test publish subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.pdr.preserve.cmd import status
from nistoar.base import config as cfgmod
from nistoar.pdr.utils.cli import CommandFailure, explain

tmpdir = tempfile.TemporaryDirectory(prefix="_test_preserve.")
testdir = Path(__file__).parents[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_preserve.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestStatusCmd(test.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.workdir = Path(tmpdir.name)/"preserve"
        cls.workdir.mkdir()
        (cls.workdir/"mds3-0002").mkdir()
        with open(cls.workdir/"mds3-0002"/"_state.json", 'w') as fd:
            json.dump({
                "_aipid": "mds3-0002",
                "_orig_sip": "/data/pdr/publish/pdp1/submitted/mds3:0002",
                "_stage_dir": "/data/pdr/preserve/mds3-0002/stage",
                "_work_dir": "/data/pdr/preserve/mds3-0002/work",
                "_completed": 1,
                "_message": "finalizing",
                "_pubstatfile": "/data/pdr/publish/pdp1/status/mds3:0002.json",
                "version": "(unknown)"
            }, fd)
        (cls.workdir/"_history").mkdir()
        with open(cls.workdir/"_history"/"mds3-0004_history.json", 'w') as fd:
            json.dump({
                "version": "1.0.0",
                "reqtime": 1790828243.9223738,
                "reqdate": "2026-10-01T04:17:23.922374",
                "comptime": 1790828244.6104958,
                "runtime": 0.3779940605163574,
                "exitcode": 0,
                "jobpid": 13,
                "headbag": "mds3-0004.1_0_0.mbag0_4-0.zip",
                "aipid": "mds3-0004",
                "message": "Preservation and publication completed",
                "steps": 127,
                "laststep": "published"
            }, fd)

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(status)
        self.ofile = self.workdir/"status.txt"
        self.cfg = {}

    def tearDown(self):
        if self.ofile.exists():
            self.ofile.unlink()

    def test_parse(self):
        args = self.cmd.parse_args("-q status -s mds2-88888".split()) 
        self.assertEqual(args.workdir, '')
        self.assertIsNone(args.histdir)
        self.assertIsNone(args.presdir)
        self.assertIsNone(args.outfile)
        self.assertTrue(args.silent)
        self.assertFalse(args.stateonly)

        args = self.cmd.parse_args("-q status -t -o /tmp/state.txt -H /tmp/hdir -P /tmp/preserve mds2-88888".split()) 
        self.assertEqual(args.workdir, '')
        self.assertEqual(args.histdir, "/tmp/hdir")
        self.assertEqual(args.presdir, "/tmp/preserve")
        self.assertEqual(args.outfile, "/tmp/state.txt")
        self.assertTrue(not args.silent)
        self.assertFalse(not args.stateonly)

    def test_stateonly_unstarted(self):
        args = self.cmd.parse_args(f"-q status -t -o {self.ofile} -P /tmp mds2-0001".split())

        self.cmd.execute(args, self.cfg)
        self.assertTrue(self.ofile.is_file())
        with open(self.ofile) as fd:
            out = fd.read().strip().splitlines()

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0], "unstarted")

    def test_stateonly_completed(self):
        args = self.cmd.parse_args(f"-q status -t -o {self.ofile} -P {self.workdir} -H {self.workdir/'_history'} mds3-0004".split())

        self.cmd.execute(args, self.cfg)
        self.assertTrue(self.ofile.is_file())
        with open(self.ofile) as fd:
            out = fd.read().strip().splitlines()

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0], "completed")

    def test_describe_unstarted(self):
        args = self.cmd.parse_args(f"-q status -o {self.ofile} -P /tmp mds2-0001".split())

        self.cmd.execute(args, self.cfg)
        self.assertTrue(self.ofile.is_file())
        with open(self.ofile) as fd:
            out = fd.read().strip().splitlines()

        self.assertGreater(len(out), 2)
        self.assertEqual(out[0], "Preservation status for mds2-0001:")
        self.assertTrue(out[1].startswith("UNSUBMITTED"))

    def test_describe_inprogress(self):
        args = self.cmd.parse_args(f"-q status -o {self.ofile} -P {self.workdir} -H {self.workdir/'_history'} mds3-0002".split())

        self.cmd.execute(args, self.cfg)
        self.assertTrue(self.ofile.is_file())
        with open(self.ofile) as fd:
            out = fd.read().strip().splitlines()

        self.assertGreater(len(out), 2)
        self.assertEqual(out[0], "Preservation status for mds3-0002:")
        self.assertTrue(out[1].startswith("IN PROGRESS version (unknown)"))

    def test_describe_completed(self):
        args = self.cmd.parse_args(f"-q status -o {self.ofile} -P {self.workdir} -H {self.workdir/'_history'} mds3-0004".split())

        self.cmd.execute(args, self.cfg)
        self.assertTrue(self.ofile.is_file())
        with open(self.ofile) as fd:
            out = fd.read().strip().splitlines()

        self.assertGreater(len(out), 2)
        self.assertEqual(out[0], "Preservation status for mds3-0004:")
        self.assertTrue(out[1].startswith("COMPLETED version 1.0.0"))


        

if __name__ == '__main__':
    test.main()




        
