"""
test publish subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.pdr.preserve.cmd import restart
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

class TestRestartCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(restart)
        self.cfg = {}

    def test_parse(self):
        args = self.cmd.parse_args("-q restart mds2-88888 -W".split())
        self.assertIsNone(args.histdir)
        self.assertIsNone(args.presdir)
        self.assertIsNone(args.sipdir)
        self.assertEqual(args.wait, '1h')
        
        args = self.cmd.parse_args("-q restart -H /tmp/hdir -P /tmp/preserve -W 2m mds2-88888".split())
        self.assertEqual(args.histdir, '/tmp/hdir')
        self.assertEqual(args.presdir, '/tmp/preserve')
        self.assertIsNone(args.sipdir)
        self.assertEqual(args.wait, '2m')
        
    def test_gettimeout(self):
        log = logging.getLogger()
        self.assertEqual(restart.gettimeout(None, log), 60)
        self.assertEqual(restart.gettimeout('30', log), 30)
        self.assertEqual(restart.gettimeout('30s', log), 30)
        self.assertEqual(restart.gettimeout('30m', log), 1800)
        self.assertEqual(restart.gettimeout('1h', log), 3600)
        self.assertEqual(restart.gettimeout('1m', log), 60)
        


        

if __name__ == '__main__':
    test.main()




        
