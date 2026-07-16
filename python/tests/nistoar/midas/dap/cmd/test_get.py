"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import get, create_DAPService, get_agent
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.midas.dbio import status
from nistoar.midas.dbio.project import NotSubmitable

tmpdir = tempfile.TemporaryDirectory(prefix="_test_review.")
testdir = Path(__file__).parents[0]

try:
    os.getlogin()
    def getlogin():
        return os.getlogin()
except OSError as ex:
    # this will happen if there is not controlling terminal
    import pwd
    def getlogin():
        return pwd.getpwuid(os.getuid())[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_review.log"))
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

class TestReviewCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(get)

        root = os.path.join(tmpdir.name, "dbfiles")
        self.cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": root,
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "mds3"
                    }
                }
            },
            'doi_naan': '10.88888',
            'nerdstorage': {
                'type': 'fsbased',
                'store_dir': os.path.join(root, "nerdm")
            }
        }
        self.log = logging.getLogger()
        
    def tearDown(self):
        os.environ['LOGNAME'] = getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q get mds3:88888".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "get")
        self.assertEqual(args.dbid, "mds3:88888")
        self.assertEqual(args.prop, [])
        self.assertFalse(args.outfile)
        self.assertFalse(args.dosumm)

        args = self.cmd.parse_args("-q get -s mds3:88888 data".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "get")
        self.assertEqual(args.dbid, "mds3:88888")
        self.assertEqual(args.prop, "data")
        self.assertFalse(args.outfile)
        self.assertTrue(args.dosumm)


    def test_dbio_rec(self):
        outfile = os.path.join(tmpdir.name, "out.json")
#        self.assertFalse(os.path.exists(outfile))
        args = self.cmd.parse_args(f"-q get mds3:0001 -o {outfile}".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob", {"title": "Hello"})
        self.assertEqual(rec.status.state, status.EDIT)

        self.cmd.execute(args, self.cfg)

        self.assertTrue(os.path.isfile(outfile))
        rec = read_json(outfile)

        self.assertEqual(rec.get('id'), 'mds3:0001')
        self.assertEqual(rec.get('name'), 'goob')
        self.assertIn('data', rec)
        self.assertEqual(rec['data'].get('title'), 'Hello')
        self.assertIn('status', rec)
        self.assertNotIn('components', rec['data'])

    

    def test_data(self):
        outfile = os.path.join(tmpdir.name, "out.json")
#        self.assertFalse(os.path.exists(outfile))
        args = self.cmd.parse_args(f"-q get mds3:0001 data -o {outfile}".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob", {"title": "Hello"})
        self.assertEqual(rec.data.get('title'), "Hello")
        self.assertEqual(rec.status.state, status.EDIT)

        self.cmd.execute(args, self.cfg)

        self.assertTrue(os.path.isfile(outfile))
        rec = read_json(outfile)

        self.assertNotIn('id', rec)
        self.assertNotIn('data', rec)
        self.assertNotIn('status', rec)
        self.assertIn('@id', rec)
        self.assertEqual(rec.get('title'), 'Hello')
        self.assertFalse(rec.get('components'))

    
        
        

        

if __name__ == '__main__':
    test.main()
        
