"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import revreq, create_DAPService, get_agent
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
        self.cmd.load_subcommand(revreq)

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
            'external_review': {
                "name": "simulated",
                "nps_endpoint": "https://example.com/review"
            }
        }
        self.log = logging.getLogger()
        
    def tearDown(self):
        os.environ['LOGNAME'] = getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q revreq mds2-88888 -C mindata --change rmfile".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "revreq")
        self.assertFalse(args.instruct)
        self.assertFalse(args.markreq)
        self.assertFalse(args.secrev)
        self.assertFalse(args.nosecrev)
        self.assertEqual(args.changes, ["mindata", "rmfile"])
        self.assertIsNone(args.server)
        self.assertIsNone(args.sysname)

    def test_create(self):
        args = self.cmd.parse_args("-q revreq mds3:0001".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)

        self.assertTrue(svc._extrevcli)
        
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("sim"))
        
        prec = svc.get_record("mds3:0001")
        self.assertTrue(prec)
        self.assertEqual(rec.id, prec.id)

    def test_apply(self):
        args = self.cmd.parse_args("-q revreq mds3:0001 -C newrec -r".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("sim"))

        self.cmd.execute(args, self.cfg)

        prec = svc.get_record(rec.id)
        rev = prec.status.get_review_from("simulated")
        self.assertTrue(rev)

        
        
        


        


if __name__ == '__main__':
    test.main()
