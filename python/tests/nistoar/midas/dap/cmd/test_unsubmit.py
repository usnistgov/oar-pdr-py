"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import unsubmit, create_DAPService, get_agent
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.midas.dbio import status
from nistoar.midas.dbio.project import NotSubmitable

tmpdir = tempfile.TemporaryDirectory(prefix="_test_unsubmit.")
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
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_unsubmit.log"))
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
        self.cmd.load_subcommand(unsubmit)

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
                "nps_endpoint": "https://example.com/review",
                "as_system": "nps1"
            }
        }
        self.log = logging.getLogger()
        
    def tearDown(self):
        os.environ['LOGNAME'] = getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q unsubmit mds2-88888 -c simulated -f nps1".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "unsubmit")
        self.assertFalse(args.forgetall)
        self.assertEqual(args.forget, "nps1")
        self.assertEqual(args.cancel, "simulated")

    def test_reset(self):
        args = self.cmd.parse_args("-q unsubmit mds3:0001".split())
        
        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))

        svc.submit(rec.id)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.SUBMITTED)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'requested')
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'requested')
        self.assertNotEqual(list(prec.acls.iter_perm_granted('write')), [])

    def test_cancel(self):
        args = self.cmd.parse_args("-q unsubmit mds3:0001 -c nps1".split())
        
        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))

        svc.submit(rec.id)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.SUBMITTED)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'requested')
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'canceled')
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertNotEqual(list(prec.acls.iter_perm_granted('write')), [])


    def test_forget(self):
        args = self.cmd.parse_args("-q unsubmit mds3:0001 -f nps1".split())
        
        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))

        svc.submit(rec.id)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.SUBMITTED)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'requested')
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNone(rev)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertNotEqual(list(prec.acls.iter_perm_granted('write')), [])

    def test_forget_all(self):
        args = self.cmd.parse_args("-q unsubmit -F mds3:0001".split())
        
        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))

        svc.submit(rec.id)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.SUBMITTED)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('phase'), 'requested')
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        rev = prec.status.get_review_from("nps1")
        self.assertIsNone(rev)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertNotEqual(list(prec.acls.iter_perm_granted('write')), [])

        
        
        


        


if __name__ == '__main__':
    test.main()
