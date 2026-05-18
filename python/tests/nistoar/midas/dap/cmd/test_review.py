"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import review, create_DAPService, get_agent
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
        self.cmd.load_subcommand(review)

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
            'doi_naan': '10.88888'
        }
        self.log = logging.getLogger()
        
    def tearDown(self):
        os.environ['LOGNAME'] = getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q review mds2-88888 goob".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "review")
        self.assertEqual(args.phase, "goob")
        self.assertEqual(args.revsys, "testrev")
        self.assertIs(args.replace, False)
        self.assertIs(args.change, False)
        self.assertIsNone(args.revid)
        self.assertIsNone(args.feedback)

    def test_apply(self):
        args = self.cmd.parse_args("-q review mds3:0001 open".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("testrev"))
        
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "open")
        self.assertNotIn('info_at', rev)
        self.assertNotIn('feedback', rev)

        args = "-q review -U http://goober.net mds3:0001 tech".split()
        args.extend(["-f", "Looks good!", "-f", "warn: Proceed!"])
        args = self.cmd.parse_args(args)
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "tech")
        self.assertEqual(rev.get('info_at'), "http://goober.net")
        fb = rev.get('feedback')
        self.assertIsNotNone(fb)
        self.assertEqual(len(fb), 2)
        self.assertEqual(fb[0]['reviewer'], "unknown")
        self.assertEqual(fb[1]['reviewer'], "unknown")
        self.assertEqual(fb[0]['description'], "Looks good!")
        self.assertEqual(fb[1]['description'], "Proceed!")
        self.assertEqual(fb[1]['type'], "warn")
        self.assertNotIn('type', fb[0])

        svc.submit("mds3:0001")
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.SUBMITTED)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "tech")
        self.assertEqual(rev.get('info_at'), "http://goober.net")
        fb = rev.get('feedback')
        self.assertIsNotNone(fb)
        self.assertEqual(len(fb), 2)
        self.assertEqual(fb[0]['reviewer'], "unknown")
        self.assertEqual(fb[1]['reviewer'], "unknown")
        self.assertEqual(fb[0]['description'], "Looks good!")
        self.assertEqual(fb[1]['description'], "Proceed!")
        self.assertEqual(fb[1]['type'], "warn")
        self.assertNotIn('type', fb[0])

        # test that you can't publish with an unfinished review
        with self.assertRaises(NotSubmitable):
            svc.publish("mds3:0001")
        
        args = "-q review -U http://goober.net mds3:0001 groupmgr -E".split()
        args.extend(["-n", "J. Bossman", "-f", "req:keep it clean "])
        args = self.cmd.parse_args(args)
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "groupmgr")
        self.assertEqual(rev.get('info_at'), "http://goober.net")
        fb = rev.get('feedback')
        self.assertIsNotNone(fb)
        self.assertEqual(len(fb), 3)
        self.assertEqual(fb[0]['reviewer'], "unknown")
        self.assertEqual(fb[1]['reviewer'], "unknown")
        self.assertEqual(fb[0]['description'], "Looks good!")
        self.assertEqual(fb[1]['description'], "Proceed!")
        self.assertEqual(fb[1]['type'], "warn")
        self.assertNotIn('type', fb[0])
        self.assertEqual(fb[2]['reviewer'], "J. Bossman")
        self.assertEqual(fb[2]['type'], "req")
        self.assertEqual(fb[2]['description'], "keep it clean")

        args = "-q review mds3:0001 divmgr -X".split()
        args.extend(["-n", "dir3", "-f", "all good"])
        args = self.cmd.parse_args(args)
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "divmgr")
        self.assertEqual(rev.get('info_at'), "http://goober.net")
        fb = rev.get('feedback')
        self.assertIsNotNone(fb)
        self.assertEqual(len(fb), 1)
        self.assertEqual(fb[0]['reviewer'], "dir3")
        self.assertEqual(fb[0]['description'], "all good")
        
        args = "-q review mds3:0001 oumgr -X".split()
        args = self.cmd.parse_args(args)
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "oumgr")
        self.assertEqual(rev.get('info_at'), "http://goober.net")
        fb = rev.get('feedback')
        self.assertIsNotNone(fb)
        self.assertEqual(len(fb), 0)

    def test_cancel(self):
        args = "-q review mds3:0001 open".split()
        args.extend(["-f", "Uh, oh"])
        args = self.cmd.parse_args(args)

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("testrev"))
        
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "open")
        self.assertNotIn('info_at', rev)
        fb = rev.get('feedback')
        self.assertEqual(len(fb), 1)
        self.assertEqual(fb[0]['reviewer'], "unknown")
        self.assertEqual(fb[0]['description'], "Uh, oh")

        args = self.cmd.parse_args("-q review mds3:0001 cancel".split())
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.EDIT)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "canceled")
        self.assertNotIn('info_at', rev)
        self.assertEqual(rev.get('feedback'), [])

    def test_approve(self):
        args = "-q review mds3:0001 open".split()
        args.extend(["-f", "Uh, oh"])
        args = self.cmd.parse_args(args)

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("testrev"))

        svc.submit("mds3:0001")
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.SUBMITTED)

        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.SUBMITTED)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "open")
        self.assertNotIn('info_at', rev)
        fb = rev.get('feedback')
        self.assertEqual(len(fb), 1)
        self.assertEqual(fb[0]['reviewer'], "unknown")
        self.assertEqual(fb[0]['description'], "Uh, oh")

        args = self.cmd.parse_args("-q review mds3:0001 approve".split())
        self.cmd.execute(args, self.cfg)
        rec = svc.get_record("mds3:0001")
        self.assertEqual(rec.status.state, status.ACCEPTED)
        rev = rec.status.get_review_from("testrev")
        self.assertIsNotNone(rev)
        self.assertEqual(rev.get('@id'), "mds3:0001")
        self.assertEqual(rev.get('phase'), "approved")
        self.assertNotIn('info_at', rev)
        self.assertEqual(rev.get('feedback'), [])
        


if __name__ == '__main__':
    test.main()




        
