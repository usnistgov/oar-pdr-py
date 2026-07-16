"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import revperm, create_DAPService, get_agent
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.midas.dbio import status
from nistoar.midas.dbio.project import NotSubmitable

tmpdir = tempfile.TemporaryDirectory(prefix="_test_revperm.")
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

class TestRevpermCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(revperm)

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
            'reviewer_ids': [ "greenlantern" ],
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
        args = self.cmd.parse_args("-q revperm mds2-88888 -U".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "revperm")
        self.assertTrue(args.unset)
        self.assertFalse(args.for_review)
        self.assertEqual(args.readers, [])

        args = self.cmd.parse_args("-q revperm mds2-88888 -r frodo,sam -r gollum".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "revperm")
        self.assertFalse(args.unset)
        self.assertFalse(args.for_review)
        self.assertEqual(args.readers, "frodo,sam gollum".split())

    def test_set_unset(self):
        args = self.cmd.parse_args("-q revperm mds3:0001".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))
        self.assertEqual(list(rec.acls.iter_perm_granted('read')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('write')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('admin')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('delete')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('publish')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('_write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_delete')), [who.actor])

        args = self.cmd.parse_args("-q revperm mds3:0001 -u".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern"])
        permitted = list(prec.acls.iter_perm_granted('write'))
        self.assertIn(who.actor, permitted)
        self.assertIn("greenlantern", permitted)
        self.assertEqual(len(permitted), 2)
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_admin')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_delete')), [])

        # Return to review process
        args = self.cmd.parse_args("-q revperm mds3:0001".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('_write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_delete')), [who.actor])

        # Complete review process
        args = self.cmd.parse_args("-q revperm mds3:0001 -U".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), [])
        
    def test_set_unset_with_readers(self):
        args = self.cmd.parse_args("-q revperm mds3:0001 -r frodo,gollum".split())

        who = get_agent(args, self.cfg)
        svc = create_DAPService(who, args, self.cfg, self.log)
        self.assertTrue(svc._extrevcli)
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)
        self.assertIsNone(rec.status.get_review_from("nps1"))
        self.assertEqual(list(rec.acls.iter_perm_granted('read')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('write')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('admin')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('delete')), [who.actor])
        self.assertEqual(list(rec.acls.iter_perm_granted('publish')), [])

        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern",
                                                                     "frodo", "gollum"])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('_write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_delete')), [who.actor])

        args = self.cmd.parse_args("-q revperm mds3:0001 -u".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern",
                                                                     "frodo", "gollum"])
        permitted = list(prec.acls.iter_perm_granted('write'))
        self.assertIn(who.actor, permitted)
        self.assertIn("greenlantern", permitted)
        self.assertEqual(len(permitted), 2)
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_write')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_admin')), [])
        self.assertNotEqual(list(prec.acls.iter_perm_granted('_delete')), [])

        # Return to review process
        args = self.cmd.parse_args("-q revperm mds3:0001".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor, "greenlantern",
                                                                     "frodo", "gollum"])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), ["greenlantern"])
        self.assertEqual(list(prec.acls.iter_perm_granted('_write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('_delete')), [who.actor])

        # Complete review process
        args = self.cmd.parse_args("-q revperm mds3:0001 -U".split())
        self.cmd.execute(args, self.cfg)
        prec = svc.get_record(rec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(list(prec.acls.iter_perm_granted('read')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('write')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('admin')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('delete')), [who.actor])
        self.assertEqual(list(prec.acls.iter_perm_granted('publish')), [])
        
        

        


if __name__ == '__main__':
    test.main()
