"""
test create subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

import jwt

from nistoar.pdr.utils import cli
from nistoar.midas.cli.jwt import create

tmpdir = tempfile.TemporaryDirectory(prefix="_test_jwt_create.")
testdir = Path(__file__).parents[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_jwt_create.log"))
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

class TestJWTCreateCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(create)
        self.cfg = { "secret": "goober" }
        self.outf = os.path.join(tmpdir.name, "token.txt")

    def tearDown(self):
        if os.path.isfile(self.outf):
            os.remove(self.outf)

    def test_parse(self):
        args = self.cmd.parse_args("-q create -o goob.txt oarop pipe".split())
        self.assertEqual(args.subj, "oarop")
        self.assertEqual(args.agclass, "pipe")
        self.assertEqual(args.acttype, "auto")
        self.assertEqual(args.outfile, "goob.txt")
        self.assertEqual(len(args.props), 0)
        self.assertEqual(len(args.agents), 0)
        self.assertEqual(args.lifetime, "2y")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "create")

        args = self.cmd.parse_args("-q create -u -a bob -a alice,carol oarop pipe".split())
        self.assertEqual(args.subj, "oarop")
        self.assertEqual(args.agclass, "pipe")
        self.assertEqual(args.acttype, "user")
        self.assertIsNone(args.outfile)
        self.assertEqual(args.props, [])
        self.assertEqual(args.agents, ["bob", "alice,carol"])
        self.assertEqual(args.lifetime, "2y")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "create")

    def test_make_data(self):
        args = self.cmd.parse_args("-q create oarop pipe".split())
        data = create._make_data(args)
        self.assertEqual(data['client_id'], 'pipe')
        self.assertEqual(data['actortype'], 'auto')
        self.assertEqual(len(data), 2)

        args = self.cmd.parse_args("-q create -u -p gurn=goob -a bob -p lifetime=0s -a alice,carol -p goober=peas -p client_id=yaya oarop pipe".split())
        data = create._make_data(args)
        self.assertEqual(data['client_id'], 'pipe')
        self.assertEqual(data['actortype'], 'user')
        self.assertEqual(data['agents'], "bob alice carol")
        self.assertEqual(data['gurn'], "goob")
        self.assertEqual(data['goober'], "peas")
        for prop in "lifetime sub exp client_id":
            self.assertNotIn(prop, data)
        
    def test_execute(self):
        args = self.cmd.parse_args(f"-q create -o {self.outf} oarop pipe".split())
        self.assertTrue(not os.path.exists(self.outf))
        create.execute(args, self.cfg)
        self.assertTrue(os.path.isfile(self.outf))

        with open(self.outf) as fd:
            token = fd.read().strip()
        self.assertTrue(token)

        info = jwt.decode(token, self.cfg['secret'], "HS256")
        self.assertEqual(info["sub"], "oarop")
        self.assertEqual(info["client_id"], "pipe")
        self.assertEqual(info["actortype"], "auto")
        self.assertLess(info["exp"] - time.time(), 3600*24*365*2)
        self.assertGreater(info["exp"] - time.time(), 3600*24*365*2-60)
        
    def test_execute2(self):
        args = self.cmd.parse_args(f"-q create -o {self.outf} -u -p gurn=goob -a bob -L 3d -p lifetime=3m -a alice,carol -p goober=peas -p client_id=yaya oarop pipe".split())
        self.assertTrue(not os.path.exists(self.outf))
        create.execute(args, self.cfg)
        self.assertTrue(os.path.isfile(self.outf))

        with open(self.outf) as fd:
            token = fd.read().strip()
        self.assertTrue(token)

        info = jwt.decode(token, self.cfg['secret'], "HS256")
        self.assertEqual(info["sub"], "oarop")
        self.assertEqual(info["client_id"], "pipe")
        self.assertEqual(info["actortype"], "user")
        self.assertLess(info["exp"] - time.time(), 3600*24*3)
        self.assertGreater(info["exp"] - time.time(), 3600*24*3-60)
        self.assertEqual(info["agents"], "bob alice carol")
        self.assertEqual(info["goober"], "peas")
        self.assertEqual(info["gurn"], "goob")
        self.assertNotIn("lifetime", info)
        self.assertEqual(len(info), 7)
        
        

        

if __name__ == '__main__':
    test.main()

