"""
test show subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

import jwt

from nistoar.pdr.utils import cli
from nistoar.midas.cli.jwt import show, create

tmpdir = tempfile.TemporaryDirectory(prefix="_test_jwt_show.")
testdir = Path(__file__).parents[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_jwt_show.log"))
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
        self.cmd.load_subcommand(show)
        self.cfg = { "secret": "goober and the peas" }
        self.outf = os.path.join(tmpdir.name, "claims.dat")
        self.tok = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY3RvcnR5cGUiOiJhdXRvIiwiY2xpZW50X2lkIjoicGlwZSIsImFnZW50cyI6ImJvYiBhbGljZSBjYXJvbCIsImdvb2JlciI6InBlYXMiLCJzdWIiOiJvYXJvcCIsImV4cCI6MTg0NjQ3MTM2NX0.QZHtVsPPMMcsiqgvXcK_52jI3xOHjx1KDhyAPWAY7Dc"

    def tearDown(self):
        if os.path.isfile(self.outf):
            os.remove(self.outf)

    def test_parse(self):
        args = self.cmd.parse_args(f"-q show {self.tok}".split())
        self.assertEqual(args.token, self.tok)
        self.assertEqual(args.fmt, "text")
        self.assertIsNone(args.outfile)
        self.assertIsNone(args.infile)
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "show")
        
        args = self.cmd.parse_args(f"-q show -j -f - -o data.json".split())
        self.assertEqual(args.infile, "-")
        self.assertEqual(args.outfile, "data.json")
        self.assertEqual(args.fmt, "json")
        self.assertIsNone(args.token)
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "show")
        
    def test_write_claimset(self):
        data = { "sub": "you", "goob": "gurn", "client_id": "repo" }
        with open(self.outf, 'w') as fd:
            show.write_claimset(data, fd)

        lines = []
        with open(self.outf) as fd:
            for line in fd:
                lines.append(line)

        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "Subject: you\n")
        self.assertEqual(lines[1], "Expiration: never\n")
        self.assertEqual(lines[2], "  goob: gurn\n")
        self.assertEqual(lines[3], "  client_id: repo\n")

    def test_write_claimset_with_exp(self):
        data = { "sub": "you", "goob": "gurn", "client_id": "repo", "exp": 1846471365 }
        with open(self.outf, 'w') as fd:
            show.write_claimset(data, fd)

        lines = []
        with open(self.outf) as fd:
            for line in fd:
                lines.append(line)

        self.assertEqual(len(lines), 5)
        self.assertEqual(lines[0], "Subject: you\n")
        self.assertEqual(lines[1], "Expiration: 2028-07-06T00:42:45\n")
        self.assertEqual(lines[2], "  goob: gurn\n")
        self.assertEqual(lines[3], "  client_id: repo\n")

    def test_execute_json_output(self):
        args = self.cmd.parse_args(f"-q show -j -o {self.outf} {self.tok}".split())
        self.assertTrue(not os.path.exists(self.outf))
        show.execute(args, self.cfg)
        self.assertTrue(os.path.isfile(self.outf))

        with open(self.outf) as fd:
            data = json.load(fd)

        self.assertEqual(data['sub'], "oarop")
        self.assertEqual(data['actortype'], "auto")
        self.assertEqual(data['client_id'], "pipe")
        self.assertEqual(data['agents'], "bob alice carol")
        self.assertEqual(data['goober'], "peas")
        self.assertTrue(data['exp'])
        self.assertTrue(isinstance(data['exp'], (float, int)))
        self.assertGreater(data['exp'], 100000)
        self.assertEqual(len(data), 6)

    def test_execute_select_json(self):
        args = self.cmd.parse_args(f"-q show -j -p actortype -o {self.outf} {self.tok}".split())
        self.assertTrue(not os.path.exists(self.outf))
        show.execute(args, self.cfg)
        self.assertTrue(os.path.isfile(self.outf))

        with open(self.outf) as fd:
            data = json.load(fd)

        self.assertEqual(data['actortype'], "auto")
        self.assertEqual(len(data), 1)

    def test_execute_select_none(self):
        args = self.cmd.parse_args(f"-q show -j -p purpose -o {self.outf} {self.tok}".split())
        self.assertTrue(not os.path.exists(self.outf))
        show.execute(args, self.cfg)
        self.assertTrue(os.path.isfile(self.outf))

        with open(self.outf) as fd:
            data = json.load(fd)

        self.assertIsNone(data['purpose'])
        self.assertEqual(len(data), 1)
        
        

        

if __name__ == '__main__':
    test.main()

