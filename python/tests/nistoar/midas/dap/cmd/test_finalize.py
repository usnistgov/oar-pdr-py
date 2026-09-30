"""
test finalize subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import finalize, create_DAPService, get_agent
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.midas.dbio import status
from nistoar.pdr.utils.cli import CommandFailure, explain

tmpdir = tempfile.TemporaryDirectory(prefix="_test_finalize.")
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
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_finalize.log"))
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

class TestFinalizeCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(finalize)

    def tearDown(self):
        os.environ['LOGNAME'] = getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q finalize -m Shazam! mds2-88888".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "finalize")
        self.assertEqual(args.message, "Shazam!")

    def test_execute(self):
        root = os.path.join(tmpdir.name, "dbfiles")
        cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": root,
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "mds3"
                    }
                }
            },
            'nerdstorage': {
                "type": 'fsbased',
                "store_dir": os.path.join(tmpdir.name, "nerdm"),
                "default_shoulder": "mds3"
            },
            'doi_naan': '10.88888',
            'resolver_url': "https://example.com/resolve"  # os.environ['OAR_PDR_RESOLVER_URL']
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q finalize -m finely-finalized mds3:0001".split())

        who = get_agent(args, cfg)
        svc = create_DAPService(who, args, cfg, logging.getLogger())
        rec = svc.create_record("goob")
        self.assertEqual(rec.status.state, status.EDIT)

        with self.assertRaises(CommandFailure):
            # because the record is insufficiently complete
            self.cmd.execute(args, cfg)

        svc.update_data(rec.id, {"title": "Shazam!: the Movie",
                                 "description":  "meh",
                                 "topic": [{
                                     "scheme": "https://data.nist.gov/od/dm/nist-themes/v2.0",
                                     "tag": "Information Technolog"
                                 }] })

        try:
            self.cmd.execute(args, cfg)
        except CommandFailure as ex:
            raise
        except Exception as ex:
            raise
        dbfile = Path(root) / 'dap' / 'mds3:0001.json'
        self.assertTrue(dbfile.is_file())

        data = read_json(dbfile)
        self.assertEqual(data["status"]['state'], status.READY)
        self.assertEqual(data["status"]['message'], "finely-finalized")

        md = svc.get_nerdm_data(rec.id)
        self.assertEqual(md.get('accessLevel'), 'public')



        

if __name__ == '__main__':
    test.main()



