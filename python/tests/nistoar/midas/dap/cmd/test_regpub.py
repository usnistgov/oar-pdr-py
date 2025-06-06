"""
test regpub subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.midas.dap.cmd import regpub
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.nsd import service as nsd

tmpdir = tempfile.TemporaryDirectory(prefix="_test_regpub.")
testdir = Path(__file__).parents[0]
nsddatadir = testdir.parents[2] / "nsd" / "data"

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_regpub.log"))
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

class TestRegpubCmd(test.TestCase):

    def setUp(self):
        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(regpub)

    def tearDown(self):
        os.environ['LOGNAME'] = os.getlogin()
        dapdir = os.path.join(tmpdir.name, "dbfiles")
        if os.path.isdir(dapdir):
            shutil.rmtree(dapdir)

    def test_parse(self):
        args = self.cmd.parse_args("-q regpub -o ava1 -r public -w rlp3 grg2 -A mds2-88888".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "regpub")
        self.assertTrue(args.as_id)
        self.assertEqual(args.nerdref, "mds2-88888")
        self.assertEqual(args.owner, "ava1")
        self.assertEqual(args.rp, ["public"])
        self.assertEqual(args.wp, ["rlp3", "grg2"])
        self.assertEqual(args.ap, [])
        self.assertFalse(args.overwrite)

    @test.skipIf(not os.environ.get('OAR_PDR_RESOLVER_URL'), "skipping test involving resolver")
    def test_resolve_id(self):
        cfg = { "resolver_url": os.environ.get('OAR_PDR_RESOLVER_URL') }
        self.assertTrue(cfg['resolver_url'])
        args = self.cmd.parse_args("-q regpub mds2-88888".split())

        nerd = regpub.resolve_id("pdr0-0001", args, cfg)
        self.assertTrue(nerd)
        self.assertIn("@type", nerd)
        self.assertEqual(nerd["@id"], "ark:/88434/pdr0-0001")

    def test_get_agent(self):
        cfg = {}
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        agent = regpub.get_agent(args, cfg)
        self.assertEqual(agent.vehicle, "midasadm")
        self.assertEqual(agent.actor, os.getlogin())
        self.assertEqual(agent.actor_type, agent.USER)
        self.assertEqual(agent.groups, (agent.ADMIN,))

        os.environ['LOGNAME'] = "gjc1"
        agent = regpub.get_agent(args, cfg)
        self.assertEqual(agent.vehicle, "midasadm")
        self.assertEqual(agent.actor, "gjc1")
        self.assertEqual(agent.actor_type, agent.USER)
        self.assertEqual(agent.groups, (agent.ADMIN,))

        args = self.cmd.parse_args("-q -A ava1 regpub mds2-88888".split())
        agent = regpub.get_agent(args, cfg)
        self.assertEqual(agent.vehicle, "midasadm")
        self.assertEqual(agent.actor, "ava1")
        self.assertEqual(agent.actor_type, agent.USER)
        self.assertEqual(agent.groups, (agent.ADMIN,))
        
        cfg['auto_users'] = "ava1"
        agent = regpub.get_agent(args, cfg)
        self.assertEqual(agent.vehicle, "midasadm")
        self.assertEqual(agent.actor, "ava1")
        self.assertEqual(agent.actor_type, agent.AUTO)
        self.assertEqual(agent.groups, (agent.ADMIN,))

    def test_create_DAPService(self):
        self.assertTrue(nsddatadir.is_dir())
        cfg = {
            'dbio': {
                "factory": "inmem",
                "people_service": {
                    "factory": "files",
                    "dir": str(nsddatadir)
                }
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        who = regpub.get_agent(args, cfg)
        svc = regpub.create_DAPService(who, args, cfg, log)

        self.assertTrue(svc)
        self.assertTrue(isinstance(svc._store, InMemoryResourceStorage))
        self.assertTrue(isinstance(svc.dbcli, InMemoryDBClient))
        self.assertTrue(svc.dbcli.people_service)
        
        self.assertFalse(svc.exists("mds2:88888"))

        cfg['dbio']['factory'] = "inmen"   # typo
        with self.assertRaises(cli.CommandFailure):
            regpub.create_DAPService(who, args, cfg, log)

    # @test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
    def test_create_DAPService_mongo(self):
        cfg = {
            'dbio': {
                "factory": "mongo",
                "db_url": os.environ['MONGO_TESTDB_URL']
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        who = regpub.get_agent(args, cfg)
        svc = regpub.create_DAPService(who, args, cfg, log)

        self.assertTrue(svc)
        self.assertTrue(isinstance(svc._store, InMemoryResourceStorage))
        self.assertTrue(isinstance(svc.dbcli, MongoDBClient))
        
    def test_create_DAPService_mongo_default(self):
        cfg = {
            'dbio': {
                "factory": "mongo",
                "user": "gurn"
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        who = regpub.get_agent(args, cfg)
        svc = regpub.create_DAPService(who, args, cfg, log)

        self.assertTrue(svc)
        self.assertTrue(isinstance(svc._store, InMemoryResourceStorage))
        self.assertTrue(isinstance(svc.dbcli, MongoDBClient))
        self.assertEqual(svc.dbcli._dburl, "mongodb://gurn:gurn@localhost:27017/midas")
        
    def test_create_DAPService_fsbased(self):
        cfg = {
            'working_dir': tmpdir.name,
            'dbio': {
                "factory": "fsbased"
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        who = regpub.get_agent(args, cfg)
        svc = regpub.create_DAPService(who, args, cfg, log)

        self.assertTrue(svc)
        self.assertTrue(isinstance(svc._store, InMemoryResourceStorage))
        self.assertTrue(isinstance(svc.dbcli, FSBasedDBClient))
        self.assertEqual(str(svc.dbcli._root), os.path.join(tmpdir.name, "dbfiles"))

    @test.skipIf(not os.environ.get('OAR_PDR_RESOLVER_URL'), "skipping test involving resolver")
    def test_execute_notfound(self):
        cfg = {
            'dbio': {
                "factory": "inmem"
            },
            'doi_naan': '10.88888',
            'resolver_url': os.environ['OAR_PDR_RESOLVER_URL']
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        
        try:
            self.cmd.execute(args, cfg)
            self.fail("fatal error did not occur")
        except cli.CommandFailure as ex:
            self.assertEqual(ex.stat, 1)
    
    @test.skipIf(not os.environ.get('OAR_PDR_RESOLVER_URL'), "skipping test involving resolver")
    def test_execute(self):
        root = os.path.join(tmpdir.name, "dbfiles")
        cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": root
            },
            'doi_naan': '10.88888',
            'resolver_url': os.environ['OAR_PDR_RESOLVER_URL']
        }
        log = logging.getLogger()
        args = self.cmd.parse_args("-q regpub -o ava1 mds2-2419".split())
        
        self.cmd.execute(args, cfg)

        dbfile = Path(root) / 'dap' / 'mds2:2419.json'
        self.assertTrue(dbfile.is_file())

        data = read_json(dbfile)
        self.assertEqual(data['id'], 'mds2:2419')
        self.assertEqual(data['owner'], 'ava1')
        self.assertIn('ava1', data['acls']['admin'])
        self.assertIn('ava1', data['acls']['read'])
        self.assertEqual(len(data['acls']['read']), 1)

        # test overwrite protection
        with self.assertRaises(cli.CommandFailure):
            self.cmd.execute(args, cfg)

        # now overwrite
        args = self.cmd.parse_args("-q regpub -o gjc1 -W mds2-2419".split())
        self.cmd.execute(args, cfg)
        self.assertTrue(dbfile.is_file())

        data = read_json(dbfile)
        self.assertEqual(data['id'], 'mds2:2419')
        self.assertEqual(data['owner'], 'gjc1')
        self.assertIn('gjc1', data['acls']['admin'])
        self.assertIn('gjc1', data['acls']['read'])
        self.assertEqual(len(data['acls']['read']), 1)

    @test.skipIf(not os.environ.get('OAR_PDR_RESOLVER_URL'), "skipping test involving resolver")
    def test_execute_file_perms(self):
        root = os.path.join(tmpdir.name, "dbfiles")
        cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": root
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()

        nerdf = os.path.join(tmpdir.name, "nerdm.json")
        args = self.cmd.parse_args(f"-q regpub -o ava1 -w gjc1 goob -a goob -P {nerdf}".split())
        nerd = regpub.resolve_id("mds2-2419", args, cfg, os.environ['OAR_PDR_RESOLVER_URL'])
        write_json(nerd, nerdf)
        
        self.cmd.execute(args, cfg)

        dbfile = Path(root) / 'dap' / 'mds2:2419.json'
        self.assertTrue(dbfile.is_file())

        data = read_json(dbfile)
        self.assertEqual(data['id'], 'mds2:2419')
        self.assertEqual(data['owner'], 'ava1')
        self.assertIn('ava1', data['acls']['admin'])
        self.assertIn('goob', data['acls']['admin'])
        self.assertEqual(len(data['acls']['admin']), 2)
        self.assertIn('ava1', data['acls']['write'])
        self.assertIn('gjc1', data['acls']['write'])
        self.assertIn('goob', data['acls']['write'])
        self.assertEqual(len(data['acls']['write']), 3)
        self.assertIn('ava1', data['acls']['read'])
        self.assertIn('grp0:public', data['acls']['read'])
        self.assertEqual(len(data['acls']['read']), 2)

    def test_owner_from_contact_point(self):
        cfg = { "factory": "files", "dir": str(nsddatadir) }
        ps = nsd.create_people_service(cfg)
        self.assertTrue(ps)
        
        args = self.cmd.parse_args("-q regpub mds2-88888".split())
        log = logging.getLogger()

        cp = {
            "fn": "Phil Proctor",
            "hasEmail": "mailto:phillip.proctor@nist.gov"
        }
        self.assertEqual(regpub.owner_from_contact_point(cp, ps, args, log), "pgp1")

        del cp["hasEmail"]
        self.assertEqual(regpub.owner_from_contact_point(cp, ps, args, log), "pgp1")
    
        cp = {
            "fn": "Austin",
            "hasEmail": "mailto:phillip.proctor@nist.gov"
        }
        self.assertEqual(regpub.owner_from_contact_point(cp, ps, args, log), "pgp1")

        with self.assertRaises(cli.CommandFailure):
            regpub.owner_from_contact_point({"fn": "Madonna"}, ps, args, log)

        cp = {
            "fn": "Ossman, David",
            "hasEmail": "mailto:os@gmail.gov",
            "orcid": "0000-9999-0000-0000"
        }
        self.assertEqual(regpub.owner_from_contact_point(cp, ps, args, log), "do1")

    @test.skipIf(not os.environ.get('OAR_PDR_RESOLVER_URL'), "skipping test involving resolver")
    def test_execute_lookup_owner(self):
        root = os.path.join(tmpdir.name, "dbfiles")
        cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": root,
                "people_service": { "factory": "files", "dir": str(nsddatadir) }
            },
            'doi_naan': '10.88888'
        }
        log = logging.getLogger()

        nerdf = os.path.join(tmpdir.name, "nerdm.json")
        args = self.cmd.parse_args(f"-q regpub -w gjc1 goob -a goob -P {nerdf}".split())
        nerd = regpub.resolve_id("mds2-2419", args, cfg, os.environ['OAR_PDR_RESOLVER_URL'])
        nerd['contactPoint']['hasEmail'] = "mailto:peter.bergman@nist.gov"
        write_json(nerd, nerdf)
        
        self.cmd.execute(args, cfg)

        dbfile = Path(root) / 'dap' / 'mds2:2419.json'
        self.assertTrue(dbfile.is_file())

        data = read_json(dbfile)
        self.assertEqual(data['id'], 'mds2:2419')
        self.assertEqual(data['owner'], 'ppb1')
        self.assertIn('ppb1', data['acls']['admin'])
        self.assertIn('goob', data['acls']['admin'])
        self.assertEqual(len(data['acls']['admin']), 2)
        self.assertIn('ppb1', data['acls']['write'])
        self.assertIn('gjc1', data['acls']['write'])
        self.assertIn('goob', data['acls']['write'])
        self.assertEqual(len(data['acls']['write']), 3)
        self.assertIn('ppb1', data['acls']['read'])
        self.assertIn('grp0:public', data['acls']['read'])
        self.assertEqual(len(data['acls']['read']), 2)




        

if __name__ == '__main__':
    test.main()

