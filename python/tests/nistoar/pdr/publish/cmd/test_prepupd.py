"""
test subcommand prepudp
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile, re
import unittest as test
from pathlib import Path

from nistoar.pdr.utils import cli, read_json, write_json
from nistoar.pdr.publish.cmd import prepupd
from nistoar.base import config as cfgmod
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.public.sim import repo

tmpdir = tempfile.TemporaryDirectory(prefix="_test_publish.")
testdir = Path(__file__).parents[0]
pdrdir = Path(__file__).resolve().parents[2]
datadir = pdrdir / "preserve" / "data"
rmmdir = pdrdir / "describe" / "data" / "rmm-test-archive"
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]
ormdir = basedir / "metadata"
assert storedir.is_dir()

uwsgiscript = repo.__file__
port = 9991
prefixes = ["10.88434", "10.88888"]
arkpre = re.compile(r'^ark:/\d+/')

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startServices(authmeth=None):
    tdir = tmpdir.name
    srvport = port
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} --set-ph readonly=False " \
          "--wsgi-file {3} --pidfile {4} --set-ph bagdir={5} --set-ph archive_dir={6} " \
          "--set-ph baseurl=http://localhost:{2}/"
    cmd = cmd.format(os.path.join(tdir,"simreposrv.log"), uwsgi_opts, srvport,
                     uwsgiscript, pidfile, storedir, rmmdir)
    os.system(cmd)

def stopServices():
    tdir = tmpdir.name
    srvport = port

    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    if os.path.exists(pidfile):
        cmd = "uwsgi --stop {0}".format(pidfile)
        # print(cmd)
        os.system(cmd)

    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_publish.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)
    startServices()
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    stopServices()
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestPrepupdCmd(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.workdir = Path(self.tempdir.name)
        self.cachedir = self.workdir/'headbags'
        if not self.cachedir.exists():
            self.cachedir.mkdir()

        self.cfg = {
            'sip_bags_dir': self.workdir,
            'repo_access': {
                'distrib_service': {
                    'service_endpoint': "http://localhost:9991/ds/"
                },
                'metadata_service': {
                    'service_endpoint': "http://localhost:9991/"
                },
                "store_dir": str(storedir),
                'restricted_store_dir': str(storedir),
                'headbag_cache': str(self.cachedir)
            }
        }

        self.cmd = cli.CLISuite("midasadm")
        self.cmd.load_subcommand(prepupd)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_parse(self):
        args = self.cmd.parse_args("-q prepupd -b here -A mds2-8888".split())
        self.assertEqual(args.id, "mds2-8888")
        self.assertTrue(args.is_aipid)
        self.assertTrue(not args.is_pdrid)
        self.assertEqual(args.outdir, "here")
        self.assertIsNone(args.cachedir)
        self.assertIsNone(args.mdsvcurl)
        self.assertIsNone(args.distsvcurl)

        args = self.cmd.parse_args("-q prepupd -d here -M https://mockrepo/m -D https://mockrepo/d".split() +
                                   "--cache-dir=/tmp ark:/88888/mds2-8888".split())
        self.assertEqual(args.id, "ark:/88888/mds2-8888")
        self.assertTrue(not args.is_aipid)
        self.assertTrue(not args.is_pdrid)
        self.assertEqual(args.outdir, "here")
        self.assertEqual(args.cachedir, "/tmp")
        self.assertEqual(args.mdsvcurl, "https://mockrepo/m")
        self.assertEqual(args.distsvcurl, "https://mockrepo/d")

    def test_process_options(self):
        rcfg = self.cfg['repo_access']
        
        args = self.cmd.parse_args("-q prepupd -b here -P -A mds2-8888".split())
        with self.assertRaises(CommandFailure):
            prepupd.process_options(args, rcfg)

        self.assertEqual(rcfg['store_dir'], str(storedir))
        self.assertEqual(rcfg['metadata_service']['service_endpoint'], "http://localhost:9991/")
        self.assertEqual(rcfg['distrib_service']['service_endpoint'], "http://localhost:9991/ds/")

        args = self.cmd.parse_args("-q prepupd -M https://mockrepo/m -D https://mockrepo/d".split() +
                                   "--cache-dir=/tmp ark:/88888/mds2-8888".split())
        prepupd.process_options(args, self.cfg)
        self.assertEqual(rcfg['headbag_cache'], "/tmp")
        self.assertEqual(rcfg['metadata_service']['service_endpoint'], "https://mockrepo/m")
        self.assertEqual(rcfg['distrib_service']['service_endpoint'], "https://mockrepo/d")
        self.assertEqual(str(args.outdir), str(self.workdir))

    def test_aipid(self):
        obag = self.workdir/"mds2-7223"
        self.assertTrue(not obag.exists())
        
        args = self.cmd.parse_args("-q prepupd -A mds2-7223".split())
        self.cmd.execute(args, self.cfg)
        self.assertTrue(obag.is_dir())
        self.assertTrue((obag/"metadata").is_dir())

    def test_pdrid(self):
        obag = self.workdir/"mds2-7223"
        self.assertTrue(not obag.exists())
        
        args = self.cmd.parse_args("-q prepupd -P ark:/88434/mds2-7223".split())
        self.cmd.execute(args, self.cfg)
        self.assertTrue(obag.is_dir())
        self.assertTrue((obag/"metadata").is_dir())
        

        

if __name__ == '__main__':
    test.main()


        
        
        
        

