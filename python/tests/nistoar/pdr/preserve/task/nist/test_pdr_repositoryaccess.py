import os, json, pdb, logging, tempfile, zipfile, shutil, time
from pathlib import Path
import unittest as test

from nistoar.testing import *
from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.distrib import DistribServiceException
from nistoar.base import config

pdrdir = Path(__file__).resolve().parents[3] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]

tmpdir = tempfile.TemporaryDirectory(prefix="_test_repoaccess.")
port = 9091
baseurl = "http://localhost:{0}/".format(port)

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    tdir = tmpdir.name
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    status = os.system(cmd) == 0
    time.sleep(0.5)
    return status

def stopService(authmeth=None):
    tdir = tmpdir.name
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

testbag = Path(tmpdir.name) / "mds2-7223.1_0_0.mbag0_4-0"
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_state.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

    with zipfile.ZipFile(storedir/"mds2-7223.1_0_0.mbag0_4-0.zip") as zip:
        zip.extractall(os.path.join(tmpdir.name))
    # (Path(tmpdir.name)/"mds2-7223.1_0_0.mbag0_4-0").rename(testbag)
    startService()

def tearDownModule():
    stopService()
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestRepositoryAccess(test.TestCase):

    def setUp(self):
        self.restricted = tempfile.TemporaryDirectory(prefix="restricted.", dir=tmpdir.name)
        self.cfg = {
            "distrib_service": {
                "service_endpoint": "http://localhost:9091"
            },
            'store_dir': storedir,
            'restricted_store_dir': self.restricted.name
        }
        self.repo = pdr.RepositoryAccess(self.cfg)

    def tearDown(self):
        self.restricted.cleanup()
    
    def ctor(self):
        self.assertTrue(self.repo.distrib)
        self.assertTrue(self.log)

    def test_latest_headbag(self):
        hb = self.repo.latest_headbag("goober1a")
        self.assertIsNone(hb)

        hb = self.repo.latest_headbag("mds2-7223", False)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")
        
        hb = self.repo.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")

        shutil.copyfile(storedir/"mds2-7223.1_1_0.mbag0_4-1.zip",
                        Path(self.restricted.name)/"mds2-7223.1_1_1.mbag0_4-2.zip")
        hb = self.repo.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_1.mbag0_4-2.zip")

        hb = self.repo.latest_headbag("mds2-7223", False)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")
        
    def test_latest_headbag_nodistrib(self):
        del self.cfg['distrib_service']
        self.repo = pdr.RepositoryAccess(self.cfg)

        hb = self.repo.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")

        with self.assertRaises(DistribServiceException):
            self.repo.latest_headbag("mds2-7223", False)

    def test_available_aips_for(self):
        aips = self.repo.available_aips_for("pdr2210", "1.0")
        self.assertEqual(len(aips), 2)
        self.assertEqual(aips[0]['name'], "pdr2210.1_0.mbag0_3-0.zip")
        self.assertEqual(aips[1]['name'], "pdr2210.1_0.mbag0_3-1.zip")

        aips = self.repo.available_aips_for("mds3-goob", "1.1.0")
        self.assertEqual(len(aips), 0)

    def test_aip_available(self):
        self.assertTrue(self.repo.aip_available("pdr2210.1_0.mbag0_3-1.zip"))


        


if __name__ == '__main__':
    test.main()
    

