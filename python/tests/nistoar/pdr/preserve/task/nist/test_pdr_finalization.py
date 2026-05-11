import os, json, pdb, logging, tempfile, zipfile, shutil, time
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.preserve.task import framework as fw
from nistoar.base import config
from nistoar.pdr.distrib import DistribServiceException
from nistoar.pdr.preserve.bagit import BagBuilder
from nistoar.pdr.utils import read_nerd, write_json

pdrdir = Path(__file__).resolve().parents[3] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]
assert storedir.is_dir()

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
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir, "simsrv"+str(srvport)+".pid"))
                                                 
    os.system(cmd)
    time.sleep(1)

tmpdir = tempfile.TemporaryDirectory(prefix="_test_finalize.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_preserveq.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    assert startService()

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
            'distrib_service': {
                'service_endpoint': baseurl
            },
            'store_dir': storedir,
            'restricted_store_dir': self.restricted.name
            # no metadata service support, yet.
        }
        self.ra = pdr.RepositoryAccess(self.cfg)

    def tearDown(self):
        self.restricted.cleanup()

    def ctor(self):
        self.assertTrue(self.ra.distrib)
        self.assertTrue(self.log)

    def test_latest_headbag(self):
        hb = self.ra.latest_headbag("goober1a")
        self.assertIsNone(hb)

        hb = self.ra.latest_headbag("mds2-7223", False)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")
        
        hb = self.ra.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")

        shutil.copyfile(storedir/"mds2-7223.1_1_0.mbag0_4-1.zip",
                        Path(self.restricted.name)/"mds2-7223.1_1_1.mbag0_4-2.zip")
        hb = self.ra.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_1.mbag0_4-2.zip")

        hb = self.ra.latest_headbag("mds2-7223", False)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")
        
    def test_latest_headbag_nodistrib(self):
        del self.cfg['distrib_service']
        self.ra = pdr.RepositoryAccess(self.cfg)

        hb = self.ra.latest_headbag("mds2-7223", True)
        self.assertEqual(hb, "mds2-7223.1_1_0.mbag0_4-1.zip")

        with self.assertRaises(DistribServiceException):
            self.ra.latest_headbag("mds2-7223", False)

        
class TestPDRBagFinalization(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="workdir.", dir=tmpdir.name)
        self.workdir = Path(self.tmpdir.name)
        self.restricted = self.workdir/"restricted"
        self.restricted.mkdir()
        self.ingestdir = self.workdir/"ingest"
        self.ingestdir.mkdir()
        self.dcdir = self.workdir/"doimint"
        self.dcdir.mkdir()
        self.cfg = {
            'repo_access': {
                'distrib_service': {
                    'service_endpoint': baseurl
                },
                "store_dir": storedir,
                "restricted_store_dir": self.restricted
            },
            'ingest': {
                'rmm': {
                    'data_dir': self.ingestdir,
                    'service_endpoint': 'https://pdr.nist.gov:8888/'
                },
                'doi': {
                    'data_dir': self.dcdir,
                    'minting_naan': '10.88888',
                    'datacite_api': {
                        'service_endpoint': 'https://goob.datacite.org/dois',
                        'user': "gurn",
                        'pass': "cranston"
                    }
                }
            }
        }

        self.testbag = self.workdir/"mds2-7223"
        with zipfile.ZipFile(storedir/"mds2-7223.1_1_0.mbag0_4-1.zip") as zip:
            zip.extractall(self.workdir)
            (self.workdir/"mds2-7223.1_1_0.mbag0_4-1").rename(self.testbag)
        nerd = read_nerd(self.testbag/"metadata"/"nerdm.json")
        nerd['doi'] = "doi:10.88888/mds2-7223"
        write_json(nerd, self.testbag/"metadata"/"nerdm.json")

        self.fin = pdr.PDRBagFinalization(self.cfg)
        self.mgr = st.JSONPreservationStateManager({"working_dir": str(self.workdir)}, "mds2-7223", 
                                                   str(self.testbag), persistin=Path(self.workdir))


    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.fin.cfg, self.cfg)
        self.assertEqual(self.mgr.aipid, "mds2-7223")
        self.assertTrue(self.fin._ingester)
        self.assertTrue(self.fin._doiminter)

    def test_null_revert(self):
        self.fin.revert(self.mgr)
        self.fin.revert(self.mgr)

    def test_apply(self):
        self.assertIsNone(self.mgr.get_finalized_aip())
        aipid = "mds2-7223"
        for dir in self.ingestdir.iterdir():
            if dir.is_dir():
                self.assertFalse((dir/"mds2-7223.json").exists())

        with self.assertRaises(fw.AIPFinalizationException):
            self.fin.apply(self.mgr)

        self.fin.cfg['allow_replace'] = True
        self.fin.apply(self.mgr)

        self.assertEqual(self.mgr.get_finalized_aip(), str(self.workdir/("mds2-7223.1_1_0.mbag0_4-2")))
        self.assertTrue(self.ingestdir/"staging"/"mds2-7223.json")
        self.assertTrue(self.dcdir/"staging"/"mds2-7223.json")
        self.assertEqual(self.mgr.steps_completed, self.mgr.FINALIZED)
        self.assertEqual(self.mgr.get_state_property("nerdm:version"), "1.1.0")
        
    def test_run(self):
        bgb = BagBuilder.forBag(self.mgr.get_sip())
        bgb.update_annotations_for('', {'version': '2.0.0.0'})
        bgb.done()

        self.assertIsNone(self.mgr.get_finalized_aip())
        self.fin.run(self.mgr, False)

        self.assertEqual(self.mgr.get_finalized_aip(), str(self.workdir/("mds2-7223.2_0_0_0.mbag0_4-2")))
        self.assertTrue((self.ingestdir/"staging"/"mds2-7223.json").is_file())
        self.assertTrue((self.dcdir/"staging"/"mds2-7223.json").is_file())
        
        self.fin.run(self.mgr, False)
        self.fin.revert(self.mgr)
        self.assertTrue(not (self.ingestdir/"staging"/"mds2-7223.json").exists())
        self.assertTrue(not (self.dcdir/"staging"/"mds2-7223.json").exists())
        self.assertIsNone(self.mgr.get_finalized_aip())
        
        


if __name__ == '__main__':
    test.main()
