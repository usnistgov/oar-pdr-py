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
ormdir = basedir / "metadata"
assert storedir.is_dir()

port = 9091
prefixes = ["10.88434", "10.88888"]

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startServices(authmeth=None):
    tdir = tmpdir.name
    srvport = port
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3} {4}"
    cmd = cmd.format(os.path.join(tdir,"simdistsrv.log"), srvport,
                     os.path.join(basedir, wpy), pidfile, uwsgi_opts)
    os.system(cmd)

    srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    wpy = "python/tests/nistoar/pdr/ingest/rmm/sim_ingest_srv.py"
    cmd = "uwsgi --daemonize {0} --http-socket :{1} " \
          "--wsgi-file {2} --set-ph auth_key=critic --set-ph auth_meth=header --pidfile {3}"
    cmd = cmd.format(os.path.join(tdir,"simingsrv.log"), srvport,
                     os.path.join(basedir, wpy), pidfile, uwsgi_opts)
    os.system(cmd)

    srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    mocksvr = ormdir / "python" / "tests" / "nistoar" / "doi" / "sim_datacite_srv.py"
    cmd = "uwsgi --daemonize {0} --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3} --set-ph prefixes={4}"
    cmd = cmd.format(os.path.join(tdir,"simsdcrv.log"), srvport, mocksvr,
                     pidfile, ",".join(prefixes), uwsgi_opts)
    os.system(cmd)

    time.sleep(0.5)

def stopServices():
    tdir = tmpdir.name
    srvport = port

    for p in range(srvport, srvport+3):
        pidfile = os.path.join(tdir,"simsrv"+str(p)+".pid")
        if os.path.exists(pidfile):
            cmd = "uwsgi --stop {0}".format(pidfile)
            # print(cmd)
            os.system(cmd)

    time.sleep(1)

tmpdir = tempfile.TemporaryDirectory(prefix="_test_publication.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_preserve.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    startServices()
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    stopServices()
    tmpdir.cleanup()

class TestPDRBagFinalization(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="workdir.", dir=tmpdir.name)
        self.workdir = Path(self.tmpdir.name)
        self.ingestdir = self.workdir/"ingest"
        self.ingestdir.mkdir()
        self.dcdir = self.workdir/"doimint"
        self.dcdir.mkdir()
        self.cfg = {
            'repo_access': {
                'distrib_service': {
                    'service_endpoint': 'http://localhost:9091/'
                },
                "store_dir": storedir  # ,
#                "restricted_store_dir": self.restricted
            },
            'allow_replace': True,
            'ingest': {
                'rmm': {
                    'data_dir': self.ingestdir,
                    'service_endpoint': 'http://localhost:9092/nerdm/',
                    'auth_key': 'critic',
                    'auth_method': 'header'
                },
                'doi': {
                    'data_dir': self.dcdir,
                    'minting_naan': '10.88888',
                    'datacite_api': {
                        'service_endpoint': 'http://localhost:9093/dois/',
                        'user': "gurn",
                        'pass': "cranston"
                    }
                }
            }
        }
        self.smcfg = {
            "working_dir": str(self.workdir),
            "persist_in": str(self.workdir)
        }

        self.testbag = self.workdir/"mds2-7223"
        with zipfile.ZipFile(storedir/"mds2-7223.1_1_0.mbag0_4-1.zip") as zip:
            zip.extractall(self.workdir)
            (self.workdir/"mds2-7223.1_1_0.mbag0_4-1").rename(self.testbag)
        nerd = read_nerd(self.testbag/"metadata"/"nerdm.json")
        nerd['doi'] = "doi:10.88888/mds2-7223"
        write_json(nerd, self.testbag/"metadata"/"nerdm.json")

        self.fin = pdr.PDRBagFinalization(self.cfg)
        self.pub = pdr.PDRPublication(self.cfg)
        self.mgr = st.JSONPreservationStateManager(self.smcfg, "mds2-7223", str(self.testbag))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.pub)
        self.assertTrue(self.pub.cfg)
        self.assertEqual(self.mgr.aipid, "mds2-7223")
#        self.assertTrue(self.pub._storer)
        self.assertTrue(self.pub._ingester)
        self.assertTrue(self.pub._doiminter)

    def test_apply(self):
        self.fin.run(self.mgr, False)
        self.assertEqual(self.mgr.steps_completed, self.mgr.FINALIZED)
        self.assertTrue((self.ingestdir/"staging"/"mds2-7223.json").exists())
        self.assertTrue((self.dcdir/"staging"/"mds2-7223.json").exists())

        self.pub.apply(self.mgr)
        self.assertEqual(self.mgr.message, "AIP is released")
        self.assertTrue(not (self.ingestdir/"staging"/"mds2-7223.json").exists())
        self.assertTrue(not (self.dcdir/"staging"/"mds2-7223.json").exists())
        self.assertTrue((self.ingestdir/"succeeded"/"mds2-7223.json").exists())
        self.assertTrue((self.dcdir/"published"/"mds2-7223.json").exists())

    def test_fail_on_incomplete(self):
        self.cfg['ingest']['rmm']['fail_on_incomplete'] = True
        self.cfg['ingest']['rmm']['service_endpoint'] = 'http://localhost:9092/nerdum/'
        self.pub = pdr.PDRPublication(self.cfg)

        with self.assertRaises(fw.AIPPublicationException):
            self.pub.apply(self.mgr)


if __name__ == '__main__':
    test.main()
