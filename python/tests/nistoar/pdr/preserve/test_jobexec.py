import sys, os, json, pdb, logging, tempfile, zipfile, shutil, time, re
from pathlib import Path
import unittest as test

# prevent attempting to import "bagit" from test's subdirectory
execdir = Path(__file__).resolve().parent
if str(execdir) in sys.path:
    sys.path.remove(str(execdir))

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.preserve.task import framework as fw
from nistoar.pdr.preserve.task.state import JSONPreservationStateManager
from nistoar.base import config
from nistoar.pdr.distrib import DistribServiceException
from nistoar.pdr.preserve.bagit import BagBuilder
from nistoar.pdr.utils import read_nerd, write_json
from nistoar.pdr.preserve import jobexec

pdrdir = execdir.parents[0]
datadir = pdrdir / "preserve" / "data"
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]
ormdir = basedir / "metadata"
assert storedir.is_dir()

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
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simdistsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    os.system(cmd)

    srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    wpy = "python/tests/nistoar/pdr/ingest/rmm/sim_ingest_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --set-ph auth_key=critic --set-ph auth_meth=header --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simingsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    os.system(cmd)

    srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    mocksvr = ormdir / "python" / "tests" / "nistoar" / "doi" / "sim_datacite_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4} --set-ph prefixes={5}"
    cmd = cmd.format(os.path.join(tdir,"simsdcrv.log"), uwsgi_opts, srvport, mocksvr,
                     pidfile, ",".join(prefixes))
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
    stopServices()
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

from nistoar.pdr.constants import ARK_PFX_PAT
ARK_PFX_RE = re.compile(ARK_PFX_PAT)

class TestPreservationTask(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.workdir = self.tempdir.name
        self.statedir   = os.path.join(self.workdir, "pstate")
        self.stagedir   = os.path.join(self.workdir, "stage")
        self.storedir   = os.path.join(self.workdir, "store")
        self.restricted = os.path.join(self.workdir, "restricted")
        self.ingestdir  = os.path.join(self.workdir, "ingest")
        self.dcdir      = os.path.join(self.workdir, "doimint")
        for d in (self.statedir, self.stagedir, self.storedir, self.restricted,
                  self.ingestdir, self.dcdir):
            if not os.path.exists(d):
                os.mkdir(d)

        self.smcfg = {
            "sip_dir":     self.workdir,
            "working_dir": self.workdir,
            "stage_dir":   self.stagedir,
            "persist_in":  self.statedir
        }

        self.config = {
            "task": {
                "store_dir": self.storedir,
                'restricted_store_dir': self.restricted,
                'repo_access': {
                    'distrib_service': {
                        'service_endpoint': "http://localhost:9991/"
                    }
                },
                'ingest': {
                    'rmm': {
                        'data_dir': self.ingestdir,
                        'service_endpoint': 'http://localhost:9992/nerdm/',
                        'auth_key': 'critic',
                        'auth_method': 'header'
                    },
                    'doi': {
                        'data_dir': self.dcdir,
                        'minting_naan': '10.88434',
                        'datacite_api': {
                            'service_endpoint': 'http://localhost:9993/dois/',
                            'user': "gurn",
                            'pass': "cranston"
                        }
                    }
                },

                "finalize": {
                },
                "validate": {
                    "check_data_files": False,
                },
                "serialize": {
                    "multibag": {
                        "validate": True
                    }
                },
                "archive": {
                    "polling": {
                        "cycle_time": 5,
                        "wait_for_completion": False
                    }
                },
                "publish": {
                    'allow_replace': True
                },
                "cleanup": {
                }
            }
        }

        # prep SIP bag
        srcbag = datadir / "mds3sipbag"
        nerd = read_nerd(srcbag/"metadata"/"nerdm.json")
        self.aipid = ARK_PFX_RE.sub('', nerd["@id"])
        destdir = self.workdir
        bagdir = os.path.join(destdir, self.aipid)
        shutil.copytree(srcbag, bagdir)

        self.log = logging.getLogger('preserve').getChild(self.aipid)
        JSONPreservationStateManager.for_aip(self.smcfg, self.aipid, bagdir, self.log)
        self.statefile = os.path.join(self.statedir, f"{self.aipid}_state.json")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_make_preservation_task(self):
        self.assertTrue(os.path.isfile(self.statefile))

        task = jobexec.make_preservation_task(self.statefile, self.config, self.log)
        self.assertFalse(task.started)
        self.assertFalse(task.completed)

    def test_process(self):
        jobexec.process(self.aipid, self.config, [self.statefile], self.log)

        self.assertTrue(os.path.isfile(self.statefile))
        mgr = JSONPreservationStateManager.from_file(self.statefile)
        
        self.assertTrue(mgr.all_completed)
        self.assertIsNone(mgr.get_finalized_aip())
        self.assertFalse(os.path.exists(mgr.get_sip()))
        self.assertEqual(len(mgr.get_serialized_files()), 1)
        staged = list(os.listdir(self.stagedir))
        self.assertEqual(len(staged), 1)
        self.assertEqual(staged[0], f"{self.aipid}.1_0_0.mbag0_4-0.zip")
        mbdir = os.path.join(self.workdir, "multibag")
        self.assertFalse(os.path.exists(mbdir))

                         


if __name__ == '__main__':
    test.main()

