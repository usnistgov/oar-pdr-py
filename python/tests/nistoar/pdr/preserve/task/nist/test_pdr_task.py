import os, json, pdb, logging, tempfile, zipfile, shutil, time, re
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.preserve.task import framework as fw
from nistoar.pdr.preserve.task.state import JSONPreservationStateManager
from nistoar.base import config
from nistoar.pdr.distrib import DistribServiceException
from nistoar.pdr.preserve.bagit import BagBuilder
from nistoar.pdr.utils import read_nerd, write_json

pdrdir = Path(__file__).resolve().parents[3]
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
            },
        }

        # prep SIP bag
        srcbag = datadir / "mds3sipbag"
        nerd = read_nerd(srcbag/"metadata"/"nerdm.json")
        self.aipid = ARK_PFX_RE.sub('', nerd["@id"])
        destdir = self.workdir
        bagdir = os.path.join(destdir, self.aipid)
        shutil.copytree(srcbag, bagdir)

        self.sm = JSONPreservationStateManager.for_aip(self.smcfg, self.aipid, bagdir,
                                                       logging.getLogger('preserve').getChild(self.aipid))
        self.factory = pdr.PDRPreservationTaskFactory(self.config)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_factory_ctor(self):
        self.assertIn('repo_access', self.factory.cfg)
        self.assertIn('repo_access', self.factory.cfg['archive'])
        self.assertIn('store_dir', self.factory.cfg['finalize'])
        self.assertIn('restricted_store_dir', self.factory.cfg['finalize'])
        self.assertIn('store_dir', self.factory.cfg['archive'])
        self.assertIn('restricted_store_dir', self.factory.cfg['archive'])

    def test_create_task(self):
        task = self.factory.create_task(self.sm)
        self.assertEqual(task.aipid, self.aipid)

        mgr = task._statemgr
        self.assertTrue(mgr)
        self.assertEqual(mgr.get_sip(), os.path.join(self.workdir, self.aipid))
        self.assertTrue(os.path.isdir(mgr.get_sip()))
        self.assertEqual(mgr.steps_completed, 0)
        self.assertTrue(mgr.log)

    def test_finalize(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        
        task.finalize()
        self.assertTrue(task.finalized())
        self.assertEqual(mgr.completed, "finalized")
        self.assertTrue(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_finalized_aip()))
        self.assertTrue(not os.path.isdir(mgr.get_sip()))

        submitteddir = os.path.join(self.ingestdir, "staging")
        self.assertTrue(os.path.isdir(submitteddir))
        self.assertTrue(os.path.isfile(os.path.join(submitteddir,self.aipid+".json")))
        submitteddir = os.path.join(self.dcdir, "staging")
        self.assertTrue(os.path.isdir(submitteddir))
        self.assertTrue(os.path.isfile(os.path.join(submitteddir,self.aipid+".json")))

    def test_validate(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        
        task.validate(as_is=False)
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertEqual(mgr.completed, "validated")
        self.assertTrue(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_finalized_aip()))
        self.assertTrue(not os.path.isdir(mgr.get_sip()))

    def test_validated_stick(self):
        # turn always_apply off
        self.factory.cfg['validate']['always_apply'] = False
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        
        task.validate(as_is=False)
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())  # because always_apply is set to False
        self.assertEqual(mgr.completed, "validated")
        self.assertTrue(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_finalized_aip()))
        self.assertTrue(not os.path.isdir(mgr.get_sip()))

    def test_validate_as_is(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        
        with self.assertRaises(fw.AIPValidationException):
            task.validate(as_is=True)
        self.assertTrue(not task.finalized())
        self.assertTrue(not task.validated())
        self.assertEqual(mgr.steps_completed, 0)
        self.assertEqual(mgr.completed, "unstarted")
        self.assertIsNone(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_sip()))

    def test_serialize(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        
        task.serialize()
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertTrue(task.serialized())
        mbags = mgr.get_serialized_files()
        self.assertTrue(mbags)
        self.assertEqual(len(mbags), 2)
        self.assertEqual(os.path.basename(mbags[0]),
                         os.path.basename(mgr.get_finalized_aip())+".zip")
        self.assertEqual(mbags[1], mbags[0]+".sha256")
        self.assertTrue(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_finalized_aip()))
        self.assertTrue(not os.path.isdir(mgr.get_sip()))

    def test_serialize_split(self):
        # force multibag splitting
        self.factory.cfg['serialize']['multibag']['max_bag_size'] = 9000
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        self.assertTrue(not task.serialized())
        
        task.serialize()
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertTrue(task.serialized())
        mbags = mgr.get_serialized_files()
        self.assertTrue(mbags)
        self.assertEqual(len(mbags), 4)
        self.assertEqual(os.path.basename(mbags[0]),
                         os.path.basename(mgr.get_finalized_aip())+".zip")
        self.assertEqual(mbags[1], mbags[0]+".sha256")
        self.assertEqual(mbags[2], re.sub(r'-0', '-1', mbags[0]))
        self.assertEqual(mbags[3], mbags[2]+".sha256")
        self.assertTrue(mgr.get_finalized_aip())
        self.assertTrue(os.path.isdir(mgr.get_finalized_aip()))
        self.assertTrue(not os.path.isdir(mgr.get_sip()))
        
    def test_archive(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        self.assertTrue(not task.submitted_to_archive())
        
        task.archive()
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertTrue(task.serialized())
        self.assertTrue(task.submitted_to_archive())
        submitted = os.listdir(self.storedir)
        self.assertTrue(len(submitted), 2)
        self.assertTrue(task.archived())

    def test_publish(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        self.assertTrue(not task.published())
        
        task.publish()
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertTrue(task.serialized())
        self.assertTrue(task.archived())
        self.assertTrue(task.published())
#        self.assertTrue(mgr.all_completed)

        ingesteddir = os.path.join(self.ingestdir, "succeeded")
        self.assertTrue(os.path.isdir(ingesteddir))
        self.assertTrue(os.path.isfile(os.path.join(ingesteddir,self.aipid+".json")))
        ingesteddir = os.path.join(self.dcdir, "published")
        self.assertTrue(os.path.isdir(ingesteddir))
        self.assertTrue(os.path.isfile(os.path.join(ingesteddir,self.aipid+".json")))

    def test_run(self):
        task = self.factory.create_task(self.sm)
        mgr = task._statemgr
        self.assertTrue(not task.finalized())
        self.assertTrue(not task.published())
        
        task.run()
        self.assertTrue(task.finalized())
        self.assertTrue(task.validated())
        self.assertTrue(task.published())
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

