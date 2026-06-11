import sys, os, json, pdb, logging, tempfile, zipfile, shutil, time, re
from pathlib import Path
import unittest as test

from nistoar.testing import *
import nistoar.pdr.preserve.bagit.builder as bldr
import nistoar.pdr.exceptions as exceptions
from nistoar.pdr.preserve.bagit.bag import NISTBag
from nistoar.pdr.utils import prov
from nistoar.base import config

from nistoar.pdr.publish.service import pdp
from nistoar.pdr.publish.service import status

tstdir = Path(__file__).resolve().parent
pdrdir = tstdir.parents[1]
datadir = pdrdir / "preserve" / "data"
storedir = pdrdir / "distrib" / "data"
archdatadir = pdrdir / "describe" / "data" / "rmm-test-archive"
basedir = pdrdir.parents[3]
ormdir = basedir / "metadata"
assert storedir.is_dir()

port = 9991
prefixes = ["10.18434", "10.88888"]
arkpre = re.compile(r'^ark:/\d+/')

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startServices(authmeth=None):
    tdir = tmpdir()
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

    srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    arcdir = os.path.join(tdir, "archive")
    shutil.copytree(archdatadir, arcdir)
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4} --set-ph archive_dir={5}"
    cmd = cmd.format(os.path.join(tdir,"simdescsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile, arcdir)
    os.system(cmd)

    time.sleep(0.5)

def stopServices():
    tdir = tmpdir()
    srvport = port

    for p in range(srvport, srvport+4):
        pidfile = os.path.join(tdir,"simsrv"+str(p)+".pid")
        if os.path.exists(pidfile):
            cmd = "uwsgi --stop {0}".format(pidfile)
            # print(cmd)
            os.system(cmd)

    time.sleep(1)

tempcleanup = True

# tmpdir = tempfile.TemporaryDirectory(prefix="_test_publication.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_publish.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(config.LOG_FORMAT))
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

    global tempcleanup
    if tempcleanup:
        rmtmpdir()
    else:
        print("\ntest state retained in "+tmpdir())

from nistoar.pdr.constants import ARK_PFX_PAT
ARK_PFX_RE = re.compile(ARK_PFX_PAT)

tstag = prov.Agent("test", prov.Agent.AUTO, "tester", "test")
ncnrag = prov.Agent("ncnr_client", prov.Agent.AUTO, "tester", "ncnr")

class TestFullPDP1Process(test.TestCase):
    """
    This test combines the preservation service with the publishing service to demonstrate 
    the complete publishing workflow.
    """

    def setUp(self):
        self.tf = Tempfiles(tmpdir())
        self.workdir = self.tf.mkdir("pdr")
        self.statedir   = os.path.join(self.workdir, "state")
        self.hbagdir    = os.path.join(self.workdir, "headbags")
        self.storedir   = os.path.join(self.workdir, "store")
        self.restricted = os.path.join(self.workdir, "restricted")
        self.ingestdir  = os.path.join(self.workdir, "ingest")
        self.dcdir      = os.path.join(self.workdir, "doimint")
        self.upldir     = os.path.join(self.workdir, "uploads")
        for d in (self.statedir, self.hbagdir, self.storedir, self.restricted,
                  self.ingestdir, self.dcdir, self.upldir):
            if not os.path.exists(d):
                os.mkdir(d)

        self.config = {
            "working_dir": self.workdir,
            "uploads_dir": self.upldir,
            "clients": {
                "ncnr": {
                    "default_shoulder": "ncnr0",
                    "localid_provider": True,
                    "auth_key": "NCNRdev"
                },
                "test": {
                    "default_shoulder": "mds3",
                    "localid_provider": True,
                    "auth_key": "MDSdev"
                },
                "default": {
                    "default_shoulder": "pdp0",
                    "localid_provider": False,
                    "auth_key": "MIDASdev"
                }
            },
            "shoulders": {
                "ncnr0": {
                    "allowed_clients": [ "ncnr" ],
                    "bagger": {
                        "override_config_for": "pdp0",
                        "factory_function": "nistoar.pdr.publish.service.pdp.PDPBaggerFactory"
                    },
                    "id_minter": {
                        "naan": "88434",
                        "based_on_sipid": True,
                        "sequence_start": 21
                    }
                },
                "mds3": {
                    "allowed_clients": [ "test" ],
                    "bagger": {
                        "bag_builder": {
                            "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.7"
                        },
                        "doi_naan": "10.18434",
                        "assign_doi": "always",
                        "finalize": {},
                        "repo_base_url": "https://test.pdr.net/"
                    },
                    "id_minter": {
                        "naan": "88434",
                        "based_on_sipid": True,
                        "add_check_digit": False,
                        "sequence_start": 17
                    }
                }
            },
            'repo_access': {
                'headbag_cache': "headbags",   # relative to working dir
                'distrib_service': {
                    'service_endpoint': "http://localhost:9991/"
                },
                'metadata_service': {
                    'service_endpoint': "http://localhost:9994"
                },
                'store_dir': "store",
                'restricted_store_dir': "restricted"
            },
            "preservation": {
                "wait_to_start": 0.1,
                "task": {
                    'ingest': {
                        'rmm': {
                            'data_dir': self.ingestdir,
                            'service_endpoint': 'http://localhost:9992/nerdm/',
                            'auth_key': 'critic',
                            'auth_method': 'header'
                        },
                        'doi': {
                            'data_dir': self.dcdir,
                            'minting_naan': '10.18434',
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
                        "nist": {
                            "profile_version": "0.5"
                        }
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
        }

        self.pubsvc = pdp.PDP1Service(self.config)
    
    def tearDown(self):
        global tempcleanup
        if tempcleanup:
            self.tf.clean()

    def test_publish(self):
        # start with a clean slate
        self.assertTrue(self.pubsvc)
        self.assertTrue(self.pubsvc.pressvc)

        datasrc = datadir/'mds3sipbag'
        srcbag = NISTBag(datasrc)
        nerdm = srcbag.nerdm_record()
        pdrid = nerdm['@id']
        aipid = ARK_PFX_RE.sub('', pdrid)
        sipid = re.sub(r'-', ':', aipid)

        # should not be necessary
        #aipid += "sz"
        # nerdm['pdr:sipid'] = sipid

        # nothin' goin' on
        self.assertEqual(self.pubsvc.state_of(sipid), status.NOT_FOUND)
        self.assertEqual(list(self.pubsvc.pressvc.active_aip_ids()), [])

        # make it happen
        self.pubsvc.accept_resource_metadata(nerdm, tstag, sipid, True)

        self.assertEqual(self.pubsvc.state_of(sipid), status.PENDING)
        sipdir = os.path.join(self.workdir, 'sipbags', sipid)
        self.assertTrue(os.path.isdir(sipdir))

        # copy in the data files
        self.pubsvc.init_data_upload(sipid, 'fs', tstag)
        uploaddir = os.path.join(self.upldir, sipid)
        self.assertTrue(os.path.isdir(uploaddir))

        for f in os.listdir(srcbag.data_dir):
            src = os.path.join(srcbag.data_dir, f)
            if os.path.isfile(src):
                os.link(src, os.path.join(uploaddir, f))
            else:
                shutil.copytree(src, os.path.join(uploaddir, f))

        # import data files; the data dir starts empty
        tgtbag = NISTBag(sipdir)
        self.assertEqual(list(os.listdir(tgtbag.data_dir)), [])  # target data dir is empty
        
        self.pubsvc.import_files(sipid, tstag)

        for f in "trial1.json trial2.json trial3/trial3a.json".split():
            self.assertTrue(os.path.isfile(os.path.join(tgtbag.data_dir, f)))
            self.assertFalse(os.path.exists(os.path.join(uploaddir, f)))

        self.pubsvc.finalize(sipid, tstag)
        self.assertEqual(self.pubsvc.state_of(sipid), status.FINALIZED)

        self.pubsvc.publish(sipid, tstag)

        state = self.pubsvc.state_of(sipid)
        self.assertTrue(state == status.SUBMITTED or state == status.PUBLISHED)

        if state != status.PUBLISHED:
            time.sleep(1.5)
            pstat = self.pubsvc.pressvc.status_of(aipid)
            self.assertGreater(pstat.steps, 0)

        self.assertEqual(self.pubsvc.state_of(sipid), status.PUBLISHED)

        self.assertFalse(os.path.exists(sipdir))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        dosave = sys.argv.pop(1).lower()
        if dosave != "0" and dosave != "false":
            tempcleanup = False
    test.main()
        
