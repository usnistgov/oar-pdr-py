import os, json, pdb, logging, tempfile, pathlib, re, time, csv
from typing import Mapping
import unittest as test

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate, ObjectNotFound, PartNotAccessible
from nistoar.midas.dbio import project as prj, status
from nistoar.midas.dap.service import mds3
from nistoar.pdr.utils import read_nerd, prov
from nistoar.nerdm.constants import CORE_SCHEMA_URI
from nistoar.nerdm.convert.latest import NERDm2Latest
from nistoar.pdr.utils.validate import ALL, REQ, WARN
from nistoar.pdr import def_etc_dir
from nistoar.base import config
from nistoar.pdr.public.sim import repo
from nistoar.midas.dap.pubmonitor import MIDASPublishingMonitor

import yaml

# test records
testdir = pathlib.Path(__file__).parents[0]
pdrdir = testdir.parents[2] / 'pdr'
pdr2210 = pdrdir / 'describe' / 'data' / 'pdr2210.json'
ncnrexp0 = pdrdir / 'publish' / 'data' / 'ncnrexp0.json'
storedir = pdrdir / 'distrib' / 'data'
datadir = pdrdir / 'preserve' / 'data'
basedir = testdir.parents[5]
ormdir = basedir/'metadata'
modeldir = ormdir / 'model'
scrpdir = basedir / 'scripts'
pdpconfig = testdir.parents[2]/'pdr'/'publish'/'service'/'wsgi'/'pdr-publish2.yml'
mds3config = testdir/'mds3config.yml'
simplenerd = datadir / '1491nerdm.json'

uwsgiscript = repo.__file__
port = 9991
prefixes = ["10.88434", "10.18434"]
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
                     uwsgiscript, pidfile, storedir, tdir)
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
    pdpconfig = os.path.join(tdir, 'pdpconfig.yml')
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    mocksvr = scrpdir/"pdp-uwsgi.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4} --set-ph oar_config_file={5}"
    cmd = cmd.format(os.path.join(tdir,"pdpsrv.log"), uwsgi_opts, srvport, mocksvr,
                     pidfile, pdpconfig)
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

tmpdir = tempfile.TemporaryDirectory(prefix="_test_mds3.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_mds3.log"))
    loghdlr.setLevel(logging.DEBUG)
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

nistr = prov.Agent("midas", prov.Agent.USER, "nstr1", "midas")

class TestPublishViaMDS3DAPService(test.TestCase):

    @classmethod
    def load_config(cls, override):
        out = None
        with open(pdpconfig) as fd:
            out = yaml.safe_load(fd)
        return config.merge_config(override, out)

    @classmethod
    def setUpClass(cls):
        cls.workdir = os.path.join(tmpdir.name, "pdr")
        if not os.path.exists(cls.workdir):
            os.mkdir(cls.workdir)

        cls.hbagdir    = os.path.join(cls.workdir, "headbags")
        cls.storedir   = os.path.join(cls.workdir, "store")
        cls.restricted = os.path.join(cls.workdir, "restricted")
        cls.ingestdir  = os.path.join(cls.workdir, "ingest")
        cls.dcdir      = os.path.join(cls.workdir, "doimint")
        cls.upldir     = os.path.join(cls.workdir, "uploads")
        cls.idregdir   = os.path.join(cls.workdir, "idregs")
        for d in (cls.hbagdir, cls.storedir, cls.restricted,
                  cls.ingestdir, cls.dcdir, cls.upldir, cls.idregdir):
            if not os.path.exists(d):
                os.mkdir(d)
        
        cfg = {
            'logdir': tmpdir.name,
            'logfile': 'pdp.log',
            'working_dir': cls.workdir,
            'base_ep': "/pdp",
            'uploads_dir': cls.upldir,
            'id_registry_dir': cls.idregdir,
            'repo_access': {
                'headbag_cache': cls.hbagdir,
                'store_dir': cls.storedir,
                'restricted_store_dir': cls.restricted
            },
            'nerdm_cache': os.path.join(cls.ingestdir, 'succeeded'),
            'preservation': {
                'task': {
                    'ingest': {
                        'rmm': { 'data_dir': cls.ingestdir },
                        'doi': { 'data_dir': cls.dcdir }
                    }
                }
            },
            "record_to": os.path.join(cls.workdir, "requests.log")
        }

        pdpcfg = cls.load_config(cfg)
        with open(os.path.join(tmpdir.name,"pdpconfig.yml"), 'w') as fd:
            yaml.dump(pdpcfg, fd, indent=2, sort_keys=False) # default_flow_style=True, width=float('inf')

        startServices()

    @classmethod
    def tearDownClass(cls):
        stopServices()

    def setUp(self):
        self.qfile = os.path.join(tmpdir.name, "monq.tsv")
        with open(self.qfile, 'a') as fd:
            pass
        self.cfg = {
            "dbio": {
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mds3"
                    },
                    "allowed_shoulders": {
                        "midas": ["mds2", "mds3"]
                    }
                }
            },
            "assign_doi": "always",
            "doi_naan": "10.88888",
            "nerdstorage": {
#                "type": "fsbased",
#                "store_dir": os.path.join(tmpdir.name)
                "type": "inmem",
            },
            "default_responsible_org": {
                "@type": "org:Organization",
                "@id": mds3.NIST_ROR,
                "title": "NIST"
            },
            "taxonomy_dir": modeldir,
            "taxonomy_file_pattern": "taxonomy.*\.json",
#            "taxonomy_dir": os.environ.get('OAR_ETC_DIR', def_etc_dir),
            "available_collections": {
                "peanuts": { "@id": "ark:/88888/pdr0-goober", "title": "Peanuts!" },
                "raisins": { "@id": "ark:/88888/pdr0-gurn", "title": "Raisinettes" }
            },
            "reviewer_ids": [ "nstr1" ],
            "publish": {
                "service_endpoint": "http://localhost:9993/pdp/pdp1",
                "auth": { "auth_key": 'MIDASTOKEN' },
                "monitor": { "queue_file": self.qfile }
            }
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsy": 2 }})

    def tearDown(self):
        if os.path.isfile(self.qfile):
            os.remove(self.qfile)

    def create_service(self):
        self.svc = mds3.DAPService(self.dbfact, self.cfg, nistr, rootlog.getChild("mds3"))
        self.nerds = self.svc._store
        return self.svc

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.svc.dbcli)
        self.assertEqual(self.svc.cfg, self.cfg)
        self.assertEqual(self.svc.who.actor, "nstr1")
        self.assertEqual(self.svc.who.agent_class, "midas")
        self.assertTrue(self.svc.log)
        self.assertTrue(self.svc._store)
        self.assertTrue(self.svc._valid8r)
        self.assertEqual(self.svc._minnerdmver, (0, 6))
        self.assertTrue(self.svc._pubcli)

    def open_nerd(self, nerdf='simplenerd'):
        with open(simplenerd) as fd:
            nerdm = json.load(fd)
        n2latest = NERDm2Latest(logging.getLogger("test.open_nerd"))
        return n2latest.convert(nerdm)

    def test_publish(self):
        self.create_service()
        nerdm = self.open_nerd()
        prec = self.svc.create_record("simple", nerdm)
        id = prec.id
        self.svc.update_data(id, {"title": "Shazam!: the Movie",
                                  "description":  "meh",
                                  "topic": [{
                                      "scheme": "https://data.nist.gov/od/dm/nist-themes/v2.0",
                                      "tag": "Information Technolog"
                                  }] })
        self.svc.replace_authors(id, [
            { "familyName": "Cranston", "givenName": "Gurn", "middleName": "J." },
            { "fn": "Edgar Allen Poe", "affiliation": "NIST" },
            { "familyName": "Grant", "givenName": "U.", "middleName": "S." }
        ])
        self.assertEqual(prec.status.state, status.EDIT)

        stat = self.svc.submit(prec.id)
        self.assertEqual(stat.state, status.SUBMITTED)   # for review
        stat = self.svc.publish(prec.id)
        self.assertNotEqual(stat.state, status.EDIT)
        self.assertNotEqual(stat.state, status.UNWELL)
        self.assertEqual(stat.state, status.INPRESS)

        self.assertTrue(os.path.isfile(self.qfile))
        qitems = []
        with open(self.qfile, 'r') as fd:
            rdr = csv.reader(fd, delimiter='\t')
            for row in rdr:
                qitems.append(row)
        self.assertEqual(len(qitems), 1)
        self.assertEqual(qitems[0][0], "mds3:0001")
        self.assertEqual(len(qitems[0]), 3)

        statdir = os.path.join(tmpdir.name, "pdr/publish/pdp1/status")
        pmon = MIDASPublishingMonitor(self.dbfact, statdir, self.qfile, 2, self.cfg, nistr,
                                      logging.getLogger("test.pubmon"))
        pmon.monitor(stop_after=2, timeout=5)

        qitems = []
        with open(self.qfile, 'r') as fd:
            rdr = csv.reader(fd, delimiter='\t')
            for row in rdr:
                qitems.append(row)
        self.assertEqual(len(qitems), 0)
        
        stat = self.svc.get_status(prec.id)
        self.assertEqual(stat.state, status.PUBLISHED)

                         
if __name__ == '__main__':
    test.main()
        
        
