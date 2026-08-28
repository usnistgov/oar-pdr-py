import sys, os, json, pdb, logging, tempfile, zipfile, shutil, time, re
from pathlib import Path
from typing import List, Tuple, Mapping
import unittest as test
from io import StringIO

from nistoar.testing import *

from nistoar.pdr.publish.service.wsgi import pdp
from nistoar.pdr.publish.service import status
from nistoar.pdr.preserve.bagit.bag import NISTBag
from nistoar.pdr.utils import prov
from nistoar.base import config

import yaml

tstdir = Path(__file__).resolve().parent
pdrdir = tstdir.parents[2]
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

class WebResponse:

    def __init__(self, status: str, headers: Mapping, body: List):
        self._status = status.split(' ', 1)
        self.hdrlines = headers
        self.body = body

    def _decode(self, resplist):
        return [e.decode() for e in resplist]

    @property
    def content(self):
        return "".join(self._decode(self.body))

    def json(self):
        return json.loads(self.content)

    @property
    def status(self) -> Tuple[int, str]:
        code = int(self._status[0])
        return (code, self._status[1])

    @property
    def status_code(self) -> int:
        return self.status[0]

    @property
    def reason(self) -> int:
        return self.status[1]


class WebRequest:

    def __init__(self, reqenv):
        self._status = None
        self._resphdrs = {}
        self.env = reqenv

    def start(self, status, headers, extup=None):
        self._status = status
        self._rsphdrs = headers

    def response(self, body: List):
        return WebResponse(self._status, self._resphdrs, body)

    @classmethod
    def get(cls, path: str, authkey: str=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('GET', path, authkey, headers, qparams=qparams)

    @classmethod
    def head(cls, path: str, authkey: str=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('HEAD', path, authkey, headers, qparams=qparams)

    @classmethod
    def delete(cls, path: str, authkey: str=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('DELETE', path, authkey, headers, qparams=qparams)

    @classmethod
    def post(cls, path: str, authkey: str=None, body=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('POST', path, authkey, body, headers, qparams)

    @classmethod
    def put(cls, path: str, authkey: str=None, body=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('PUT', path, authkey, body, headers, qparams)

    @classmethod
    def patch(cls, path: str, authkey: str=None, body=None, headers: Mapping=None, qparams: Mapping=None):
        return cls.request('PATCH', path, authkey, body, headers, qparams)

    @classmethod
    def request(cls, meth, path, authkey: str=None, body=None, 
                headers: Mapping=None,qparams: Mapping=None):
        env = {
            'REQUEST_METHOD': meth,
            'PATH_INFO': path
        }
        if body:
            env['wsgi.input'] = body
        if headers:
            env.update(headers)
        if qparams:
            env['QUERY_STRING'] = "&".join([f"{k}={v}" for k,v in qparams.items()])
        if authkey:
            env['HTTP_AUTHORIZATION'] = f"Bearer {authkey}"
        return cls(env)

Req = WebRequest

class TestPDPApp(test.TestCase):

    def load_config(self, override):
        out = None
        with open(tstdir/"pdr-publish2.yml") as fd:
            out = yaml.safe_load(fd)
        return config.merge_config(override, out)

    def setUp(self):
        self.tf = Tempfiles(tmpdir())
        self.workdir = self.tf.mkdir("pdr")

        self.hbagdir    = os.path.join(self.workdir, "headbags")
        self.storedir   = os.path.join(self.workdir, "store")
        self.restricted = os.path.join(self.workdir, "restricted")
        self.ingestdir  = os.path.join(self.workdir, "ingest")
        self.dcdir      = os.path.join(self.workdir, "doimint")
        self.upldir     = os.path.join(self.workdir, "uploads")
        self.idregdir   = os.path.join(self.workdir, "idregs")
        for d in (self.hbagdir, self.storedir, self.restricted,
                  self.ingestdir, self.dcdir, self.upldir, self.idregdir):
            if not os.path.exists(d):
                os.mkdir(d)

        cfg = {
            'base_ep': "/pdp",
            'uploads_dir': self.upldir,
            'id_registry_dir': self.idregdir,
            'repo_access': {
                'headbag_cache': self.hbagdir,
                'store_dir': self.storedir,
                'restricted_store_dir': self.restricted
            },
            'nerdm_cache': os.path.join(self.ingestdir, 'succeeded'),
            'preservation': {
                'task': {
                    'ingest': {
                        'rmm': { 'data_dir': self.ingestdir },
                        'doi': { 'data_dir': self.dcdir }
                    }
                }
            }
        }
        self.cfg = self.load_config(cfg)
        self.app = pdp.PDPApp(self.cfg, workdir=self.workdir, base_ep="/pdp/")
        self.token = self.cfg['authorized'][1]['auth_key']

    def tearDown(self):
        global tempcleanup
        if tempcleanup:
            self.tf.clean()

    def test_publish(self):
        self.assertTrue(self.app)

        datasrc = datadir/'mds3sipbag'
        srcbag = NISTBag(datasrc)
        nerdm = srcbag.nerdm_record()
        pdrid = nerdm['@id']
        aipid = ARK_PFX_RE.sub('', pdrid)
        sipid = re.sub(r'-', ':', aipid)
        nerdm['@id'] = sipid
        # nerdm['pdr:sipid'] = sipid

        basep = '/pdp/pdp1/'

        # nothin' goin' on
        req = Req.head(basep+sipid, self.token)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 404)

        req = Req.get(basep, self.token)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])
        
        # make it happen
        input = StringIO(json.dumps(nerdm))
        req = Req.post(basep, self.token, input)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 201)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertIn('nanoparticles', saved['keyword'])
        self.assertNotIn('testing', saved['keyword'])
        self.assertEqual(saved['pdr:sipid'], sipid)
        self.assertEqual(saved['pdr:status'], 'pending')

        sipdir = os.path.join(self.workdir, 'publish/pdp1/sipbags', sipid)
        self.assertTrue(os.path.isdir(sipdir))

        req = Req.get(basep+sipid, self.token)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertIn('nanoparticles', saved['keyword'])
        self.assertNotIn('testing', saved['keyword'])
        self.assertEqual(saved['pdr:sipid'], sipid)
        self.assertEqual(saved['pdr:state'], 'pending')
        self.assertIn('pdrid', saved['pdr:pub_status'])
        self.assertNotIn('bagfiles', saved['pdr:pub_status'])

        # update via PUT
        saved['keyword'].append('testing')
        input = StringIO(json.dumps(saved))
        req = Req.put(basep+sipid, self.token, input)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertIn('nanoparticles', saved['keyword'])
        self.assertIn('testing', saved['keyword'])
        self.assertEqual(saved['pdr:sipid'], sipid)
        self.assertEqual(saved['pdr:state'], 'pending')
        
        # copy in the data files
        input = StringIO(json.dumps({'type': 'fs'}))
        req = Req.put(basep+sipid+"/:data", self.token, input)
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        where = resp.json()
        self.assertEqual(where['type'], 'fs')
        self.assertEqual(where['location'], sipid)
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

        req = Req.patch(basep+sipid, self.token, qparams={'action': 'import'})
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertEqual(saved['pdr:sipid'], sipid)
        self.assertEqual(saved['pdr:state'], 'pending')
        self.assertTrue(saved.get('pdr:message'))
        self.assertIn('trial1.json', saved['pdr:imported'])
        self.assertIn('trial2.json', saved['pdr:imported'])
        self.assertIn('trial3/trial3a.json', saved['pdr:imported'])
        
        for f in "trial1.json trial2.json trial3/trial3a.json".split():
            self.assertTrue(os.path.isfile(os.path.join(tgtbag.data_dir, f)))
            self.assertFalse(os.path.exists(os.path.join(uploaddir, f)))

        req = Req.patch(basep+sipid, self.token, qparams={'action': 'finalize'})
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertEqual(saved['pdr:sipid'], sipid)
        self.assertEqual(saved['pdr:state'], 'finalized')

        req = Req.patch(basep+sipid, self.token, qparams={'action': 'publish'})
        body = self.app(req.env, req.start)
        resp = req.response(body)
        self.assertEqual(resp.status_code, 200)
        saved = resp.json()
        self.assertEqual(saved['@id'], pdrid)
        self.assertEqual(saved['pdr:sipid'], sipid)
        state = saved['pdr:state']
        self.assertIn(state, ['submitted', 'published'])

        for i in range(10):
            if state == 'published' or state == 'failed' or state == 'on-hold':
                break
            time.sleep(0.5)
            req = Req.get(basep+sipid, self.token)
            body = self.app(req.env, req.start)
            resp = req.response(body)
            self.assertEqual(resp.status_code, 200)
            saved = resp.json()
            state = saved['pdr:state']

        self.assertEqual(saved['pdr:state'], 'published')

        archbag = f"{aipid}.1_0_0.mbag0_4-0.zip"
        self.assertTrue(os.path.isfile(os.path.join(self.storedir, archbag)))
        self.assertTrue(os.path.isfile(os.path.join(self.storedir, archbag+".sha256")))
        self.assertTrue(os.path.isfile(os.path.join(self.hbagdir, archbag)))
        self.assertFalse(os.path.exists(sipdir))
        inprogdir = os.path.join(self.workdir,"preserve",aipid)

            

        
        

if __name__ == '__main__':
    if len(sys.argv) > 1:
        dosave = sys.argv.pop(1).lower()
        if dosave != "0" and dosave != "false":
            tempcleanup = False
    test.main()
        
