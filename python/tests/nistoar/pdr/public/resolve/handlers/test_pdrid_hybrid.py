import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.public.resolve.handlers.pdrid import PDRIDHandler
from nistoar.pdr.utils import read_json, write_json
from nistoar.pdr import constants as const

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
desctestdir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(testdir))), "describe")
datadir = os.path.join(desctestdir, 'data', 'rmm-test-archive', 'versions')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)
VER_DELIM  = const.RELHIST_EXTENSION
DSVER_DELIM = "/_v"
verpath_re = re.compile(VER_DELIM+r'/\d+\.\d+.\d+')
dsverpath_re = re.compile(DSVER_DELIM+r'/\d+\.\d+.\d+')

cachedir = None
def setup_cache():
    global cachedir
    ensure_tmpdir()
    cachedir = os.path.join(tmpdir(), "cache")
    os.mkdir(cachedir)
    for f in "mds2-2106-v1_5_0.json mds2-2106-v1_6_0.json".split():
        nerd = read_json(os.path.join(datadir, f))
        nerd['big'] = True
        if VER_DELIM in nerd.get('@id',''):
            nerd['@id'] = verpath_re.sub('', nerd['@id'])
        for cmp in nerd.get('components', []):
            if DSVER_DELIM in cmp.get('downloadURL',''):
                cmp['downloadURL'] = dsverpath_re.sub('', cmp['downloadURL'])
            if VER_DELIM in cmp.get('downloadURL',''):
                cmp['downloadURL'] = verpath_re.sub('', cmp['downloadURL'])
        write_json(nerd, os.path.join(cachedir, f))

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    adir = artifactdir(__name__)
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(adir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    os.system(cmd)
    time.sleep(0.5)

def stopService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir, "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    setup_cache()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(artifactdir(__name__),"test_simsrv.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    startService()

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    stopService()
    rmtmpdir()

class TestPDRIDHandler(test.TestCase):

    def setUp(self):
        self.cfg = {
            "metadata_cache_dir": os.path.join(tmpdir(), "cache"),
            "locations": {
                "landingPageService": "https://data.nist.gov/pdr/od/id/",
                "resolverService":    "https://data.nist.gov/od/id/"
            },
            "APIs": {
                "mdSearch":    baseurl
            }                
        }
        self.resp = []

    def tearDown(self):
        self.resp = []

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def gethandler(self, path, req):
        return PDRIDHandler(path, req, self.start, self.cfg, rootlog)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def test_get_dataset_nerdm(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "application/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: "+req['HTTP_ACCEPT'])
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 1584")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        self.assertIn('components', data)
        self.assertTrue(not data.get('big'))

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 9334")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106")
        self.assertEqual(data['version'], "1.6.0")
        self.assertEqual(len(data.get('releaseHistory', {}).get('hasRelease', [])), 7)
        self.assertIn('components', data)
        self.assertTrue(data.get('big'))
        
    def test_get_releaseset(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 3708")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v")
        self.assertNotIn('components', data)
        self.assertIn('hasRelease', data)
        self.assertEqual(len(data['hasRelease']), 7)

    def test_resolve_dataset_version(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/1.6.0"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 9382")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v/1.6.0")
        self.assertIn('components', data)
        self.assertTrue(data.get('big'))

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/1.3.0"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 9460")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v/1.3.0")
        self.assertEqual(data['version'], "1.3.0")
        self.assertEqual(len(data.get('releaseHistory', {}).get('hasRelease', [])), 7)
        self.assertIn('components', data)
        self.assertTrue(not data.get('big'))


    def test_resolve_ediid(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "1E0F15DAAEFB84E4E0531A5706813DD8436"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 3320")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds003r0x6")
        self.assertIn('components', data)

    def test_get_file_comp(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:f/Readme.txt",
            'QUERY_STRING': 'format=nerdm'
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 1017")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/cmps/Readme.txt")
        self.assertEqual(data['downloadURL'], "https://data.nist.gov/od/ds/mds2-2106/Readme.txt")
        self.assertEqual(data['isPartOf'], "ark:/88434/mds2-2106")
        self.assertEqual(data['version'], "1.6.0")
        self.assertIn('@context', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/1.4.0/pdr:f/Readme.txt",
            'QUERY_STRING': 'format=nerdm'
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Length: 1061")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v/1.4.0/cmps/Readme.txt")
        self.assertEqual(data['downloadURL'],
                         "https://data.nist.gov/od/ds/ark:/88434/mds2-2106/_v/1.4.0/Readme.txt")
        self.assertEqual(data['isPartOf'], "ark:/88434/mds2-2106/pdr:v/1.4.0")
        self.assertEqual(data['version'], "1.4.0")
        self.assertIn('@context', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/1.4.0/pdr:f/Readme.txt",
            'QUERY_STRING': "format=native"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0],
                         "Location: https://data.nist.gov/od/ds/ark:/88434/mds2-2106/_v/1.4.0/Readme.txt")


        

        






if __name__ == '__main__':
    test.main()


