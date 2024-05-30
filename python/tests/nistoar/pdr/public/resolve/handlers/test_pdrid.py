import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.public.resolve.handlers.pdrid import PDRIDHandler

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    adir = artifactdir(__name__)
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3}"
    cmd = cmd.format(os.path.join(adir,"simsrv.log"), srvport,
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

    def test_post(self):
        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "application/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "405 ")
        
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

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "text/json, */*;q=0.8"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: text/json")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        self.assertIn('components', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=nerdm&format=dcat"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        
        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        self.assertIn('components', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=nerdm",
            'HTTP_ACCEPT': "text/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: "+req['HTTP_ACCEPT'])
        
        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        self.assertIn('components', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=nerdm",
            'HTTP_ACCEPT': "text/*"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: text/json")
        
        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        self.assertIn('components', data)

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
        self.assertEqual(ct[0].strip(), "Content-Length: 9391")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106")
        self.assertEqual(data['version'], "1.6.0")
        self.assertEqual(len(data.get('releaseHistory', {}).get('hasRelease', [])), 7)
        self.assertIn('components', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=nerdm",
            'HTTP_ACCEPT': "text/html"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0].strip(), "406 Not Acceptable")
        
    def test_get_dataset_html(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "text/html"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "application/xhtml+xml"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=html&format=nerdm&format=dcat"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0].strip(), "406 Not Acceptable")
        
    def test_get_dataset_text(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "text/plain"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        hd = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(hd[0].strip(), "Content-Type: text/plain")
        hd = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(body[0][:5], "NIST ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=text",
            'HTTP_ACCEPT': "text/plain"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        hd = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(hd[0].strip(), "Content-Type: text/plain")
        hd = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(body[0][:5], "NIST ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=text&format=html"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        hd = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(hd[0].strip(), "Content-Type: text/plain")
        hd = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(body[0][:5], "NIST ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107",
            'QUERY_STRING': "format=text/plain"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        hd = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(hd[0].strip(), "Content-Type: text/plain")
        hd = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(hd), 1)
        self.assertEqual(body[0][:5], "NIST ")

    def test_bad_format(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107/pdr:v",
            'QUERY_STRING': "format=datacite"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )
        self.assertEqual(self.resp[0][:4], "400 ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2107/pdr:v",
            'HTTP_ACCEPT': "text/csv"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )
        self.assertEqual(self.resp[0][:4], "406 ")

    def test_get_releaseset(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v"
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
        self.assertEqual(ct[0].strip(), "Content-Length: 9535")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v/1.6.0")
        self.assertIn('components', data)

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

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "mds2-2106/pdr:v/1.3.0"
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

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:v/1.3.0",
            'HTTP_ACCEPT': "text/html"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/pdr/od/id/ark:/88434/mds2-2106/pdr:v/1.3.0")


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

        self.resp = []
        req = {
            'REQUEST_METHOD': "HEAD",
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
        self.assertEqual(len("".join(body)), 0)

                
    def test_get_access_comp(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:see/nvd.nist.gov",
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
        self.assertEqual(ct[0].strip(), "Content-Length: 491")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds003r0x6/pdr:see/nvd.nist.gov")
        self.assertEqual(data['accessURL'], "https://nvd.nist.gov")
        self.assertEqual(data['isPartOf'], "ark:/88434/mds003r0x6")
        self.assertEqual(data['version'], "1.0.0")
        self.assertIn('@context', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:see/nvd.nist.gov",
            'QUERY_STRING': "format=native"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://nvd.nist.gov")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:see/nvd.nist.gov"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://nvd.nist.gov")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:see/nvd.nist.gov",
            'HTTP_ACCEPT': "text/xml"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://nvd.nist.gov")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:v/1.0.0/pdr:see/nvd.nist.gov",
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
        self.assertEqual(ct[0].strip(), "Content-Length: 515")

        data = json.loads("\n".join(body))
        self.assertEqual(data['@id'], "ark:/88434/mds003r0x6/pdr:v/1.0.0/pdr:see/nvd.nist.gov")
        self.assertEqual(data['accessURL'], "https://nvd.nist.gov")
        self.assertEqual(data['isPartOf'], "ark:/88434/mds003r0x6/pdr:v/1.0.0")
        self.assertEqual(data['version'], "1.0.0")
        self.assertIn('@context', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds003r0x6/pdr:v/1.0.0/pdr:see/nvd.nist.gov",
            'QUERY_STRING': "format=native"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://nvd.nist.gov")


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
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:f/Readme.txt",
            'QUERY_STRING': "format=native"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/od/ds/mds2-2106/Readme.txt")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:f/Readme.txt",
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/od/ds/mds2-2106/Readme.txt")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "ark:/88434/mds2-2106/pdr:f/Readme.txt",
            'HTTP_ACCEPT': "text/plain"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "302 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: https://data.nist.gov/od/ds/mds2-2106/Readme.txt")

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


