import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.public.resolve.handlers.aip import AIPHandler

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), srvport,
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
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_simsrv.log"))
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

class TestAIPHandler(test.TestCase):

    def setUp(self):
        self.cfg = {
            "locations": {
                "landingPageService": "https://data.nist.gov/pdr/od/id/",
                "distributionService":    baseurl,
                "resolverService":    "https://data.nist.gov/od/id/"
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
        return AIPHandler(path, req, self.start, rootlog, self.cfg)

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
        
    def test_get_aip_info(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210",
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

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['versions'], ['1.0', '2', '3.1.3'])
        self.assertEqual(data['latestVersion'], '3.1.3')
        self.assertEqual(data['maxMultibagSequence'], 5)
        self.assertNotIn('version', data)
        self.assertIn('headBag', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210",
            'HTTP_ACCEPT': "text/plain"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: "+req['HTTP_ACCEPT'])
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['versions'], ['1.0', '2', '3.1.3'])
        self.assertEqual(data['latestVersion'], '3.1.3')
        self.assertEqual(data['maxMultibagSequence'], 5)
        self.assertNotIn('version', data)
        self.assertIn('headBag', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['versions'], ['1.0', '2', '3.1.3'])
        self.assertEqual(data['latestVersion'], '3.1.3')
        self.assertEqual(data['maxMultibagSequence'], 5)
        self.assertNotIn('version', data)
        self.assertIn('headBag', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210",
            'QUERY_STRING': "format=json",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0].strip(), "406 Not Acceptable")
        
    def test_resolve_aip_file(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210.1_0.mbag0_3-1.zip",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + baseurl + "_aip/pdr2210.1_0.mbag0_3-1.zip/_info")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210.1_0.mbag0_3-1",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + baseurl + "_aip/pdr2210.1_0.mbag0_3-1.zip/_info")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210.1_0.mbag0_3-1"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + baseurl + "_aip/pdr2210.1_0.mbag0_3-1.zip/_info")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210.1_0.mbag0_3-1",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + baseurl + "_aip/pdr2210.1_0.mbag0_3-1.zip")

    def test_resolve_aip_distrib_list(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:d/"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertTrue(isinstance(data, list), "/pdr:d did not return a list")
        self.assertEqual(len(data), 4)
        nms = [b.get('name') for b in data]
        for nm in "pdr2210.1_0.mbag0_3-0.zip pdr2210.1_0.mbag0_3-1.zip pdr2210.2.mbag0_3-2.zip pdr2210.3_1_3.mbag0_3-5.zip".split():
            self.assertIn(nm, nms)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr1010/pdr:d",
            'QUERY_STRING': "format=json",
            'HTTP_ACCEPT': "text/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: text/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertTrue(isinstance(data, list), "/pdr:d did not return a list")
        self.assertEqual(len(data), 2)
        nms = [b.get('name') for b in data]
        for nm in "pdr1010.mbag0_3-1.zip pdr1010.mbag0_3-2.zip".split():
            self.assertIn(nm, nms)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr1010/pdr:d/",
            'HTTP_ACCEPT': 'application/zip'
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "406 ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "mds2-2106/pdr:d"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "404 ")
        
    def test_resolve_aip_distrib_bag(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:d/2",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data.get('name'), "pdr2210.2.mbag0_3-2.zip")
        self.assertIn('downloadURL', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:d/pdr2210.2.mbag0_3-2.zip",
            'HTTP_ACCEPT': "text/json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: text/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data.get('name'), "pdr2210.2.mbag0_3-2.zip")
        self.assertIn('downloadURL', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:d/2",
            'QUERY_STRING': "format=native",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + data['downloadURL'])

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:d/2"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data.get('name'), "pdr2210.2.mbag0_3-2.zip")
        self.assertIn('downloadURL', data)

    def test_resolve_aip_head(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:h/",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['name'], "pdr2210.3_1_3.mbag0_3-5.zip")
        self.assertEqual(data['sinceVersion'], "3.1.3")
        self.assertIn('downloadURL', data)
        
        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:h/",
            'QUERY_STRING': "format=native",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + data['downloadURL'])

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:h"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['name'], "pdr2210.3_1_3.mbag0_3-5.zip")
        self.assertEqual(data['sinceVersion'], "3.1.3")
        self.assertIn('downloadURL', data)
        
        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:h/pdr:v"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "403 ")

    def test_resolve_aip_version_head(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0/pdr:h/",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['name'], "pdr2210.1_0.mbag0_3-1.zip")
        self.assertEqual(data['sinceVersion'], "1.0")
        self.assertIn('downloadURL', data)
        
        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0/pdr:h/",
            'QUERY_STRING': "format=native",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(hdr[0], "Location: " + data['downloadURL'])

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0/pdr:h"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['name'], "pdr2210.1_0.mbag0_3-1.zip")
        self.assertEqual(data['sinceVersion'], "1.0")
        self.assertIn('downloadURL', data)
        
        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0/pdr:h/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "403 ")

    def test_resolve_aip_version_list(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(len(hdr), 1)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "307 ")
        hdr = [h for h in self.resp if h.startswith("Location: ")]
        self.assertEqual(len(hdr), 1)

    def test_resolve_aip_version(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/2",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['versions'], ['1.0', '2', '3.1.3'])
        self.assertEqual(data['version'], '2')
        self.assertEqual(data['maxMultibagSequence'], 2)
        self.assertNotIn('latestVersion', data)
        self.assertIn('headBag', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertEqual(data['aipid'], "pdr2210")
        self.assertEqual(data['versions'], ['1.0', '2', '3.1.3'])
        self.assertEqual(data['version'], '1.0')
        self.assertEqual(data['maxMultibagSequence'], 1)
        self.assertNotIn('latestVersion', data)
        self.assertIn('headBag', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0",
            'HTTP_ACCEPT': "application/zip"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "406 ")

    def test_resolve_version_members(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "pdr2210/pdr:v/1.0/pdr:d/",
            'QUERY_STRING': "format=json"
        }
        hdlr = self.gethandler(req['PATH_INFO'], req)
        body = self.tostr( hdlr.handle() )

        self.assertEqual(self.resp[0][:4], "200 ")
        ct = [h for h in self.resp if h.startswith("Content-Type:")]
        self.assertEqual(len(ct), 1)
        self.assertEqual(ct[0].strip(), "Content-Type: application/json")
        ct = [h for h in self.resp if h.startswith("Content-Length:")]
        self.assertEqual(len(ct), 1)

        data = json.loads("\n".join(body))
        self.assertTrue(isinstance(data, list), "/pdr:d did not return a list")
        self.assertEqual(len(data), 2)
        nms = [b.get('name') for b in data]
        for nm in "pdr2210.1_0.mbag0_3-0.zip pdr2210.1_0.mbag0_3-1.zip".split():
            self.assertIn(nm, nms)
        for b in data:
            self.assertIn('downloadURL', b)

        self.resp = []
        





if __name__ == '__main__':
    test.main()


