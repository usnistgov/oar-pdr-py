import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.public.resolve import wsgi

testdir = os.path.dirname(os.path.abspath(__file__))
basedir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir))))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

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

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    rmtmpdir()

class TestResolverApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.cfg = {
            "locations": {
                "landingPageService": "https://data.nist.gov/pdr/od/id/",
                "distributionService":    baseurl,
                "resolverService":    "https://data.nist.gov/od/id/"
            }
        }
        self.app = wsgi.ResolverApp(self.cfg)
        self.resp = []

    def tearDown(self):
        self.resp = []

    def test_ready(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
            'HTTP_ACCEPT': 'text/plain'
        }

        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")

    def test_aip(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/aip',
            'HTTP_ACCEPT': 'text/plain'
        }

        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")

    def test_pdrid(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/id',
            'HTTP_ACCEPT': 'text/plain'
        }

        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")

    def test_post_aip(self):
        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/aip/ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "application/json"
        }
        body = self.tostr( self.app(req, self.start) )

        self.assertEqual(self.resp[0][:4], "405 ")

    def test_post_pdrid(self):
        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/pdrid/ark:/88434/mds2-2107",
            'HTTP_ACCEPT': "application/json"
        }
        body = self.tostr( self.app(req, self.start) )

        self.assertEqual(self.resp[0][:4], "405 ")
        
    def test_get_unknown(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/goob",
        }
        body = self.tostr( self.app(req, self.start) )

        self.assertEqual(self.resp[0][:4], "404 ")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/goob/mds2-2112",
        }
        body = self.tostr( self.app(req, self.start) )

        self.assertEqual(self.resp[0][:4], "404 ")

        
        





if __name__ == '__main__':
    test.main()


