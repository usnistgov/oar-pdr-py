import os, sys, pdb, json, logging, re, tempfile
import unittest as test
from copy import deepcopy

from nistoar.testing import *
from nistoar.web.rest import ready, WSGIAppSuite

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_nsd.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
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

class TestReady(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.hdlr = None
        self.resp = []

    def gethandler(self, path, env):
        return ready.Ready(path, env, self.start)

    def test_set_response(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr._code, 0)
        self.assertEqual(self.hdlr._msg, "unknown status")
        self.hdlr.set_response(388, "Mojo")
        self.assertEqual(self.hdlr._code, 388)
        self.assertEqual(self.hdlr._msg, "Mojo")
        self.assertEqual(self.resp, [])

        self.hdlr.end_headers()
        self.assertEqual(self.resp[0], "388 Mojo")

    def test_add_header(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(len(self.hdlr._hdr), 0)
        
        self.hdlr.add_header("Authorization", "Bearer Key")
        self.hdlr.add_header("Goober", "Gurn")
        self.hdlr.add_header("Goober", "Gomer")
        self.assertEqual(len(self.hdlr._hdr), 3)
        self.assertEqual(self.hdlr._hdr.get_all("Authorization"), ["Bearer Key"])
        self.assertEqual(self.hdlr._hdr.get_all("Goober"), ["Gurn", "Gomer"])

        self.hdlr.end_headers()
        self.assertEqual(self.resp[0], "0 unknown status")
        self.assertEqual(self.resp[1], "Authorization: Bearer Key")
        self.assertEqual(self.resp[2], "Goober: Gurn")
        self.assertEqual(self.resp[3], "Goober: Gomer")

    def test_get_requested_formats(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.get_requested_formats(), [])

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html"
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.get_requested_formats(), ["text", "html"])

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=datacite"
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.get_requested_formats(), ["datacite"])

    def test_ordered_accepts(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html",
            'HTTP_ACCEPT': 'text/html, */*;q=0.8, application/xhtml+xml, application/xml;q=0.9'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.get_accepts(),
                         "text/html application/xhtml+xml application/xml */*".split())
        
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html",
            'HTTP_ACCEPT': 'text/html'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.get_accepts(), "text/html".split())
        
    def test_send_ok(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.send_ok() )
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "200 OK")
        self.resp = []
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.send_ok("It's all good.", message="PERFECT") )
        self.assertEqual(body, ["It's all good."])
        self.assertEqual(self.resp[0], "200 PERFECT")
        self.resp = []
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.send_ok('"It is all good."', "text/json", "Got it", 201) )
        self.assertEqual(body, ['"It is all good."'])
        self.assertEqual(self.resp[0], "201 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_send_ok_head(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.send_ok('"It is all good."', "text/json", "Got it", 201, ashead=True) )
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "201 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_send_error(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.send_error(200, "OK") )
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "200 OK")
        self.resp = []

        body = self.tostr( self.hdlr.send_error(400, "Icky input") )
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "400 Icky input")
        self.resp = []

        body = self.tostr( self.hdlr.send_error(499, "Got it", '"It is all good."', "text/json") )
        self.assertEqual(body, ['"It is all good."'])
        self.assertEqual(self.resp[0], "499 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_handle(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 280)
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 17)
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 17)
        self.assertEqual("\n".join(body), " service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 17)
        self.assertEqual("\n".join(body), " service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 17)
        self.assertEqual("\n".join(body), " service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "406 Not Acceptable")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        self.assertEqual("\n".join(body), "format parameter is inconsistent with Accept header")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'HTTP_ACCEPT': "application/xhtml"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: application/xhtml")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 280)
        self.resp = []


    def test_handle_head(self):
        req = {
            'REQUEST_METHOD': "HEAD",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 280)
        self.assertEqual("\n".join(body), "")
        self.resp = []

        req = {
            'REQUEST_METHOD': "HEAD",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.tostr( self.hdlr.handle() )
        self.assertEqual(self.resp[0], "406 Not Acceptable")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        self.assertEqual("\n".join(body), "")
        self.resp = []

class TestReadyApp(test.TestCase):
    
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.app = ready.ReadyApp(rootlog)
        self.resp = []

    def test_create_handler(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        hdlr = self.app.create_handler(req, self.start, "/", None)

        self.assertTrue(isinstance(hdlr, ready.Ready))
        self.assertTrue(hdlr.app)
        self.assertEqual(hdlr.app.name, "Ready")

        body = self.tostr( hdlr.handle() )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")
        
class TestWSGISuiteAppReady(test.TestCase):

    class ReadyWSGIAppSuite(WSGIAppSuite):
        def __init__(self):
            subapp = ready.ReadyApp(rootlog.getChild('ready'))
            WSGIAppSuite.__init__(self, {}, {'': subapp}, rootlog, "/ready")

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.app = self.ReadyWSGIAppSuite()
        self.resp = []

    def testReady(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/ready/'
        }
        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")

    def testForbidden(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "403 Forbidden")

    def testNotFound(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/goober/goon'
        }
        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "404 Not Found")

    def testNotFound2(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/ready/goob'
        }
        body = self.tostr( self.app(req, self.start) )
        self.assertEqual(self.resp[0], "404 Not found")
        
    
        
        


if __name__ == '__main__':
    test.main()
