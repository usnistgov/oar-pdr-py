import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
from pathlib import Path
import unittest as test

from nistoar.midas.nsdi.wsgi import v1
from nistoar.nsd.client import NSDClient

tmpdir = tempfile.TemporaryDirectory(prefix="_test_nsdi.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_nsdi.log"))
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

@test.skipIf(not os.environ.get('PEOPLE_TEST_URL'), "test people service not available")
class TestPeopleIndexHandler(test.TestCase):
    
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.hdlr = None
        self.cli = NSDClient(os.environ.get('PEOPLE_TEST_URL'))
        self.resp = []

    def create_handler(self, req):
        path = req.get("PATH_INFO", "/").strip("/")
        return v1.PeopleIndexHandler(self.cli, path, req, self.start, log=rootlog)

    def test_do_GET(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/People",
            "QUERY_STRING": "prompt=phil"
        }
        self.hdlr = self.create_handler(req)
        body = self.hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(set(resp.keys()), set(['phillip']))

    def test_bad_path(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/Purple",
            "QUERY_STRING": "prompt=phil"
        }
        self.hdlr = self.create_handler(req)
        body = self.hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(resp["http:status"], 404)
        self.assertEqual(resp["http:reason"], "Not Found")
        self.assertEqual(resp["oar:message"], "Not Found")


@test.skipIf(not os.environ.get('PEOPLE_TEST_URL'), "test people service not available")
class TestOrgIndexHandler(test.TestCase):
    
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.hdlr = None
        self.cli = NSDClient(os.environ.get('PEOPLE_TEST_URL'))
        self.resp = []

    def create_handler(self, req):
        path = req.get("PATH_INFO", "/").strip("/")
        return v1.OrgIndexHandler(self.cli, path, req, self.start, log=rootlog)

    def test_do_GET(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/Group",
            "QUERY_STRING": "prompt=ve"
        }
        self.hdlr = self.create_handler(req)
        body = self.hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(set(resp.keys()), set(["verterans' tapdance administration"]))

    def test_bad_path(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/Purple",
            "QUERY_STRING": "prompt=phil"
        }
        self.hdlr = self.create_handler(req)
        body = self.hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(resp["http:status"], 404)
        self.assertEqual(resp["http:reason"], "Not Found")
        self.assertEqual(resp["oar:message"], "Not a recognized organization type: purple")


@test.skipIf(not os.environ.get('PEOPLE_TEST_URL'), "test people service not available")
class TestNSDIndexerApp(test.TestCase):
    
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.cfg = {
            "nsd": { "service_endpoint": os.environ.get('PEOPLE_TEST_URL') }
        }
        self.app = v1.NSDIndexerApp(rootlog, self.cfg)
        self.resp = []

    def test_group(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/Group",
            "QUERY_STRING": "prompt=ve"
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(set(resp.keys()), set(["verterans' tapdance administration"]))

    def test_people(self):
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/People",
            "QUERY_STRING": "prompt=phil"
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(set(resp.keys()), set(['phillip']))

    

        
                         
if __name__ == '__main__':
    test.main()
