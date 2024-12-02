import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
from pathlib import Path
import unittest as test

from nistoar.nsd.wsgi import nsd1 as wsgi
from nistoar.nsd import service


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

testdir = Path(__file__).parents[1]
datadir = testdir / 'data'

dburl = None
if os.environ.get('MONGO_TESTDB_URL'):
    dburl = os.environ.get('MONGO_TESTDB_URL')

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestHandlers(test.TestCase):

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
            "dir": datadir
        }
        self.svc = service.MongoPeopleService(dburl)
        self.svc.load(self.cfg, rootlog)
        self.resp = []

    def test_get_OUs(self):
        path = "NISTOU"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": f"/{path}/"
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("DOL DOC DOF DOS".split()))

    def test_get_Divisions(self):
        path = "NISTDivision"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": f"/{path}/"
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("LERA SAA".split()))

    def test_get_Groups(self):
        path = "NISTGroup"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": f"/{path}/"
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("BWM VTA".split()))

    def test_select_people(self):
        path = "People/list"
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": f"/{path}/"
        }
        req['wsgi.input'] = StringIO(json.dumps({"firstName": ["Phillip"]}))
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)        
        self.assertEqual(set([u['lastName'] for u in resp]), set("Austin Proctor".split()))


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestApp(test.TestCase):

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
            "db_url": dburl,
            "data": {
                "dir": datadir
            }
        }
        self.svc = service.MongoPeopleService(dburl)
        self.svc.load(self.cfg['data'], rootlog)
        self.app = wsgi.NSDApp(self.cfg)
        self.resp = []

    def test_divs(self):
        path = "NISTDivision"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": f"/{path}/"
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("LERA SAA".split()))

    def test_people(self):
        path = "People/list"
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": f"/{path}/"
        }
        req['wsgi.input'] = StringIO(json.dumps({"peopleID": [12]}))
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 1)        
        self.assertEqual(set([u['lastName'] for u in resp]), set("Ossman".split()))
        
    def test_status(self):
        path = ""
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/"
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(resp['status'], "ready")
        self.assertEqual(resp['person_count'], 4)
        self.assertEqual(resp['org_count'], 8)
        self.assertTrue(resp['message'].startswith("Ready"))
        


        
                         
if __name__ == '__main__':
    test.main()
