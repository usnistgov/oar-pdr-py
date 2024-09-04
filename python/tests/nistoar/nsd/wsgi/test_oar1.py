import os, json, pdb, logging, tempfile, warnings
from collections import OrderedDict
from io import StringIO
from pathlib import Path
import unittest as test

from nistoar.nsd.wsgi import oar1 as wsgi
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

def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test

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

    @ignore_warnings
    def setUp(self):
        self.cfg = {
            "dir": datadir
        }
        self.svc = service.MongoPeopleService(dburl)
        self.svc.load(self.cfg, rootlog)
        self.resp = []

    def test_get_OUs(self):
        path = "OU"
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
        path = "Div"
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
        path = "Group"
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

    def test_select_orgs(self):
        path = ""
        req = {
            "REQUEST_METHOD": "GET",
            "QUERY_STRING": "with_orG_ACRNM=F",
            "PATH_INFO": path
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 1)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("DOF".split()))

        self.resp = []
        req["QUERY_STRING"] = "with_orG_Name=s&like=DO&like=VT&with_orG_Name=f"
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 5)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("DOL DOC DOF DOS VTA".split()))

        self.resp = []
        path = "select"
        req["PATH_INFO"] = f"/select/"
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 5)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("DOL DOC DOF DOS VTA".split()))

        self.resp = []
        path = "OU/select"
        req["PATH_INFO"] = f"/{path}/"
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("DOL DOC DOF DOS".split()))

        self.resp = []
        path = "Group"
        req["PATH_INFO"] = f"/{path}"
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 1)
        self.assertEqual(set([u['orG_ACRNM'] for u in resp]), set("VTA".split()))

    def test_select_org_index(self):
        path = "index"
        req = {
            "REQUEST_METHOD": "GET",
            "QUERY_STRING": "like=SA&like=VT",
            "PATH_INFO": path
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set(resp.keys()),
                         set(("small animal administration", "veterans' tapdance administration")))

        self.resp = []
        path = "Group/index"
        req["PATH_INFO"] = f"{path}"
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 1)
        self.assertEqual(set(resp.keys()), set(("veterans' tapdance administration",)))

        self.resp = []
        path = list(resp["veterans' tapdance administration"].keys())[0]
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = wsgi.OrgHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(resp['orG_ACRNM'], "VTA")
        self.assertEqual(str(resp['orG_ID']), path)

        
    def test_select_people(self):
        path = ""
        req = {
            "REQUEST_METHOD": "GET",
            "QUERY_STRING": "with_firstName=hil&with_firstName=peter",
            "PATH_INFO": path
        }
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 3)
        self.assertEqual(set([u['lastName'] for u in resp]), set("Austin Bergman Proctor".split()))

        self.resp = []
        req["QUERY_STRING"] = "with_firstName=hil&like=phil&with_firstName=peter"
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set([u['lastName'] for u in resp]), set("Austin Proctor".split()))

        self.resp = []
        path = "select"
        req["PATH_INFO"] = path
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)
        self.assertEqual(set([u['lastName'] for u in resp]), set("Austin Proctor".split()))


    def test_select_org_index(self):
        path = "index"
        req = {
            "REQUEST_METHOD": "GET",
            "QUERY_STRING": "like=phi",
            "PATH_INFO": path
        }
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 3)
        self.assertEqual(set(resp.keys()), set(("phillip", "proctor", "austin")))

        self.resp = []
        path = list(resp["austin"].keys())[0]
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(resp['lastName'], "Austin")
        self.assertEqual(str(resp['peopleID']), path)



    def test_post_select_people(self):
        path = "select"
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": path
        }
        req['wsgi.input'] = StringIO(json.dumps({"firstName": ["Phillip"]}))
        hdlr = wsgi.PeopleHandler(self.svc, path, req, self.start)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(len(resp), 2)        
        self.assertEqual(set([u['lastName'] for u in resp]), set("Austin Proctor".split()))


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestPeopleApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    @ignore_warnings
    def setUp(self):
        self.cfg = {
            "db_url": dburl,
            "data": {
                "dir": datadir
            },
            "loaderusers": ['anonymous']
        }
#        self.svc = service.MongoPeopleService(dburl)
        self.app = wsgi.PeopleApp(self.cfg)
        self.app.load()
        self.resp = []

    def test_divs(self):
        path = "Orgs/Div"
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
        path = "People/select"
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

    @ignore_warnings
    def test_load(self):
        path = ""
        req = {
            "REQUEST_METHOD": "LOAD",
            "PATH_INFO": "/"
        }
        body = self.app(req, self.start)
        self.assertIn("200 Data Reloaded", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(resp['oar:message'], "Successfully reloaded NSD data")

        self.resp = []
        req["REQUEST_METHOD"] = "GET"
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)

        self.assertEqual(resp['status'], "ready")
        self.assertEqual(resp['person_count'], 4)
        self.assertEqual(resp['org_count'], 8)
        self.assertTrue(resp['message'].startswith("Ready"))


        
                         
if __name__ == '__main__':
    test.main()
