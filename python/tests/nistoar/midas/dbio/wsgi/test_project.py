import os, json, pdb, logging, tempfile
from collections import OrderedDict
from pathlib import Path
from io import StringIO
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.wsgi import project as prj
from nistoar.pdr.utils import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
testdir = Path(__file__).parents[1]
datadir = testdir / "data"

request_body = datadir / 'request_body.json'
with open(request_body, 'r') as file:
    request_body = json.load(file)

def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_project.log"))
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

nistr = prov.Agent("dbio", prov.Agent.USER, "nstr1", "midas")

class TestMIDASProjectApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2dict(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.cfg = {
            "clients": {
                "midas": {
                    "default_shoulder": "mdm1"
                },
                "default": {
                    "default_shoulder": "mdm0"
                }
            },
            "dbio": {
                "superusers": [ "rlp" ],
                "allowed_project_shoulders": ["mdm1", "spc1"],
                "default_shoulder": "mdm0"
            },
            "include_headers": {
                "Access-Control-Allow-Origin": "*"
            }
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})
        self.svcfact = prj.ProjectServiceFactory(base.DMP_PROJECTS, self.dbfact, self.cfg, 
                                                 rootlog.getChild("midas.prj"))
        self.app = prj.MIDASProjectApp(self.svcfact, rootlog.getChild("dmpapi"), self.cfg)
        self.resp = []
        self.rootpath = "/midas/dmp/"

    def create_record(self, name="goob", meta=None):
        cli = self.dbfact.create_client(base.DMP_PROJECTS, self.cfg["dbio"], nistr.actor)
        out = cli.create_record(name, "mdm1")
        if meta:
            out.meta = meta
            out.save()
        return out

    def sudb(self):
        return self.dbfact.create_client(base.DMP_PROJECTS, self.cfg["dbio"], "rlp")

    def test_create_handler_name(self):
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'HTTP_ACCEPT': "*/*"
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectNameHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "name")
        self.assertEqual(hdlr._id, "mdm1:0001")

        # throw in tests for acceptable
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "application/json"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "text/json"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "text/plain"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "*/json"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "goob/json"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "text/html"
        self.assertTrue(not hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "text/html,text/json"
        self.assertTrue(hdlr.acceptable())
        hdlr._env['HTTP_ACCEPT'] = "text/html,*/*"
        self.assertTrue(hdlr.acceptable())

    def test_adv_select(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name":"Superconductor Metrology",
                                                 "data": {
                                                    "title": "Superconductor Metrology"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Superconductor Metrology')
        
        self.resp = []

        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name":"Standard Reference Materials",
                                                 "data": {
                                                    "title": "Standard Reference Materials"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Standard Reference Materials')
        
        self.resp = []

        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name":"Supplementary material for:",
                                                 "data": {
                                                    "title": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')
        
        self.resp = []


        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name":"Supplementary material for:22",
                                                 "data": {
                                                    "title": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')
        
        self.resp = []
        
        path = "mdm1:0006/"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')
        
        self.resp = []


        path=":selected"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }

        req['wsgi.input'] = StringIO(json.dumps( {"filter": {"$and": [ {"data.title": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"} ]},
    "permissions": ["read", "write"]} ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(len(resp),2)
        self.assertEqual(resp[0]['data']['title'],"Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy")
        self.assertEqual(resp[1]['data']['title'],"Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy")
        
        self.resp = []


        req['wsgi.input'] = StringIO(json.dumps(request_body))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(len(resp),1)
        self.assertEqual(resp[0]['data']['title'],"Standard Reference Materials")
        self.resp = []

        req['wsgi.input'] = StringIO(json.dumps( {"filter": {"$and": [ {"data.name": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"} ]},
    "permissions": ["read", "write"]} ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("204", self.resp[0])

        
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps( {"filter": {"$a-nd": [ {"dat-a.name": "test"} ]},
    "permissions": ["read", "write"]} ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400", self.resp[0])
        self.resp = []


        req['wsgi.input'] = StringIO(json.dumps( "Wrong data test" ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400", self.resp[0])
        self.resp = []


        req['wsgi.input'] = StringIO(json.dumps({
                "name": "John Doe",
                "age": 30,
                "email": "johndoe@example.com",
                "address": {
                    "street": "123 Main Street",
                    "city": "Anytown",
                    "zipcode": "12345"
                },
                }
                ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400", self.resp[0])

    def test_get_name(self):
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }

        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        prec = self.create_record("goob1")
        prec = self.create_record("goob2")

        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: application/json", ct)
        resp = self.body2dict(body)
        self.assertEqual(resp, "goob")

        self.resp = []
        req['HTTP_ACCEPT'] = "text/plain"
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: text/plain", ct)
        self.assertEqual(body, [b"goob"])

        self.resp = []
        req['HTTP_ACCEPT'] = "application/json"
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: application/json", ct)
        self.assertEqual(body, [b'"goob"'])

        self.resp = []
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        # Check for CORS header
        cors = [h for h in self.resp if h.startswith("Access-Control-Allow-Origin")]
        self.assertGreater(len(cors), 0)
        self.assertTrue(cors[0].startswith("Access-Control-Allow-Origin: *"))

    def test_put_name(self):
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "gary")

        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        resp = self.body2dict(body)
        self.assertEqual(resp, "gary")

        # test sending content-type
        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path,
            'CONTENT_TYPE': 'text/json',
            'HTTP_ACCEPT':  'text/plain'
        }
        req['wsgi.input'] = StringIO(json.dumps("hank"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: text/plain", ct)
        self.assertEqual(body[0].decode('utf-8'), "hank")

        # check for error when text is not encoded into JSON
        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path,
            'CONTENT_TYPE': 'application/json'
        }
        req['wsgi.input'] = StringIO("harry")   # not JSON
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])

        # check for unsupported content-type
        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path,
            'CONTENT_TYPE': 'text/csv'
        }
        req['wsgi.input'] = StringIO(json.dumps("harry"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("415 ", self.resp[0])

        # check that the value is unchanged after failed attempts
        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        resp = self.body2dict(body)
        self.assertEqual(resp, "hank")

        # check requesting plain text input
        self.resp = []
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path,
            'CONTENT_TYPE': 'text/plain'
        }
        req['wsgi.input'] = StringIO("harry")
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), "harry")

        self.resp = []
        path = "mdm1:0001/name"   # does not exist
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("hank"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_name_methnotallowed(self):
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

        self.resp = []
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

    def test_get_owner(self):
        path = "mdm1:0003/owner"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }

        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        prec = self.create_record("goob1")
        prec = self.create_record("goob2")

        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: application/json", ct)
        resp = self.body2dict(body)
        self.assertEqual(resp, "nstr1")

        self.resp = []
        req['HTTP_ACCEPT'] = "text/plain"
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type" in h]
        if ct:
            ct = ct[0]
        self.assertIn("Content-Type: text/plain", ct)
        self.assertEqual(body, [b"nstr1"])

    def test_put_owner(self):
        path = "mdm1:0003/owner"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "gary")

        self.resp = []
        path = "mdm1:0003/owner"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        resp = self.body2dict(body)
        self.assertEqual(resp, "gary")

    def test_create_handler_full(self):
        path = "mdm1:0001/"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0001")

        path = "mdm1:0001"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0001")

    def test_get_full(self):
        path = "mdm1:0003/"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record()

        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "goob")
        self.assertEqual(resp['id'], "mdm1:0003")

        self.resp = []
        path = "mdm1:0001"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_full_methnotallowed(self):
        path = "mdm1:0003"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

# DELETE is now allowed        
#        self.resp = []
#        path = "mdm1:0001"
#        req = {
#            'REQUEST_METHOD': 'DELETE',
#            'PATH_INFO': self.rootpath + path
#        }
#        hdlr = self.app.create_handler(req, self.start, path, nistr)
#        body = hdlr.handle()
#        self.assertIn("405 ", self.resp[0])

    def test_create(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])

        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody", "data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "big")
        self.assertEqual(resp['owner'], "nstr1")
        self.assertEqual(resp['id'], "mdm1:0003")
        self.assertEqual(resp['data'], {"color": "red"})
        self.assertEqual(resp['meta'], {})

    def test_delete(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody",
                                                 "data": {"color": "red", "pos": {"x": 0, "y": 1}}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['data'], {"color": "red", "pos": {"x": 0, "y": 1}})
        recid = resp['id']

        self.resp = []
        path = recid+"/data/pos/x"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertIs(resp, True)

        self.resp = []
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertIs(resp, False)

        self.resp = []
        path = recid+"/data"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, {"color": "red", "pos": {"y": 1}})

        self.resp = []
        path = recid+"/data"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertIs(resp, True)

        self.resp = []
        path = recid+"/data"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, {})

        self.resp = []
        path = recid
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("501 ", self.resp[0])


    def test_search(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), [])

        prec = self.create_record("bob")
        self.assertEqual(prec.name, "bob")
        prec = self.create_record("carole")
        self.assertEqual(prec.name, "carole")
        prec = self.sudb().get_record_by_name("carole", prec.owner)
        self.assertEqual(prec.name, "carole")
        self.assertEqual(prec.id, "mdm1:0004")
        self.assertTrue(prec.authorized(prec.acls.WRITE, "nstr1"))
        prec.acls.revoke_perm_from(prec.acls.WRITE, "nstr1")
        prec.save()
        self.assertTrue(not prec.authorized(prec.acls.WRITE, "nstr1"))

        self.resp = []
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        matches = self.body2dict(body)
        self.assertEqual(len(matches), 2)
        names = [m['name'] for m in matches]
        self.assertIn("bob", names)
        self.assertIn("carole", names)

        self.resp = []
        req['QUERY_STRING'] = "perm=write"
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        matches = self.body2dict(body)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]['name'], "bob")

        self.resp = []
        req['QUERY_STRING'] = "perm=write&perm=read"
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        matches = self.body2dict(body)
        self.assertEqual(len(matches), 2)
        names = [m['name'] for m in matches]
        self.assertIn("bob", names)
        self.assertIn("carole", names)
        
    def test_getput_data(self):
        path = "mdm1:0003/data"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0003")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        prec = self.create_record()
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), {})

        self.resp = []
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"color": "red", "pos": {"vec": [1,2,3]}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(self.body2dict(body), {"color": "red", "pos": {"vec": [1,2,3]}})
        
        self.resp = []
        path += "/color"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), "red")

        self.resp = []
        path = "mdm1:0003/data/pos/vec"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps([4,5,6]))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), [4,5,6])

        self.resp = []
        path = "mdm1:0003"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        prec = self.body2dict(body)
        self.assertEqual(prec['id'], "mdm1:0003")
        self.assertEqual(prec['name'], "goob")
        self.assertEqual(prec['data'], {"color": "red", "pos": {"vec": [4,5,6]}})
        self.assertEqual(prec['meta'], {})

    def test_create_handler_datapart(self):
        path = "pdr0:0012/data/authors"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "authors")
        self.assertEqual(hdlr._id, "pdr0:0012")

    def test_create_handler_acls(self):
        path = "mdm1:0003/acls"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectACLsHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0003")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        prec = self.create_record()
        acls = dict([(p, ["nstr1"]) for p in "read write admin delete".split()])
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), acls)

    def test_acls_methnotallowed(self):
        path = "mdm1:0003/acls"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record()
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

        self.resp = []
        req['REQUEST_METHOD'] = 'DELETE'
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()

    def test_getupd_aclsperm(self):
        path = "mdm1:0003/acls/read"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectACLsHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "read")
        self.assertEqual(hdlr._id, "mdm1:0003")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        prec = self.create_record()
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1"])

        self.resp = []
        req['REQUEST_METHOD'] = 'POST'
        req['wsgi.input'] = StringIO(json.dumps("gary"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "gary"])

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        del req['wsgi.input']
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "gary"])

        self.resp = []
        req['REQUEST_METHOD'] = 'PATCH'
        req['wsgi.input'] = StringIO(json.dumps(["gary", "hank"]))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "gary", "hank"])

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        del req['wsgi.input']
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "gary", "hank"])

        self.resp = []
        req['REQUEST_METHOD'] = 'PUT'
        req['wsgi.input'] = StringIO(json.dumps(["hank", "nstr1"]))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["hank", "nstr1"])

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        del req['wsgi.input']
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["hank", "nstr1"])
        
    def test_getdel_aclspermmem(self):
        path = "mdm1:0003/acls/write/hank"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectACLsHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "write/hank")
        self.assertEqual(hdlr._id, "mdm1:0003")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        prec = self.create_record()
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), False)

        prec.acls.grant_perm_to("write", "hank")
        prec.save()
        
        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), True)
        
        self.resp = []
        req['REQUEST_METHOD'] = 'DELETE'
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), False)

        self.resp = []
        path = "mdm1:0003/acls/write"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1"])
        
    def test_get_info(self):
        path = "mdm1:0003/id"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectInfoHandler))
        prec = self.create_record("goob", {"foo": "bar"})
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "mdm1:0003")

        self.resp = []
        path = "mdm1:0001/id"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        path = "mdm1:0003/meta"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectInfoHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, {"foo": "bar"})

        self.resp = []
        path = "mdm1:0003/meta/foo"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectInfoHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "bar")

        self.resp = []
        path = "mdm1:0003/meta/bob"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectInfoHandler))
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_get_status(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody", "data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "big")
        self.assertEqual(resp['owner'], "nstr1")
        self.assertEqual(resp['id'], "mdm1:0003")
        self.assertEqual(resp['status']['state'], "edit")
        self.assertEqual(resp['status']['action'], "create")

        self.resp = []
        path = "mdm1:0003/status"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)

        self.assertEqual(resp['state'], 'edit')
        self.assertEqual(resp['action'], 'create')
        self.assertIn('modified', resp)
        self.assertIn('since', resp)
        self.assertIn('message', resp)
        
    def test_update_status_message(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody", "data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "big")
        self.assertEqual(resp['owner'], "nstr1")
        self.assertEqual(resp['id'], "mdm1:0003")
        self.assertEqual(resp['status']['state'], "edit")
        self.assertEqual(resp['status']['action'], "create")
        self.assertTrue(resp['status'].get('message'))
        self.assertNotEqual(resp['status']['message'], 'starting over')

        self.resp = []
        path = "mdm1:0003/status"
        req = {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"message": "starting over"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)

        self.assertEqual(resp['state'], 'edit')
        self.assertEqual(resp['action'], 'create')
        self.assertIn('modified', resp)
        self.assertIn('since', resp)
        self.assertEqual(resp['message'], 'starting over')

    def test_process(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody", "data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "big")
        self.assertEqual(resp['owner'], "nstr1")
        self.assertEqual(resp['id'], "mdm1:0003")
        self.assertEqual(resp['status']['state'], "edit")
        self.assertEqual(resp['status']['action'], "create")
        self.assertTrue(resp['status'].get('message'))
        self.assertNotEqual(resp['status']['message'], 'starting over')

        self.resp = []
        path = "mdm1:0003/status"
        req = {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"message": "starting over", "action": "sleep"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])

        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"action": "finalize"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)

        self.assertEqual(resp['state'], 'ready')
        self.assertEqual(resp['action'], 'finalize')
        self.assertIn('modified', resp)
        self.assertIn('since', resp)
        self.assertIn('ready', resp['message'])

        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"action": "submit", "message": "I'm done!"}))
        req['REQUEST_METHOD'] = 'PUT'
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)

        self.assertEqual(resp['state'], 'submitted')
        self.assertEqual(resp['action'], 'submit')
        self.assertIn('modified', resp)
        self.assertIn('since', resp)
        self.assertEqual(resp['message'], "I'm done!")

        

                         
if __name__ == '__main__':
    test.main()
        
        
