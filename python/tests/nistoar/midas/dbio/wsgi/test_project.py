import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.wsgi import project as prj
from nistoar.pdr.publish import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_broker.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_pdp.log"))
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

nistr = prov.PubAgent("midas", prov.PubAgent.USER, "nstr1")

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
            "superusers": [ "rlp" ],
            "clients": {
                "midas": {
                    "default_shoulder": "mdm1"
                },
                "default": {
                    "default_shoulder": "mdm0"
                }
            },
            "allowed_project_shoulders": ["mdm1", "spc1"],
            "default_shoulder": "mdm0"
        }
        self.dbfact = inmem.InMemoryDBClientFactory(self.cfg, { "nextnum": { "mdm1": 2 }})
        self.app = prj.MIDASProjectApp(base.DMP_PROJECTS, rootlog.getChild("dmpapi"), self.dbfact, self.cfg)
        self.resp = []
        self.rootpath = "/midas/dmp/"

    def create_record(self, name="goob"):
        return self.dbfact.create_client(base.DMP_PROJECTS, nistr.actor).create_record(name, "mdm1")

    def sudb(self):
        return self.dbfact.create_client(base.DMP_PROJECTS, "rlp")

    def test_create_handler_name(self):
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectNameHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0001")

    def test_get_name(self):
        path = "mdm1:0003/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "goob")

        self.resp = []
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

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

        self.resp = []
        path = "mdm1:0001/name"
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

    def test_create_handler_full(self):
        path = "mdm1:0001/"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
        self.assertEqual(hdlr._path, "")
        self.assertEqual(hdlr._id, "mdm1:0001")

        path = "mdm1:0001"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
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

        self.resp = []
        path = "mdm1:0001"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

    def test_search(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
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
        self.assertEqual(hdlr.cfg, self.cfg)
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
        self.assertEqual(hdlr.cfg, self.cfg)
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
        self.assertEqual(hdlr.cfg, self.cfg)
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
        self.assertEqual(hdlr.cfg, self.cfg)
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
        self.assertEqual(hdlr.cfg, self.cfg)
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
        

                         
if __name__ == '__main__':
    test.main()
        
        
