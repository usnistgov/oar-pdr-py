import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
import unittest as test

from nistoar.midas.dbio import inmem, base, mongo
from nistoar.midas.dbio.wsgi import project as prj
from nistoar.pdr.publish import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
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

nistr = prov.PubAgent("midas", prov.PubAgent.USER, "nstr1")

dburl = None
if os.environ.get('MONGO_TESTDB_URL'):
    dburl = os.environ.get('MONGO_TESTDB_URL')

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMIDASProjectAppMongo(test.TestCase):

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
        self.dbfact = mongo.MongoDBClientFactory({}, os.environ['MONGO_TESTDB_URL'])
        self.svcfact = prj.ProjectServiceFactory(base.DMP_PROJECTS, self.dbfact, self.cfg, 
                                                 rootlog.getChild("midas.prj"))
        self.app = prj.MIDASProjectApp(self.svcfact, rootlog.getChild("dmpapi"), self.cfg)
        self.resp = []
        self.rootpath = "/midas/dmp/"

    def tearDown(self):
        cli = self.dbfact.create_client(base.DMP_PROJECTS)
        cli.native.drop_collection("dmp")
        cli.native.drop_collection("nextnum")
        cli.native.drop_collection("prov_action_log")

    def create_record(self, name="goob", meta=None):
        cli = self.dbfact.create_client(base.DMP_PROJECTS, self.cfg["dbio"], nistr.actor)
        out = cli.create_record(name, "mdm1")
        if meta:
            out.meta = meta
            out.save()
        return out

    def test_create(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])

        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name": "big", "owner": "nobody", "data": {"color": "red"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertEqual(hdlr.cfg, self.cfg)
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "big")
        self.assertEqual(resp['owner'], "nstr1")
        self.assertEqual(resp['id'], "mdm1:0001")
        self.assertEqual(resp['data'], {"color": "red"})
        self.assertEqual(resp['meta'], {})



                         
if __name__ == '__main__':
    test.main()
        
        
