import os, json, pdb, logging, tempfile
from collections import OrderedDict
from pathlib import Path
from io import StringIO
import unittest as test
from unittest.mock import patch, Mock

from nistoar.midas.dbio import inmem, base, mongo
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

nistr = prov.Agent("midas", prov.Agent.USER, "nstr1", "midas")

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
            "dbio": {
                "superusers": [ "rlp" ],
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mdm1",
                        "public": "mdm0"
                    }
                }
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
        cli.disconnect()

    def create_record(self, name="goob", meta=None):
        cli = self.dbfact.create_client(base.DMP_PROJECTS, self.cfg["dbio"], nistr)
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

        #req still POST
        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name":"Standard Reference Materials",
                                                 "data": {
                                                    "title": "Standard Reference Materials"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Standard Reference Materials')

        #req still POST
        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name":"Supplementary material for:",
                                                 "data": {
                                                    "title": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')

        #req still POST
        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name":"Supplementary material for:22",
                                                 "data": {
                                                    "title": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"}
                                                    }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')

        
        path = "mdm1:0004/"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        #reset resp
        self.resp = []
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['data']['title'],'Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy')

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

        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps(request_body))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(len(resp),1)
        self.assertEqual(resp[0]['data']['title'],"Standard Reference Materials")

        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps( {"filter": {"$and": [ {"data.name": "Supplementary material for: The detection of carbon dioxide leaks using quasi-tomographic laser absorption spectroscopy"} ]},
    "permissions": ["read", "write"]} ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("204", self.resp[0])

        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps( {"filter": {"$a-nd": [ {"dat-a.name": "test"} ]},
    "permissions": ["read", "write"]} ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400", self.resp[0])


        #reset resp
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps( "Wrong data" ))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400", self.resp[0])

        #reset resp
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


    def test_select_by_ids(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        
        req['wsgi.input'] = StringIO(json.dumps({"name": "Record One", "data": {"title": "First Record"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp1 = self.body2dict(body)
        id1 = resp1['id']
        
        self.resp = []
        
        req['wsgi.input'] = StringIO(json.dumps({"name": "Record Two", "data": {"title": "Second Record"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp2 = self.body2dict(body)
        id2 = resp2['id']
        
        self.resp = []
        
        req['wsgi.input'] = StringIO(json.dumps({"name": "Record Three", "data": {"title": "Third Record"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp3 = self.body2dict(body)
        id3 = resp3['id']
        
        self.resp = []
        
        path = ":ids"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1},{id2}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 2)
        
        returned_ids = [r['id'] for r in results]
        self.assertIn(id1, returned_ids)
        self.assertIn(id2, returned_ids)
        self.assertNotIn(id3, returned_ids)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id3}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], id3)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': 'ids=nonexistent:0001'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 0)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])


    def test_export_by_ids(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        
        req['wsgi.input'] = StringIO(json.dumps({"name": "Export Record One", "data": {"title": "First Export Record"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp1 = self.body2dict(body)
        id1 = resp1['id']
        
        self.resp = []
        
        req['wsgi.input'] = StringIO(json.dumps({"name": "Export Record Two", "data": {"title": "Second Export Record"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp2 = self.body2dict(body)
        id2 = resp2['id']
        
        self.resp = []
        
        path = ":export"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1}&format=pdf'
        }
        
        with patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF-1.4\nmongo test content"):
            with patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule") as mock_preppy:
                mock_template = Mock()
                mock_template.get.return_value = "<document>MongoDB PDF Content</document>"
                mock_preppy.return_value = mock_template
                
                def fake_pdf_concat(results, output_filename):
                    return {
                        "format": "pdf",
                        "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                        "mimetype": "application/pdf",
                        "file_extension": ".pdf",
                        "bytes": b"%PDF-1.4\nmongo test content",  
                    }
                
                with patch.dict('nistoar.midas.export.export.CONCAT_REGISTRY', {"pdf": fake_pdf_concat}):
                    hdlr = self.app.create_handler(req, self.start, path, nistr)
                    self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
                    body = hdlr.handle()
                    
                    
                    self.assertIn("200 ", self.resp[0])
                    
                    
                    content_type_header = None
                    for header in self.resp:
                        if header.startswith('Content-Type:'):
                            content_type_header = header
                            break
                    
                    self.assertIsNotNone(content_type_header)
                    self.assertIn('application/pdf', content_type_header)
                    
                    
                    pdf_content = b''.join(body) if isinstance(body, list) else body
                    self.assertTrue(pdf_content.startswith(b'%PDF'))
                    self.assertEqual(pdf_content, b"%PDF-1.4\nmongo test content")
        
        self.resp = []
        
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1},{id2}&format=pdf'
        }
        
        with patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF-1.4\nmongo combined content"):
            with patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule") as mock_preppy:
                mock_template = Mock()
                mock_template.get.return_value = "<document>Combined MongoDB Content</document>"
                mock_preppy.return_value = mock_template
                
                
                def fake_pdf_concat(results, output_filename):
                    return {
                        "format": "pdf",
                        "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                        "mimetype": "application/pdf",
                        "file_extension": ".pdf",
                        "bytes": b"%PDF-1.4\nmongo combined content", 
                    }
                
                with patch.dict('nistoar.midas.export.export.CONCAT_REGISTRY', {"pdf": fake_pdf_concat}):
                    hdlr = self.app.create_handler(req, self.start, path, nistr)
                    body = hdlr.handle()
                    
                    
                    self.assertIn("200 ", self.resp[0])
                    
                    
                    content_type_header = None
                    for header in self.resp:
                        if header.startswith('Content-Type:'):
                            content_type_header = header
                            break
                    
                    self.assertIn('application/pdf', content_type_header)  
                    
                    
                    self.assertEqual(mock_template.get.call_count, 2)
                    
                
                    pdf_content = b''.join(body) if isinstance(body, list) else body
                    self.assertTrue(pdf_content.startswith(b'%PDF'))
                    self.assertEqual(pdf_content, b"%PDF-1.4\nmongo combined content")
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])
        



                         
if __name__ == '__main__':
    test.main()
        
        
