import os, json, pdb, logging, tempfile
from collections import OrderedDict
from pathlib import Path
from io import StringIO
import unittest as test
from unittest.mock import patch, Mock
import preppy


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
            "dbio": {
                "superusers": [ "rlp" ],
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mdm1",
                        "public": "mdm0"
                    },
                    "allowed_shoulders": {
                        "midas": ["mdm1"]
                    }
                }
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
        cli = self.dbfact.create_client(base.DMP_PROJECTS, self.cfg["dbio"], nistr)
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
        
        for record in results:
            if record['id'] == id1:
                self.assertEqual(record['data']['title'], "First Record")
            elif record['id'] == id2:
                self.assertEqual(record['data']['title'], "Second Record")
        
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
        self.assertEqual(results[0]['data']['title'], "Third Record")
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1},{id2},{id3}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 3)
        
        returned_ids = [r['id'] for r in results]
        self.assertIn(id1, returned_ids)
        self.assertIn(id2, returned_ids)
        self.assertIn(id3, returned_ids)
        
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
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1},nonexistent:0001,{id3}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 2)
        
        returned_ids = [r['id'] for r in results]
        self.assertIn(id1, returned_ids)
        self.assertIn(id3, returned_ids)
        self.assertNotIn("nonexistent:0001", returned_ids)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': 'ids='
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1},{id2}&perm=read'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 2)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1}&ids={id2}&ids={id3}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        
        self.assertIn("200 ", self.resp[0])
        results = self.body2dict(body)
        self.assertEqual(len(results), 3)
        
        self.resp = []
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'QUERY_STRING': f'ids={id1}'
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        
        cors = [h for h in self.resp if h.startswith("Access-Control-Allow-Origin")]
        self.assertGreater(len(cors), 0)
        self.assertTrue(cors[0].startswith("Access-Control-Allow-Origin: *"))

    def test_export_by_ids_single_pdf_bytes(self):
        """Test export endpoint with single record - PDF bytes output"""
        # Create test record
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        
        req['wsgi.input'] = StringIO(json.dumps({
            "name": "PDF Export Record", 
            "data": {"title": "PDF Test Record", "description": "Test record for PDF export"}
        }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        record_id = resp['id']
        
        self.resp = []
        
        with patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF-1.4\ntest pdf content"):
            with patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule") as mock_preppy:
                mock_template = Mock()
                mock_template.get.return_value = "<document>PDF Content</document>"
                mock_preppy.return_value = mock_template
                
                def fake_pdf_concat(results, output_filename):
                    return {
                        "format": "pdf",
                        "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                        "mimetype": "application/pdf",
                        "file_extension": ".pdf",
                        "bytes": b"%PDF-1.4\ntest pdf content",
                    }
                
                with patch.dict('nistoar.midas.export.export.CONCAT_REGISTRY', {"pdf": fake_pdf_concat}):
                    path = ":export"
                    req = {
                        'REQUEST_METHOD': 'GET',
                        'PATH_INFO': self.rootpath + path,
                        'QUERY_STRING': f'ids={record_id}&format=pdf'
                    }
                    
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
                    self.assertEqual(pdf_content, b"%PDF-1.4\ntest pdf content")
                    

    def test_export_by_ids_multiple_pdf_bytes(self):
        """Test export endpoint with multiple records - combined PDF bytes"""
        # Create multiple test records
        record_ids = []
        for i, name in enumerate(["PDF One", "PDF Two"], 1):
            path = ""
            req = {
                'REQUEST_METHOD': 'POST',
                'PATH_INFO': self.rootpath + path
            }
            req['wsgi.input'] = StringIO(json.dumps({
                "name": f"Export {name}", 
                "data": {"title": f"PDF Test {name}", "description": f"Test record {i}"}
            }))
            hdlr = self.app.create_handler(req, self.start, path, nistr)
            body = hdlr.handle()
            self.assertIn("201 ", self.resp[0])
            resp = self.body2dict(body)
            record_ids.append(resp['id'])
            self.resp = []
        
        
        with patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF-1.4\ncombined pdf content"):
            with patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule") as mock_preppy:
                mock_template = Mock()
                mock_template.get.return_value = "<document>Combined PDF Content</document>"
                mock_preppy.return_value = mock_template
                
                
                def fake_pdf_concat(results, output_filename):
                    return {
                        "format": "pdf",
                        "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                        "mimetype": "application/pdf", 
                        "file_extension": ".pdf",
                        "bytes": b"%PDF-1.4\ncombined pdf content",  
                    }
                
                with patch.dict('nistoar.midas.export.export.CONCAT_REGISTRY', {"pdf": fake_pdf_concat}):
                    path = ":export"
                    req = {
                        'REQUEST_METHOD': 'GET',
                        'PATH_INFO': self.rootpath + path,
                        'QUERY_STRING': f'ids={",".join(record_ids)}&format=pdf'  # Multiple records
                    }
                    
                    hdlr = self.app.create_handler(req, self.start, path, nistr)
                    body = hdlr.handle()
                    
                    self.assertIn("200 ", self.resp[0])
                    
                    content_type_header = None
                    for header in self.resp:
                        if header.startswith('Content-Type:'):
                            content_type_header = header
                            break
                    
                    self.assertIsNotNone(content_type_header)
                    self.assertIn('application/pdf', content_type_header)
                    
                    self.assertEqual(mock_template.get.call_count, len(record_ids))
                    
                    pdf_content = b''.join(body) if isinstance(body, list) else body
                    self.assertTrue(pdf_content.startswith(b'%PDF'))
                    self.assertEqual(pdf_content, b"%PDF-1.4\ncombined pdf content")

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
        self.assertEqual(resp['status']['created_by'], "dbio/nstr1")

        # test reassign on creation: via meta
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"name": "else", "owner": "nobody",
                                                 "meta": {"foruser": "harry"}}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "else")
        self.assertEqual(resp['owner'], "harry")
        self.assertEqual(resp['id'], "mdm1:0004")
        self.assertEqual(resp['meta'], {"foruser": "harry", "agent_vehicle": "dbio"})
        self.assertEqual(resp['status']['created_by'], "dbio/nstr1")

        # test reassign on creation: via query parameter
        self.resp = []
        req['QUERY_STRING'] = "foruser=True"
        req['wsgi.input'] = StringIO(json.dumps({"name": "else", "owner": "nobody"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "else")
        self.assertEqual(resp['owner'], "nobody")
        self.assertEqual(resp['id'], "mdm1:0005")
        self.assertEqual(resp['meta'], {"foruser": "nobody", "agent_vehicle": "dbio"})
        self.assertEqual(resp['status']['created_by'], "dbio/nstr1")

        # test reassign on creation: via query parameter
        self.resp = []
        req['QUERY_STRING'] = "foruser=sally"
        req['wsgi.input'] = StringIO(json.dumps({"name": "else", "owner": "nobody"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "else")
        self.assertEqual(resp['owner'], "sally")
        self.assertEqual(resp['id'], "mdm1:0006")
        self.assertEqual(resp['meta'], {"foruser": "sally", "agent_vehicle": "dbio"})
        self.assertEqual(resp['status']['created_by'], "dbio/nstr1")

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
        self.assertIn("200 ", self.resp[0])


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
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        prec = self.create_record()
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

    def test_getupd_aclsperm_bulk(self):
        path = "mdm1:0003/acls"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        prec = self.create_record()
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        acls = self.body2dict(body)
        self.assertEqual(acls.get("read"),   ["nstr1"])
        self.assertEqual(acls.get("write"),  ["nstr1"])
        self.assertEqual(acls.get("admin"),  ["nstr1"])
        self.assertEqual(acls.get("delete"), ["nstr1"])

        self.resp = []
        req['REQUEST_METHOD'] = 'PATCH'
        req['wsgi.input'] = StringIO(json.dumps({"read": ["gary", "hank"], "write": "hank"}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        acls = self.body2dict(body)
        self.assertEqual(acls.get("read"), ["nstr1", "gary", "hank"])
        self.assertEqual(acls.get("write"), ["nstr1", "hank"])
        self.assertEqual(acls.get("admin"), ["nstr1"])
        self.assertEqual(acls.get("delete"), ["nstr1"])

        self.resp = []
        req['REQUEST_METHOD'] = 'PATCH'
        req['wsgi.input'] = StringIO(json.dumps({"read": ["alice"], "write": [], "admin": []}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        acls = self.body2dict(body)
        self.assertEqual(acls.get("read"), ["nstr1", "gary", "hank", "alice"])
        self.assertEqual(acls.get("write"), ["nstr1", "hank"])
        self.assertEqual(acls.get("admin"), ["nstr1"])
        self.assertEqual(acls.get("delete"), ["nstr1"])

        self.resp = []
        req['REQUEST_METHOD'] = 'PUT'
        req['wsgi.input'] = StringIO(json.dumps({"read": ["alice"], "write": [], "admin": []}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        acls = self.body2dict(body)
        self.assertEqual(acls.get("read"), ["nstr1", "alice"])
        self.assertEqual(acls.get("write"), [])
        self.assertEqual(acls.get("admin"), ["nstr1"])
        self.assertEqual(acls.get("delete"), ["nstr1"])



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
        self.assertEqual(self.body2dict(body), ["nstr1", "hank"])  # order changes due to protect_owner

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        del req['wsgi.input']
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "hank"])  # order changes due to protect_owner

        self.resp = []
        req['REQUEST_METHOD'] = 'PATCH'
        req['wsgi.input'] = StringIO(json.dumps(["hank", "gary"]))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "hank", "gary"])

        self.resp = []
        req['REQUEST_METHOD'] = 'GET'
        del req['wsgi.input']
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), ["nstr1", "hank", "gary"])
        
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
        
        self.resp = []
        path = "mdm1:0003/acls/write/:user"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectACLsHandler))
        self.assertEqual(hdlr._path, "write/:user")
        self.assertEqual(hdlr._id, "mdm1:0003")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(self.body2dict(body), True)

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
        
    def test_get_status_todo(self):
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
        self.assertNotIn("todo", resp['status'])

        self.resp = []
        path = "mdm1:0003/status/todo"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "todo")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        self.assertIn("todo", self.resp[0])

        self.resp = []
        path = "mdm1:goob/status/todo"
        req['PATH_INFO'] = self.rootpath + path
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectStatusHandler))
        self.assertEqual(hdlr._path, "todo")
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        self.assertNotIn("todo", self.resp[0])

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

        self.assertEqual(resp['state'], 'published')
        self.assertEqual(resp['action'], 'submit')
        self.assertIn('modified', resp)
        self.assertIn('since', resp)
        self.assertEqual(resp['message'], "I'm done!")

        

                         
if __name__ == '__main__':
    test.main()



