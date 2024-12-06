import os, json, pdb, logging, tempfile, pathlib
import unittest as test
from io import StringIO
from copy import deepcopy
from collections import OrderedDict

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate, ObjectNotFound, PartNotAccessible
from nistoar.midas.dbio.wsgi import project as prj
from nistoar.midas.dap.service import mds3
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.pdr.utils import read_nerd, prov
from nistoar.nerdm.constants import CORE_SCHEMA_URI

tmpdir = tempfile.TemporaryDirectory(prefix="_test_mds3.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_mds3.log"))
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

# test records
testdir = pathlib.Path(__file__).parents[0]
pdr2210 = testdir.parents[2] / 'pdr' / 'describe' / 'data' / 'pdr2210.json'
ncnrexp0 = testdir.parents[2] / 'pdr' / 'publish' / 'data' / 'ncnrexp0.json'

class TestMDS3DAPApp(test.TestCase):

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
                    "default_shoulder": "mds3"
                },
                "default": {
                    "default_shoulder": "mds3"
                }
            },
            "dbio": {
                "superusers": [ "rlp" ],
                "allowed_project_shoulders": ["mds3", "pdr1"],
                "default_shoulder": "mds3"
            },
            "assign_doi": "always",
            "doi_naan": "10.88888",
            "nerdstorage": {
#                "type": "fsbased",
#                "store_dir": os.path.join(tmpdir.name)
                "type": "inmem",
            }
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mds3": 0 }})
        self.nerdstore = InMemoryResourceStorage(self.cfg["nerdstorage"])
        self.svcfact = mds3.DAPServiceFactory(self.dbfact, self.cfg, rootlog.getChild("midas.dap"),
                                              self.nerdstore)
        self.app = mds3.DAPApp(self.dbfact, rootlog.getChild("midas"), self.cfg, self.svcfact)
        self.resp = []
        self.rootpath = "/midas/dap/mds3"

    def create_record(self, name="goob", meta=None):
        cli = self.dbfact.create_client(base.DAP_PROJECTS, self.cfg["dbio"], nistr.actor)
        out = cli.create_record(name, "pdr1")
        if meta:
            out.meta = meta
            out.save()
        return out

    def sudb(self):
        return self.dbfact.create_client(base.DAP_PROJECTS, self.cfg["dbio"], "rlp")

    def test_create_handler_name(self):
        path = "mdm1:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectNameHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "name")
        self.assertEqual(hdlr._id, "mdm1:0001")

    def test_get_name(self):
        path = "pdr1:0001/name"
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
        path = "mds3:0001/name"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_create(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"data": { "contactPoint": {"fn": "Gurn Cranston"} },
                                                 "meta": { "resType": "Software" }}))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])  # input was missing name

        # TODO: this will succeed after we define the Software extension schema
        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"meta": { "resourceType": "Software",
                                                           "creatorisContact": "false",
                                                           "softwareLink": "https://sw.ex/gurn" },
                                                 "name": "Gurn's Opus" }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "Gurn's Opus")
        self.assertEqual(resp['id'], "mds3:0001")
        self.assertEqual(resp['meta']["resourceType"], "Software")
        self.assertEqual(resp['meta']["softwareLink"], "https://sw.ex/gurn")
        self.assertIs(resp['meta']["creatorisContact"], False)
        self.assertEqual(resp['data']['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['data']['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['data']['@type'],
                         [ "nrdw:SoftwarePublication", "nrdp:PublicDataResource", "dcat:Resource" ])


        self.resp = []
        req['wsgi.input'] = StringIO(json.dumps({"data": { "contactPoint": {"fn": "Gurn Cranston"},
                                                           "keyword": [ "testing" ] },
                                                 "meta": { "creatorisContact": "false",
                                                           "softwareLink": "https://sw.ex/gurn" },
                                                 "name": "Gurn's Penultimate" }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()

        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "Gurn's Penultimate")
        self.assertEqual(resp['id'], "mds3:0002")
        self.assertEqual(resp['meta']["resourceType"], "data")
        self.assertEqual(resp['meta']["softwareLink"], "https://sw.ex/gurn")
        self.assertIs(resp['meta']["creatorisContact"], False)
        self.assertEqual(resp['data']['@id'], 'ark:/88434/mds3-0002')
        self.assertEqual(resp['data']['doi'], 'doi:10.88888/mds3-0002')
        self.assertIn('authors', resp['data'])
        self.assertIn('contactPoint', resp['data'])  # this is included in ['data'] summary

        self.resp = []
        path = resp['id'] + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0002')
        self.assertEqual(resp['doi'], 'doi:10.88888/mds3-0002')
        self.assertEqual(resp['contactPoint'],
                         {"fn": "Gurn Cranston", "@type": "vcard:Contact"})
        self.assertEqual(resp['@type'],
                         [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertIn('_schema', resp)
        self.assertIn('_extensionSchemas', resp)
        self.assertEqual(len(resp.get('components',[])), 1)
        self.assertEqual(resp['components'][0]['accessURL'], "https://sw.ex/gurn")
        self.assertEqual(len(resp), 9)

    def test_put_patch(self):
        testnerd = read_nerd(pdr2210)
        res = deepcopy(testnerd)
        del res['references']
        del res['components']
        del res['@id']
        del res['_schema']
        del res['_extensionSchemas']

        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"data": { "contactPoint": res['contactPoint'],
                                                           "keyword": [ "testing" ] },
                                                 "meta": { "creatorisContact": "false" },
                                                 "name": "OptSortSph" }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()

        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "OptSortSph")
        self.assertEqual(resp['id'], "mds3:0001")
        self.assertEqual(resp['meta']["resourceType"], "data")
        self.assertIs(resp['meta']["creatorisContact"], False)
        self.assertEqual(resp['data']['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['data']['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['data']['keywords'], ['testing'])
        self.assertIn('authors', resp['data'])
        self.assertIn('contactPoint', resp['data'])  # this is included in ['data'] summary
        
        self.resp = []
        id = resp['id']
        path = id + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['contactPoint'],
                         {"fn": "Zachary Levine", "@type": "vcard:Contact",
                          "hasEmail": "mailto:zachary.levine@nist.gov"      })
        self.assertEqual(resp['@type'],
                         [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertIn('_schema', resp)
        self.assertIn('_extensionSchemas', resp)
        self.assertEqual(resp['keyword'], ['testing'])
        self.assertNotIn('components', resp)
        self.assertNotIn('authors', resp)
        self.assertNotIn('description', resp)
        self.assertNotIn('rights', resp)
        self.assertEqual(len(resp), 8)

        self.resp = []
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps(res))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['contactPoint'],
                         {"fn": "Zachary Levine", "@type": "vcard:Contact",
                          "hasEmail": "mailto:zachary.levine@nist.gov"      })
        self.assertEqual(len(resp['description']), 1)
        self.assertNotIn('references', resp)
        self.assertNotIn('authors', resp)
        self.assertIn('description', resp)

        self.resp = []
        path = id + '/data/authors'
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({
            "fn": "Levine, Zachary",
            "givenName": "Zachary",
            "familyName": "Levine",
            "affiliation": "NIST"
        }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp["givenName"], "Zachary")
        self.assertEqual(len(resp["affiliation"]), 1)
        self.assertIn("@id", resp)

        files = [c for c in testnerd['components'] if 'filepath' in c]
        self.resp = []
        path = id + '/data/components'
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps(files))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertTrue(isinstance(resp, list))
        self.assertEqual(len(resp), len(files))

        links = [c for c in testnerd['components'] if 'filepath' not in c]
        self.assertEqual(len(links), 1)
        self.resp = []
        path = id + '/data/components'
        req = {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps(links))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertTrue(isinstance(resp, list))
        self.assertEqual(len(resp), len(files)+len(links))
        self.assertEqual(resp[0]['accessURL'], "https://doi.org/10.18434/T4SW26")

        self.resp = []
        path = id + '/data/pdr:f'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertTrue(isinstance(resp, list))
        self.assertEqual(len(resp), len(files))
        self.assertTrue(all('filepath' in c for c in resp))

        self.resp = []
        path = id + '/data/pdr:f/trial3'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['filepath'], "trial3")
        self.assertEqual(resp['@id'], "coll_2")
        self.assertNotIn("downloadURL", resp)

        self.resp = []
        path = id + '/data/components/file_1'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['filepath'], "trial2.json")
        self.assertEqual(resp['@id'], "file_1")
        
        self.resp = []
        path = id + '/data/doi'
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("doi:10.88888/haha"))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

        self.resp = []
        path = id + '/data/rights'
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps("What ever."))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp, "What ever.")

        self.resp = []
        path = id + '/data/pdr:r'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['rights'], 'What ever.')
        self.assertIn('_schema', resp)
        self.assertIn('_extensionSchemas', resp)
        self.assertNotIn('components', resp)
        self.assertNotIn('authors', resp)
        self.assertIn('description', resp)
        self.assertEqual(resp['rights'], "What ever.")

    def test_delete(self):
        testnerd = read_nerd(pdr2210)
        res = deepcopy(testnerd)
        del res['references']
        del res['components']
        del res['@id']
        del res['_schema']
        del res['_extensionSchemas']

        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"data": { "contactPoint": res['contactPoint'],
                                                           "keyword": [ "testing" ],
                                                           "landingPage": "https://example.com/" },
                                                 "meta": { "creatorisContact": "false" },
                                                 "name": "OptSortSph" }))
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectSelectionHandler))
        self.assertNotEqual(hdlr.cfg, {})
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()

        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "OptSortSph")
        self.assertEqual(resp['id'], "mds3:0001")
        self.assertEqual(resp['data']['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['data']['doi'], 'doi:10.88888/mds3-0001')
        recid = resp['id']
        
        self.resp = []
        path = recid + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.88888/mds3-0001')
        self.assertEqual(resp['contactPoint'],
                         {"fn": "Zachary Levine", "@type": "vcard:Contact",
                          "hasEmail": "mailto:zachary.levine@nist.gov"      })
        self.assertEqual(resp['@type'],
                         [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertEqual(resp['landingPage'], "https://example.com/")
        self.assertEqual(resp['keyword'], ["testing"])
        
        self.resp = []
        path = recid + '/data/landingPage'
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertEqual(hdlr._path, "landingPage")
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertIs(resp, True)

        
        self.resp = []
        path = recid + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, prj.ProjectDataHandler))
        self.assertEqual(hdlr._path, "")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertNotIn("landingPage", resp)

    def test_get_fs_info(self):
        path = "pdr1:0001/file_space"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, nistr)
        self.assertTrue(isinstance(hdlr, mds3.DAPProjectInfoHandler))
        prec = self.create_record("goob")
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertIn('id', resp)
        self.assertIn('action', resp)
        self.assertEqual(resp['action'], '')
        self.assertNotIn('file_count', resp)  # file manager not engaged

        
        



        
        

                

        
        

        

        


                         
if __name__ == '__main__':
    test.main()
        
        
