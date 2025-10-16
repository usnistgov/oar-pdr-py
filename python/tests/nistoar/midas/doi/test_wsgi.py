import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
from pathlib import Path
import unittest as test
from unittest.mock import Mock

from nistoar.midas.doi import wsgi as doim
from nistoar.doi.resolving import DOIInfo, DOIDoesNotExist, DOIResolverError
from nistoar.nerdm.convert.doi import DOIResolver
import nistoar.doi.resolving.common as res

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

sampleref = {
  "@id": "doi:10.10/XXX",
  "@type": [
    "schema:Article"
  ],
  "refType": "IsCitedBy",
  "title": "Ecological traits of the world\\u2019s primates",
  "issued": "2019-05-13",
  "location": "https://goober.org/10.10/XXX",
  "citation": "ibid",
  "_extensionSchemas": [
    "https://data.nist.gov/od/dm/nerdm-schema/bib/v0.7#/definitions/DCiteReference"
  ]
}

sampleauths = [
  {
    "@type": "foaf:Person",
    "familyName": "Galen Acedo",
    "givenName": "Carmen",
    "fn": "Carmen Galen Acedo"
  },
  {
    "@type": "foaf:Person",
    "familyName": "Arroyo",
    "givenName": "Victor",
    "fn": "Victor Arroyo",
    "orcid": "0000-0002-0858-0324",
    "affiliation": [
      {
        "title": "The Institute",
        "@type": "schema:affiliation"
      }
    ]
  },
  {
    "@type": "foaf:Person",
    "familyName": "Andresen",
    "givenName": "Ellen",
    "fn": "Ellen Andresen"
  },
  {
    "@type": "foaf:Person",
    "familyName": "Arasa-Gisbert",
    "givenName": "Ricard",
    "fn": "Ricard Arasa-Gisbert"
  }
]

class TestDOI2NERDmHandler(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        res._client_info = None
        self.cfg = {
            "doi_resolver": {
                "app_name": "NIST Public Data Repository: pubserver (oar-pdr)",
                "app_version": "1.5+",
                "app_url": "https://data.nist.gov/",
                "email": "datasupport@nist.gov"
            }
        }
        self.resolver = DOIResolver.from_config(self.cfg['doi_resolver'])
        self.resolver.to_reference = Mock(return_value=sampleref)
        self.resolver.to_authors = Mock(return_value=sampleauths)
        self.resp = []
        
    def tearDown(self):
        res._client_info = None

    def test_send_reference(self):
        doi = "10.88888/goober"
        path = f"ref/{doi}/"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.send_reference(doi)
        self.assertIn("200 ", self.resp[0])
        ref = self.body2data(body)
        self.assertEqual(ref['@type'], ['schema:Article'])
        self.assertEqual(ref['@id'], 'doi:10.10/XXX')
        self.assertEqual(ref['refType'], 'IsCitedBy')
        self.assertEqual(ref['title'],
                         "Ecological traits of the world\\u2019s primates")
        self.assertEqual(ref['location'], "https://goober.org/10.10/XXX")
        self.assertEqual(ref['issued'], '2019-05-13')
        self.assertEqual(ref['citation'], 'ibid')

        def donotfind(*args, **kw):
            raise DOIDoesNotExist(doi)
        self.resolver.to_reference = Mock(side_effect=donotfind)
        self.resp = []
        body = hdlr.send_reference(doi)
        self.assertIn("200 ", self.resp[0])
        ref = self.body2data(body)
        self.assertEqual(ref['@id'], 'doi:10.88888/goober')
        self.assertNotIn("@type", ref)
        self.assertNotIn("title", ref)
        self.assertNotIn("location", ref)

        self.resolver.to_reference = Mock(side_effect=DOIResolverError)
        with self.assertRaises(DOIResolverError):
            hdlr.send_reference(doi)

    def test_do_GET_ref(self):
        doi = "10.88888/goober"
        path = "ref/"+doi
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ref = self.body2data(body)
        self.assertEqual(ref['@type'], ['schema:Article'])
        self.assertEqual(ref['@id'], 'doi:10.10/XXX')
        self.assertEqual(ref['refType'], 'IsCitedBy')
        self.assertEqual(ref['title'],
                         "Ecological traits of the world\\u2019s primates")
        self.assertEqual(ref['location'], "https://goober.org/10.10/XXX")
        self.assertEqual(ref['issued'], '2019-05-13')
        self.assertEqual(ref['citation'], 'ibid')

        self.resp = []
        path = "ref/doi:"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ref2 = self.body2data(body)
        self.assertEqual(ref2, ref)

        self.resp = []
        path = "ref/https://doi.org/"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ref2 = self.body2data(body)
        self.assertEqual(ref2, ref)

        self.resp = []
        path = "ref/http://dx.doi.org/"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        ref2 = self.body2data(body)
        self.assertEqual(ref2, ref)

        self.resp = []
        path = "ref/ark:/18434/mds2-5801"
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("400 ", self.resp[0])
        err = self.body2data(body)
        self.assertIn('oar:message', err)

        def failure(*args, **kw):
            raise DOIResolverError()
        self.resp = []
        self.resolver.to_reference = Mock(side_effect=failure)
        path = "ref/http://dx.doi.org/"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("503 ", self.resp[0])
        err = self.body2data(body)
        self.assertIn('oar:message', err)

    def test_send_authors(self):
        doi = "10.88888/goober"
        path = f"authors/{doi}/"
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.send_authors(doi)
        self.assertIn("200 ", self.resp[0])
        auths = self.body2data(body)
        self.assertEqual(len(auths), 4)
        self.assertIn('fn', auths[0])

        def donotfind(*args, **kw):
            raise DOIDoesNotExist(doi)
        self.resolver.to_authors = Mock(side_effect=donotfind)
        self.resp = []
        with self.assertRaises(DOIDoesNotExist):
            hdlr.send_authors(doi)

    def test_do_GET_authors(self):
        doi = "10.88888/goober"
        path = "authors/"+doi
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        auths = self.body2data(body)
        self.assertEqual(len(auths), 4)
        self.assertIn('fn', auths[0])

        self.resp = []
        path = "authors/doi:"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        auths2 = self.body2data(body)
        self.assertEqual(auths, auths2)

        def doesnotexist(*args, **kw):
            raise DOIDoesNotExist(doi)
        self.resp = []
        self.resolver.to_authors = Mock(side_effect=doesnotexist)
        path = "authors/https://doi.org/"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        err = self.body2data(body)
        self.assertIn('oar:message', err)

        def failure(*args, **kw):
            raise DOIResolverError()
        self.resp = []
        self.resolver.to_authors = Mock(side_effect=failure)
        path = "authors/http://dx.doi.org/"+doi
        hdlr = doim.DOI2NERDmHandler(self.resolver, path, req, self.start, log=rootlog)
        body = hdlr.handle()
        self.assertIn("503 ", self.resp[0])
        err = self.body2data(body)
        self.assertIn('oar:message', err)

class TestDOI2NERDmApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        res._client_info = None
        self.cfg = {
            "doi_resolver": {
                "app_name": "NIST Public Data Repository: pubserver (oar-pdr)",
                "app_version": "1.5+",
                "app_url": "https://data.nist.gov/",
                "email": "datasupport@nist.gov"
            }
        }
        self.app = doim.DOI2NERDmApp(rootlog, self.cfg)
        self.resp = []

    def test_ctor(self):
        self.assertTrue(self.app._doires)

    def test_create_handler(self):
        doi = "10.88888/goober"
        path = "authors/"+doi
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        hdlr = self.app.create_handler(req, self.start, path)
        self.assertTrue(isinstance(hdlr, doim.DOI2NERDmHandler))

    def test_GET_ref_baddoi(self):
        doi = "ark:/18434/mds2-2525"
        path = "ref/"+doi
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])

    @test.skipIf("doi" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "kindly skipping doi service checks")
    def test_GET_ref(self):
        doi = "doi:10.18434/mds2-2525"
        path = "ref/"+doi
        req = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        resp = self.body2data(body)
        self.assertEqual(resp['@id'], "doi:10.18434/mds2-2525")
        self.assertIn('title', resp)
        self.assertIn('citation', resp)


        
        
        


        
                         
if __name__ == '__main__':
    test.main()
