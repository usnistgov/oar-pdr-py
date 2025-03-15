import os, json, pdb, logging, tempfile, re, shutil
from pathlib import Path
import unittest as test
from unittest import mock
from io import StringIO
import yaml

from nistoar.nsd import sync
from nistoar.base.config import ConfigurationException

datadir = Path(__file__).parents[0] / "data"
tmpdir = tempfile.TemporaryDirectory(prefix="_test_nsd.")
outdir = os.path.join(tmpdir.name, "data")

config = {
    "dir": outdir,
    "person_file": "people.json",
    "org_file": "orgs.json",
    "source": {
        "service_endpoint": "https://nsd.example.com/",
        "token": "goober",
        "tokenService": {
            "service_endpoint": "https://auth.example.com/oauth2/v1/token",
            "client_id": "XXXX",
            "secret": "YYX"
        }
    }
}

def tearDownModule():
    tmpdir.cleanup()

with open(datadir/"orgs.json") as fd:
    orgs = json.load(fd)
with open(datadir/"person.json") as fd:
    people = json.load(fd)

# This method will be used by the mock to replace requests.get
# Thanks to Johannes Fahrenkrug for the code this function is based on
# (https://stackoverflow.com/questions/15753390/how-can-i-mock-requests-and-the-response)
class MockResponse:
    def __init__(self, json_data, status_code, reason):
        self.json_data = json_data
        self.status_code = status_code
        self.reason = reason

    def json(self):
        return self.json_data

def mocked_requests_get(*args, **kwargs):
    global config
    if args[0].endswith(sync.ORGANIZATIONS):
        return MockResponse(orgs, 200, "OK")
    elif args[0].endswith(sync.PEOPLE):
        return MockResponse({}, 405, "Method Not Allowed")
    elif args[0] == config['source']['tokenService']['service_endpoint']:
        return MockResponse(orgs, 405, "Method Not Allowed")

    return MockResponse(None, 404)

def mocked_requests_post(*args, **kwargs):
    global config
    if args[0] == config['source']['tokenService']['service_endpoint']:
        return MockResponse({"access_token": "AXSSTKN1234567890"}, 200, "OK")
    elif args[0].endswith(sync.PEOPLE):
        try:
            if not kwargs['data']:
                return MockResponse({}, 400, "Bad Input")
            data = json.loads(kwargs['data'].decode('utf-8'))
            if sync.PEOP_OUORGID_SEL in data:
                out = [p for p in people if p["ouOrgID"] in data[sync.PEOP_OUORGID_SEL]]
                out = {
                    "totalCount": len(out),
                    "userInfo": out
                }
                return MockResponse(out, 200, "OK")
        except Exception as ex:
            return MockResponse({}, 400, str(ex))
    elif args[0].endswith(sync.ORGANIZATIONS):
        return MockResponse({}, 405, "Method Not Allowed")
            
    return MockResponse(None, 404, "Not Found")


class TestNSDSyncer(test.TestCase):

    def setUp(self):
        if not os.path.isdir(outdir):
            os.mkdir(outdir)
        self.syncer = sync.NSDSyncer(config)

    def tearDown(self):
        if os.path.isdir(outdir):
            shutil.rmtree(outdir)

    def test_ctor(self):
        self.assertIn("source", self.syncer.cfg)
        self.assertIn("tokenService", self.syncer.cfg.get('source',{}))

    @mock.patch('nistoar.nsd.sync.requests.post', side_effect=mocked_requests_post)
    def test_get_token(self, post):
        token = self.syncer.get_token()
        self.assertTrue(token)
        self.assertGreater(len(token), 10)

    @mock.patch('nistoar.nsd.sync.requests.get', side_effect=mocked_requests_get)
    def test_nsd_orgs(self, get):
        scfg = self.syncer.cfg['source']
        data = sync.get_nsd_orgs(scfg['service_endpoint'], self.syncer.token)
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), 2)
        self.assertIn("orG_CD", data[0])
        self.assertIn("orG_Name", data[0])

    @mock.patch('nistoar.nsd.sync.requests.post', side_effect=mocked_requests_post)
    def test_get_people_page(self, post):
        scfg = self.syncer.cfg['source']
        url = scfg['service_endpoint']
        if not url.endswith('/'):
            url += '/'
        url += sync.PEOPLE
        
        data = sync._get_nsd_people_page(url, 3, 1, self.syncer.token)
        self.assertIn("userInfo", data)
        self.assertEqual(data['totalCount'], 4)

        data = data['userInfo']
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 4)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

    @mock.patch('nistoar.nsd.sync.requests.post', side_effect=mocked_requests_post)
    def test_write_nsd_ou_people(self, post):
        scfg = self.syncer.cfg['source']
        url = scfg['service_endpoint']
        if not url.endswith('/'):
            url += '/'
        url += sync.PEOPLE

        data = []
        sync._write_nsd_ou_people(data, url, 3, self.syncer.token)  # OU=LP
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 4)
        self.assertLess(len(data), sync.PEOP_PAGE_SZ+1)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

        data = []
        sync._write_nsd_ou_people(data, url, 1, self.syncer.token)
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 0)

    @mock.patch('nistoar.nsd.sync.requests.post', side_effect=mocked_requests_post)
    def test_get_nsd_people(self, post):
        scfg = self.syncer.cfg['source']
        data = sync.get_nsd_people(scfg['service_endpoint'], [3], self.syncer.token)
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 4)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

    def test_cache_data(self):
        ofile = os.path.join(outdir, "orgs.json")
        self.assertFalse(os.path.exists(ofile))
        pfile = os.path.join(outdir, "people.json")
        self.assertFalse(os.path.exists(pfile))

        with mock.patch('nistoar.nsd.sync.requests.post') as pmock:
            pmock.side_effect = mocked_requests_post
            with mock.patch('nistoar.nsd.sync.requests.get') as gmock:
                gmock.side_effect = mocked_requests_get

                self.syncer.cache_data()
                self.assertTrue(os.path.isfile(ofile))
                self.assertTrue(os.path.isfile(pfile))
                with open(ofile) as fd:
                    data = json.load(fd)
                self.assertEqual(len(data), 8)
                with open(pfile) as fd:
                    data = json.load(fd)
                self.assertEqual(len(data), 4)
                                
        
        
        
                         
if __name__ == '__main__':
    test.main()
