import json
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from logging import Logger
from copy import deepcopy
import requests

from nistoar.midas.dap.fm.clients.nextcloud import NextcloudApi
from nistoar.midas.dap.fm.exceptions import *
from nistoar.base.config import ConfigurationException

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'

STATUS_TESTER_URL = None
try:
    r = requests.get("https://httpstat.us/201")
    if r.status_code == 201:
        STATUS_TESTER_URL = "https://httpstat.us/"
except Exception:
    pass

class NextcloudApiTest(test.TestCase):

    def setUp(self):
        self.config = {
            'service_endpoint': 'http://mockservice/api',
            'authentication': {
                'client_cert_path': certpath,
                'client_key_path':  keypath
            }
        }

        self.cli = NextcloudApi(self.config)

    def test_get_cert_cn(self):
        cn = self.cli._get_cert_cn(self.config['authentication']['client_cert_path'])
        self.assertEqual(cn, 'oar_api')

    def test_prep_auth(self):
        kw = self.cli._prep_auth(self.config['authentication'])
        self.assertEqual(kw['cert'], (certpath, keypath))
        self.assertNotIn('auth', kw)

        cfg = deepcopy(self.config['authentication'])
        cfg['user'] = 'oar_api'
        kw = self.cli._prep_auth(cfg)
        self.assertEqual(kw['cert'], (certpath, keypath))

        cfg['user'] = 'TestUser'  # wrong user name
        with self.assertRaises(ConfigurationException):
            self.cli._prep_auth(cfg)

        cfg = deepcopy(self.config['authentication'])
        cfg['client_key_path'] = Path("/tmp/doesnotexist")
        with self.assertRaises(ConfigurationException):
            self.cli._prep_auth(cfg)

        cfg = { 'user': 'TestUser', 'pass': 'password1234' }
        kw = self.cli._prep_auth(cfg)
        self.assertEqual(kw['auth'], ('TestUser', 'password1234'))
        self.assertNotIn('cert', kw)

    def test_ctor(self):
        self.assertTrue(isinstance(self.cli.log, Logger))
        self.assertEqual(self.cli.base_url, self.config['service_endpoint']+'/')
        self.assertTrue(isinstance(self.cli.authkw, dict))
        self.assertEqual(self.cli.authkw['cert'], (certpath, keypath))
        self.assertNotIn('verify', self.cli.authkw)

        self.config['ca_bundle'] = capath
        self.cli = NextcloudApi(self.config)
        self.assertEqual(self.cli.base_url, self.config['service_endpoint']+'/')
        self.assertTrue(isinstance(self.cli.authkw, dict))
        self.assertEqual(self.cli.authkw['cert'], (certpath, keypath))
        self.assertEqual(self.cli.authkw['verify'], capath)

    @patch('requests.request')
    def test_test(self, mock_request):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_request.return_value = mock_resp

        res = self.cli.test()
        self.assertEqual(res, mock_resp)
        mock_request.assert_called_with("GET", self.config['service_endpoint']+"/test",
                                        cert=(certpath, keypath))

#    def test_test_notfound(self):
#        self.cli.base_url = "https://data.nist.gov"
#        res = self.cli.headers()

    @patch('requests.request')
    def test_headers(self, mock_request):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "okay"}'
        def mock_json():
            return json.loads(mock_resp.text)
        mock_resp.json = mock_json
        mock_request.return_value = mock_resp

        res = self.cli.headers()
        self.assertEqual(res, {"status": "okay"})
        mock_request.assert_called_with("GET", self.config['service_endpoint']+"/headers",
                                        cert=(certpath, keypath))


    @test.skipIf(not STATUS_TESTER_URL, "Network unavailable")
    def test_check_status_code(self):
        self.cli.base_url = STATUS_TESTER_URL

        r = self.cli._handle_request("GET", "200")
        self.assertEqual(r.status_code, 200)

        with self.assertRaises(FileManagerResourceNotFound):
            self.cli._handle_request("GET", "404")
        with self.assertRaises(FileManagerServerError):
            self.cli._handle_request("GET", "500")
        with self.assertRaises(FileManagerClientError):
            self.cli._handle_request("GET", "400")
        with self.assertRaises(FileManagerClientError):
            self.cli._handle_request("GET", "401")

        # requests will redirect automatically
        #
        # with self.assertRaises(UnexpectedFileManagerResponse):
        #     self.cli._handle_request("GET", "301")

                        



        


if __name__ == '__main__':
    test.main()
