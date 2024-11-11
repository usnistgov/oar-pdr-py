import json
import unittest as test
from unittest.mock import patch, Mock, create_autospec
from pathlib import Path
from copy import deepcopy
import requests
from lxml import etree

from nistoar.midas.dap.fm.clients import webdav as fmwd
from nistoar.base.config import ConfigurationException

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'
pfrespfile = datadir / "webdav-propfind.xml"

class FMWebDAVClientTest(test.TestCase):

    def setUp(self):
        self.config = {
            'service_endpoint': 'https://goober.net/remote.php/dav/files/oar_api',
            'authentication': {
                'user': 'oar_api',
                'pass': 'goober'
            }
        }

        self.cli = fmwd.FMWebDAVClient(self.config)

    def test_ctor(self):
        # test from ctor
        self.assertEqual(self.cli._wdcopts['webdav_hostname'], "https://goober.net")
        self.assertEqual(self.cli._wdcopts['webdav_root'], "/remote.php/dav/files/oar_api")
        self.assertEqual(self.cli._wdcopts['webdav_login'], "oar_api")
        self.assertEqual(self.cli._wdcopts['webdav_password'], "goober")

        self.assertIsNotNone(self.cli.wdcli)

    def test_extract_cert_cn(self):
        self.assertEqual(fmwd.extract_cert_cn(certpath), "oar_api")

    def test_add_auth_opts(self):
        opts = {}
        authcfg = {
            'client_cert_path': certpath,
            'client_key_path':  keypath
        }

        self.cli._add_auth_opts(authcfg, opts)
        self.assertEqual(opts['webdav_login'], "oar_api")
        self.assertIsNone(opts['webdav_password'])

        opts = {}
        authcfg['user'] = "oar_api"
        self.cli._add_auth_opts(authcfg, opts)
        self.assertEqual(opts['webdav_login'], "oar_api")
        self.assertIsNone(opts['webdav_password'])

        authcfg['user'] = "TestId"
        with self.assertRaises(ConfigurationException):
            self.cli._add_auth_opts(authcfg, opts)

    @patch('requests.post')
    def test_get_webdav_password(self, mock_request):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = '{"temporary_password": "goober"}'
        def mock_json():
            return json.loads(mock_resp.text)
        mock_resp.json = mock_json
        mock_request.return_value = mock_resp

        self.assertEqual(fmwd.get_webdav_password("http://mockservice/auth", certpath, keypath), "goober")

    @patch('requests.post')
    def test_authenticate(self, mock_request):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = '{"temporary_password": "goober"}'
        def mock_json():
            return json.loads(mock_resp.text)
        mock_resp.json = mock_json
        mock_request.return_value = mock_resp

        config = {
            'service_endpoint': 'http://mockservice/api',
            'authentication': {
                'client_cert_path': certpath,
                'client_key_path':  keypath
            }
        }
        self.cli = fmwd.FMWebDAVClient(config)
        self.assertIsNone(self.cli.wdcli)
        self.cli.authenticate()
        self.assertIsNotNone(self.cli.wdcli)
        self.assertEqual(self.cli._wdcopts.get('webdav_login'), 'oar_api')
        self.assertEqual(self.cli._wdcopts.get('webdav_password'), 'goober')

    def test_exists(self):
        with self.assertRaises(fmwd.FileManagerCommError):
            self.cli.exists("mds3-0000")
            
        self.cli.wdcli.check = create_autospec(self.cli.wdcli.check, return_value=True)
        self.assertTrue(self.cli.exists("mds3-0000"))

    def test_propfind_resp_to_dict(self):
        xmlmsg = etree.parse(pfrespfile)
        respel = xmlmsg.getroot()[0]

        props = fmwd.propfind_resp_to_dict(respel)
        self.assertEqual(props.get('type'), "folder")
        self.assertEqual(props.get('fileid'), "192")
        self.assertEqual(props.get('size'), "4997166")
        self.assertEqual(props.get('permissions'), "RGDNVCK")
        self.assertIn("created", props)
        self.assertIn("modified", props)

    def test_has_info_request(self):
        self.assertIn("propfind", fmwd.info_request)

    def test_parse_propfind(self):
        with open(pfrespfile) as fd:
            xmlstr = fd.read()
        path = "mds3-0012/mds3-0012"
        baseurl = "https://goober.net/remote.php/dav/files/oar_api"

        props = fmwd.parse_propfind(xmlstr, path, baseurl)
        self.assertEqual(props.get('type'), "folder")
        self.assertEqual(props.get('fileid'), "192")
        self.assertEqual(props.get('size'), "4997166")
        self.assertEqual(props.get('permissions'), "RGDNVCK")
        self.assertIn("created", props)
        self.assertIn("modified", props)
        
    def test_get_resource_info(self):
        with open(pfrespfile) as fd:
            xmlstr = fd.read()
        resp = Mock()
        resp.text = xmlstr
        resp.status_code = 200
        self.cli.wdcli.execute_request = create_autospec(self.cli.wdcli.execute_request, return_value=resp)

        props = self.cli.get_resource_info("mds3-0012/mds3-0012")
        self.assertEqual(props.get('type'), "folder")
        self.assertEqual(props.get('fileid'), "192")
        self.assertEqual(props.get('size'), "4997166")
        self.assertEqual(props.get('permissions'), "RGDNVCK")
        self.assertIn("created", props)
        self.assertIn("modified", props)
        


        


if __name__ == '__main__':
    test.main()


