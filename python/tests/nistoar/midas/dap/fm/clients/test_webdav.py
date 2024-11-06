import json
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from copy import deepcopy

from nistoar.midas.dap.fm.clients.webdav import WebDAVApi
from nistoar.base.config import ConfigurationException

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'

class WebDAVApiTest(test.TestCase):

    def setUp(self):
        self.config = {
            'service_endpoint': 'http://mockservice/api',
            'authentication': {
                'client_cert_path': certpath,
                'client_key_path':  keypath
            }
        }

        self.cli = WebDAVApi(self.config)

    def test_prep_auth(self):
        kw = self.cli._prep_auth(self.config['authentication'])
        self.assertEqual(kw['cert'], (certpath, keypath))
        self.assertNotIn('auth', kw)

        cfg = deepcopy(self.config['authentication'])
        cfg['user'] = 'oar_api'
        kw = self.cli._prep_auth(cfg)
        self.assertEqual(kw['cert'], (certpath, keypath))

        cfg = deepcopy(self.config['authentication'])
        cfg['client_key_path'] = Path("/tmp/doesnotexist")
        with self.assertRaises(ConfigurationException):
            self.cli._prep_auth(cfg)

        cfg = { 'user': 'TestUser', 'pass': 'password1234' }
        kw = self.cli._prep_auth(cfg)
        self.assertEqual(kw['auth'], ('TestUser', 'password1234'))
        self.assertNotIn('cert', kw)



        


if __name__ == '__main__':
    test.main()


