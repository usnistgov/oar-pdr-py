import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test
import yaml

from nistoar.nsd.sync import syncer as sync
from nistoar.base.config import ConfigurationException

configfile = None
if os.environ.get('DBIO_CONFIG_FILE'):
    configfile = os.environ.get('DBIO_CONFIG_FILE')
    assert configfile
    assert os.path.isfile(configfile)

config = None
if configfile:
    with open(configfile) as fd:
        config = yaml.safe_load(fd)
    if config.get("services",{}).get("nsd"):
        config = config["services"]["nsd"]
    if config.get("data"):
        config = config["data"]

nsdtoken=None

@test.skipIf(not config, "NSD service not available")
class TestNSDSyncer(test.TestCase):

    def setUp(self):
        self.syncer = sync.NSDSyncer(config)

    @classmethod
    def tearDownClass(cls):
        global nsdtoken
        nsdtoken = None

    def test_ctor(self):
        self.assertIn("source", self.syncer.cfg)
        self.assertIn("tokenService", self.syncer.cfg.get('source',{}))

    def test_get_token(self):
        global nsdtoken
        token = self.syncer.get_token()
        if not nsdtoken:
            nsdtoken = token

        self.assertTrue(token)
        self.assertGreater(len(token), 10)

    def test_nsd_orgs(self):
        global nsdtoken
        if not nsdtoken:
            nsdtoken = self.syncer.get_token()
        
        scfg = self.syncer.cfg['source']
        data = sync.get_nsd_orgs(scfg['service_endpoint'], nsdtoken)
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), 10)
        self.assertIn("orG_CD", data[0])
        self.assertIn("orG_Name", data[0])

    def test_get_people_page(self):
        global nsdtoken
        if not nsdtoken:
            nsdtoken = self.syncer.get_token()

        scfg = self.syncer.cfg['source']
        url = scfg['service_endpoint']
        if not url.endswith('/'):
            url += '/'
        url += sync.PEOPLE
        
        data = sync._get_nsd_people_page(url, 13213, 2, nsdtoken)  # OU=MML
        self.assertIn("userInfo", data)
        self.assertGreater(data['totalCount'], 50)

        data = data['userInfo']
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), 50)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

    def test_nsd_ou_people(self):
        global nsdtoken
        if not nsdtoken:
            nsdtoken = self.syncer.get_token()

        scfg = self.syncer.cfg['source']
        url = scfg['service_endpoint']
        if not url.endswith('/'):
            url += '/'
        url += sync.PEOPLE

        data = []
        sync._write_nsd_ou_people(data, url, 13210, nsdtoken)  # OU=LP
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), 50)
        self.assertLess(len(data), sync.PEOP_PAGE_SZ+1)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

        data = []
        sync._write_nsd_ou_people(data, url, 13216, nsdtoken)  # OU=ITL
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), sync.PEOP_PAGE_SZ)
        self.assertLess(len(data), 1000)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])
        self.assertIn("lastName", data[-1])
        self.assertIn("firstName", data[-1])

        

    def test_nsd_people(self):
        global nsdtoken
        if not nsdtoken:
            nsdtoken = self.syncer.get_token()
        
        scfg = self.syncer.cfg['source']
        data = sync.get_nsd_people(scfg['service_endpoint'], [13210, 13212, 13213], nsdtoken)
        self.assertTrue(isinstance(data, list))
        self.assertGreater(len(data), 50)
        self.assertIn("lastName", data[0])
        self.assertIn("firstName", data[0])

        
        
        
        
                         
if __name__ == '__main__':
    test.main()
