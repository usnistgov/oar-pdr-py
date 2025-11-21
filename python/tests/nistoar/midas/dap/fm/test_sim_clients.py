import json, tempfile, shutil, os, sys
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from logging import Logger
from copy import deepcopy
import requests

from nistoar.midas.dap.fm.exceptions import *
from nistoar.midas.dap.fm import sim

tmpdir = tempfile.TemporaryDirectory(prefix="_test_sim_clients.")
rootdir = Path(os.path.join(tmpdir.name, "fmdata"))

class SimNextcloudApiTest(test.TestCase):

    def setUp(self):
        if not os.path.exists(rootdir):
            os.mkdir(rootdir)
        self.cfg = { }
        self.nccli = sim.SimNextcloudApi(rootdir, self.cfg)

    def tearDown(self):
        if os.path.exists(rootdir):
            shutil.rmtree(rootdir)

    def test_test(self):
        resp = self.nccli.test()
        self.assertEqual(resp.status_code, 200)

    def test_headers(self):
        self.assertIn('Host', self.nccli.headers())

    def test_users(self):
        self.assertEqual(self.nccli.get_user('ava1'), {})
        self.assertEqual(self.nccli.is_user('ava1'), False)
        self.assertEqual(self.nccli.get_users(), {})

        self.nccli.create_user('ava1')
        self.assertEqual(self.nccli.is_user('ava1'), True)
        u = self.nccli.get_user('ava1')
        self.assertEqual(u['user_id'], 'ava1')
        self.assertEqual(u['enabled'], True)
        u = self.nccli.get_users()
        self.assertIn('ava1', u)
        self.assertEqual(len(u), 1)

    def test_enable_user(self):
        self.assertFalse(self.nccli.is_user('ava1'))
        self.assertFalse(self.nccli.is_user_enabled('ava1'))
        self.nccli.create_user('ava1')
        self.assertTrue(self.nccli.is_user('ava1'))
        self.assertTrue(self.nccli.is_user_enabled('ava1'))
        self.nccli.disable_user('ava1')
        self.assertTrue(self.nccli.is_user('ava1'))
        self.assertFalse(self.nccli.is_user_enabled('ava1'))
        self.nccli.enable_user('ava1')
        self.assertTrue(self.nccli.is_user('ava1'))
        self.assertTrue(self.nccli.is_user_enabled('ava1'))

    def test_user_permissions(self):
#        admin = self.nccli.cfg.get('admin_user', 'oar_api')
#        self.assertFalse(self.nccli.is_user(admin))
#        self.nccli.create_user(admin)
#        self.assertTrue(self.nccli.is_user(admin))

        updir = rootdir/'mdst:0001'/'mdst:0001'
        os.makedirs(updir)
        with open(updir/'junk', 'w') as fd:
            pass
        perms = self.nccli.get_user_permissions("mdst:0001/mdst:0001")
        self.assertIn('ocs', perms)
        self.assertIn('meta', perms['ocs'])
        self.assertIn('data', perms['ocs'])
        perms = perms['ocs']['data']
        self.assertEqual(perms, [])
        perms = self.nccli.get_user_permissions("mdst:0001/mdst:0001/junk")['ocs']['data']
        self.assertEqual(perms, [])

        self.nccli.set_user_permissions('ava1', sim.svc.PERM_DELETE, "mdst:0001")
        perms = self.nccli.get_user_permissions("mdst:0001")['ocs']['data']
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0].get('share_with'), 'ava1')
        self.assertEqual(perms[0].get('permissions'), sim.svc.PERM_DELETE)
        perms = self.nccli.get_user_permissions("mdst:0001/mdst:0001/junk")['ocs']['data']
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0].get('share_with'), 'ava1')
        self.assertEqual(perms[0].get('permissions'), sim.svc.PERM_DELETE)
        perms = self.nccli.get_user_permissions('')['ocs']['data']
        self.assertEqual(perms, [])

        with self.assertRaises(FileManagerException):
            self.nccli.set_user_permissions('ava1', sim.svc.PERM_SHARE, "mdst:0001/mdst:0001/junk")
        
        self.nccli.set_user_permissions('ava1', sim.svc.PERM_SHARE, "mdst:0001/mdst:0001")
        perms = self.nccli.get_user_permissions("mdst:0001")['ocs']['data']
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0].get('share_with'), 'ava1')
        self.assertEqual(perms[0].get('permissions'), sim.svc.PERM_DELETE)
        perms = self.nccli.get_user_permissions("mdst:0001/mdst:0001")['ocs']['data']
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0].get('share_with'), 'ava1')
        self.assertEqual(perms[0].get('permissions'), sim.svc.PERM_SHARE)
        
        self.nccli.set_user_permissions('grn2', sim.svc.PERM_SHARE, "mdst:0001/mdst:0001")
        perms = self.nccli.get_user_permissions("mdst:0001/mdst:0001")['ocs']['data']
        self.assertEqual(len(perms), 2)
        self.assertEqual(set(p.get('share_with') for p in perms), set(('ava1', 'grn2',)))
        
    def test_scan_directory_files(self):
#        admin = self.nccli.cfg.get('admin_user', 'oar_api')
        updir = rootdir/'mdst:0001'/'mdst:0001'
        os.makedirs(updir)
        with open(updir/'junk', 'w') as fd:
            pass

        resp = self.nccli.scan_directory_files("mdst:0001/mdst:0001")
        self.assertTrue(resp.startswith("<?xml"))
        self.assertIn("junk", resp)

        resp = self.nccli.scan_directory_files("mdst:0001")
        self.assertTrue(resp.startswith("<?xml"))
        self.assertNotIn("junk", resp)

class SimFMWebDAVClientTest(test.TestCase):

    def setUp(self):
        if not os.path.exists(rootdir):
            os.mkdir(rootdir)
        self.cfg = { }
        self.wdcli = sim.SimFMWebDAVClient(rootdir, self.cfg)

    def tearDown(self):
        if os.path.exists(rootdir):
            shutil.rmtree(rootdir)

    def test_exists(self):
        path = 'mdst:0001/mdst:0001'
        self.assertTrue(not self.wdcli.exists(path))
        self.assertTrue(not self.wdcli.is_directory(path))
        self.assertTrue(not self.wdcli.is_file(path))
        
        updir = rootdir/path
        os.makedirs(updir)
        with open(updir/'junk', 'w') as fd:
            pass
        self.assertTrue(self.wdcli.exists(path))
        self.assertTrue(self.wdcli.is_directory(path))
        self.assertTrue(not self.wdcli.is_file(path))

        path += "/junk"
        self.assertTrue(self.wdcli.exists(path))
        self.assertTrue(not self.wdcli.is_directory(path))
        self.assertTrue(self.wdcli.is_file(path))

    def test_ensure_directory(self):
        path = 'mdst:0001/mdst:0001'
        self.assertTrue(not (rootdir/path).exists())
        self.assertTrue(not self.wdcli.exists(path))
        self.assertTrue(not self.wdcli.is_directory(path))
        self.assertTrue(not self.wdcli.is_file(path))
        
        self.wdcli.ensure_directory(path)
        self.assertTrue((rootdir/path).exists())
        self.assertTrue(self.wdcli.exists(path))
        self.assertTrue(self.wdcli.is_directory(path))
        self.assertTrue(not self.wdcli.is_file(path))
        
        self.wdcli.ensure_directory(path)
        self.assertTrue((rootdir/path).exists())
        self.assertTrue(self.wdcli.exists(path))
        self.assertTrue(self.wdcli.is_directory(path))
        self.assertTrue(not self.wdcli.is_file(path))

        path += "/junk"
        with open(rootdir/path, 'w') as fd:
            pass
        with self.assertRaises(FileManagerException):
            self.wdcli.ensure_directory(path)

    def test_get_resource_info(self):
        path = 'mdst:0001/mdst:0001'
        with self.assertRaises(FileManagerResourceNotFound):
            self.wdcli.get_resource_info(path)

        self.wdcli.ensure_directory(path)
        self.assertTrue((rootdir/path).is_dir())
        info = self.wdcli.get_resource_info(path)
        self.assertEqual(info.get('path'), "/"+path)
        self.assertIn("created", info)
        self.assertIn("fileid", info)
        self.assertIn("size", info)
        self.assertIn("permissions", info)

    def test_delete_resource(self):
        path = 'mdst:0001/mdst:0001'
        with self.assertRaises(FileManagerResourceNotFound):
            self.wdcli.delete_resource(path)

        self.wdcli.ensure_directory(path)
        with open(rootdir/path/"junk", 'w') as fd:
            pass
        self.assertTrue((rootdir/path).is_dir())
        self.assertTrue((rootdir/path/"junk").is_file())

        self.wdcli.delete_resource(path+"/junk")
        self.assertTrue((rootdir/path).is_dir())
        self.assertTrue(not (rootdir/path/"junk").exists())

        self.wdcli.delete_resource(path)
        self.assertTrue(not (rootdir/path).exists())
        self.assertTrue(not (rootdir/path/"junk").exists())
        

    

if __name__ == "__main__":
    test.main()
