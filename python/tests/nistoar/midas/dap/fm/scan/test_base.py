import json, tempfile, shutil, os, sys, re, time, logging
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from logging import Logger
from copy import deepcopy
import requests

from nistoar.midas.dap.fm import service as fm
from nistoar.midas.dap.fm.clients import NextcloudApi, FMWebDAVClient
from nistoar.midas.dap.fm.exceptions import *
from nistoar.base.config import ConfigurationException
from nistoar.midas.dap.fm import sim
from nistoar import jobmgt 
from nistoar.midas.dap.fm.scan import base as scan, simjobexec, BasicScanner, BasicScannerFactory

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'
scandatadir = execdir.parents[1] / 'data'
testscanrep = scandatadir / 'scan-report.json'

tmpdir = tempfile.TemporaryDirectory(prefix="_test_fm_service.")
rootdir = Path(os.path.join(tmpdir.name, "fmdata"))
jobdir = Path(os.path.join(tmpdir.name, "jobqueue"))

def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_scan.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)
    
def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

def dummy_fact(sp, id, log=None):
    pass

class BasicScannerTest(test.TestCase):

    def setUp(self):
        if not os.path.exists(rootdir):
            os.mkdir(rootdir)
        if not os.path.exists(jobdir):
            os.mkdir(jobdir)
        self.config = {
            'nextcloud_base_url': 'http://mocknextcloud/nc',
            'webdav': {
                'service_endpoint': 'http://mockservice/api',
            },
            'generic_api': {
                'service_endpoint': 'http://mockservice/api',
                'authentication': {
                    'user': 'admin',
                    'pass': 'pw'
                }
            },
            'local_storage_root_dir': str(rootdir),
            'admin_user': 'admin',
            'authentication': {
                'user': 'admin',
                'pass': 'pw'
            }
        }

        self.cli = sim.SimMIDASFileManagerService(self.config)
        self.id = "mdst:YYY1"
        self.sp = self.cli.create_space_for(self.id, 'ava1')
        with open(self.sp.root_dir/self.sp.uploads_folder/'junk', 'w') as fd:
            pass
        with open(self.sp.root_dir/self.sp.uploads_folder/'.junk', 'w') as fd:
            pass
        with open(self.sp.root_dir/self.sp.uploads_folder/'TRASH/oops', 'w') as fd:
            fd.write("0\n")

    def tearDown(self):
        scan.slow_scan_queue = None
        if os.path.exists(rootdir):
            shutil.rmtree(rootdir)
        if os.path.exists(jobdir):
            shutil.rmtree(jobdir)

    def test_init_scannable_content_with_prevscan(self):
        self.scanner = BasicScanner(self.sp, "fred", scan.basic_skip_patterns)
        shutil.copyfile(testscanrep, self.scanner.system_dir / scan.GOOD_SCAN_FILE)
        shutil.copyfile(testscanrep, self.scanner.user_dir / "ngc7793-HIm0.fits")
        files = self.scanner.init_scannable_content()
        self.assertTrue(any('fileid' in md for md in files))

    def test_init_scannable_content(self):
        self.scanner = BasicScanner(self.sp, "fred", scan.basic_skip_patterns)
        files = self.scanner.init_scannable_content()
        names = [f['path'] for f in files]
        self.assertIn("junk", names)
        self.assertNotIn(".junk", names)
        self.assertIn("HIDE", names)
        self.assertIn("TRASH", names)
        self.assertIn("TRASH/oops", names)
        self.assertEqual(len(files), 4)

        files = self.scanner.init_scannable_content("TRASH")
        names = [f['path'] for f in files]
        self.assertIn("TRASH/oops", names)
        self.assertEqual(len(files), 1)

        self.scanner = BasicScanner(self.sp, "fred",
                                    [re.compile(r"^\."), re.compile(r"^#"), re.compile(r"^TRASH")])
        files = self.scanner.init_scannable_content()
        names = [f['path'] for f in files]
        self.assertIn("junk", names)
        self.assertNotIn(".junk", names)
        self.assertIn("HIDE", names)
        self.assertNotIn("TRASH", names)
        self.assertNotIn("TRASH/oops", names)
        self.assertEqual(len(files), 2)

    def test_fast_slow(self):
        self._set_scan_queue()
        skip = [ re.compile(r"^\."), re.compile(r"^#"), re.compile(r"^HIDE$") ]
        self.scanner = BasicScanner(self.sp, "fred", skip)
        self.assertIs(self.scanner.sp, self.sp)
        self.assertEqual(self.scanner.scanid, "fred")
        self.assertEqual(self.scanner.space_id, self.id)
        self.assertEqual(self.scanner.user_dir, self.sp.root_dir/self.sp.uploads_folder)

        driver = scan.UserSpaceScanDriver(self.sp, dummy_fact, scan.slow_scan_queue)
        content = driver._init_scan_md(self.scanner.scanid)
        content['contents'] = self.scanner.init_scannable_content()
        self.assertEqual(content['scan_id'], "fred")
        self.assertEqual(content['space_id'], self.sp.id)
        self.assertEqual(content['fm_space_path'], '/'.join([self.sp.id,self.sp.id]))
        self.assertGreater(content['scan_time'], 0)
        self.assertEqual(len(content['contents']), 3)
        self.assertEqual(content['contents'][0]['path'], "junk")
        self.assertNotIn('size', content['contents'][0])

        files = [f.get('path') for f in content['contents']]
        self.assertIn("junk", files)
        self.assertIn("TRASH", files)
        self.assertIn("TRASH/oops", files)
        self.assertEqual(len(files), 3)

        c = self.scanner.fast_scan(content)
        self.assertEqual(c['scan_id'], "fred")
        self.assertEqual(c['space_id'], self.sp.id)
        self.assertEqual(c['fm_space_path'], '/'.join([self.sp.id,self.sp.id]))
        self.assertEqual(len(c['contents']), 3)
        self.assertEqual(c['contents'][0]['path'], "TRASH")
        self.assertEqual(c['contents'][1]['path'], "TRASH/oops")
        self.assertEqual(c['contents'][2]['path'], "junk")
        self.assertIn('size', content['contents'][0])
        self.assertEqual(content['contents'][2]['size'], 0)
        self.assertEqual(content['contents'][0]['size'], 0)
        self.assertIn('mtime', content['contents'][0])
        self.assertNotIn('fileid', content['contents'][0])
        self.assertNotIn('checksum', content['contents'][0])
        self.assertNotIn('last_checksum_date', content['contents'][0])

        scanfile = self.sp.root_dir/self.sp.system_folder/"scan-report-fred.json"
        self.assertTrue(scanfile.is_file())
        
        c = self.scanner.slow_scan(c)
        self.assertEqual(c['scan_id'], "fred")
        self.assertEqual(c['space_id'], self.sp.id)
        self.assertEqual(c['fm_space_path'], '/'.join([self.sp.id,self.sp.id]))
        self.assertEqual(len(c['contents']), 3)
        self.assertEqual(c['contents'][0]['path'], "TRASH")
        self.assertEqual(c['contents'][1]['path'], "TRASH/oops")
        self.assertEqual(c['contents'][2]['path'], "junk")
        self.assertIn('checksum', content['contents'][2])
        self.assertIn('last_checksum_date', content['contents'][2])

        # TRASH folder
        fmd = c['contents'][0]
        self.assertEqual(fmd['resource_type'], "collection")
        for prop in "size fileid created permissions".split():
            self.assertIn(prop, fmd)
        self.assertNotIn("check_sum", fmd)
        self.assertNotIn("last_checksum_date", fmd)
        self.assertEqual(fmd["accumulated_size"], 2)
        self.assertEqual(fmd["size"], 0)
        errs = fmd.get('scan_errors', [])
        self.assertEqual(len(errs), 0,
                         f"{len(errs)} errors reported in 'scan_errors' property")

        # junk file
        fmd = c['contents'][2]
        self.assertEqual(fmd['resource_type'], "file")
        for prop in "size fileid checksum last_checksum_date created permissions".split():
            self.assertIn(prop, fmd)
        self.assertGreater(fmd['last_checksum_date'], fmd['ctime'])
        self.assertEqual(fmd["size"], 0)
        errs = fmd.get('scan_errors', [])
        self.assertEqual(len(errs), 0,
                         f"{len(errs)} errors reported in 'scan_errors' property")

        # test the report contents
        self.assertTrue(scanfile.is_file())
        with open(scanfile) as fd:
            c = json.load(fd)
        self.assertEqual(c['scan_id'], "fred")
        self.assertEqual(c['space_id'], self.sp.id)
        self.assertEqual(c['fm_space_path'], '/'.join([self.sp.id,self.sp.id]))
        self.assertEqual(len(c['contents']), 3)
        self.assertEqual(c['contents'][0]['path'], "TRASH")
        self.assertEqual(c['contents'][1]['path'], "TRASH/oops")
        self.assertEqual(c['contents'][2]['path'], "junk")
        self.assertEqual(c['accumulated_size'], 2)

    def _set_scan_queue(self):
        scan.set_slow_scan_queue(jobdir, resume=False)
        scan.slow_scan_queue.mod = simjobexec
        
    def test_driver(self):
        self._set_scan_queue()
        self.assertTrue(scan.slow_scan_queue)
        with open(self.sp.root_dir/self.sp.uploads_folder/'junk', 'w') as fd:
            fd.write("goob\n")
        driver = scan.UserSpaceScanDriver(self.sp, BasicScannerFactory, scan.slow_scan_queue)
        scanid = driver.launch_scan()

        self.assertTrue(scanid)
        scanfile = self.sp.root_dir/self.sp.system_folder/f"scan-report-{scanid}.json"
        self.assertTrue(scanfile.is_file())

        #time.sleep(0.5)
        driver.scanq.runner.runthread.join(2.0)
        with open(scanfile) as fd:
            rep = json.load(fd)

        self.assertEqual(rep['scan_id'], scanid)
        self.assertEqual(rep['space_id'], self.sp.id)
        self.assertEqual(rep['fm_space_path'], '/'.join([self.sp.id,self.sp.id]))
        self.assertEqual(len(rep['contents']), 1)
        self.assertEqual(rep['contents'][0]['path'], "junk")
        self.assertEqual(rep['contents'][0]['size'], 5)
        self.assertTrue(rep['contents'][0]['checksum'])
        self.assertEqual(rep['accumulated_size'], 5)
        self.assertIn('checksum', rep['contents'][0])
        self.assertGreater(rep['contents'][0]['last_checksum_date'], rep['contents'][0]['ctime'])

        scanfile = self.sp.root_dir/self.sp.system_folder/f"lastgoodscan.json"
        self.assertTrue(scanfile.is_file())





if __name__ == "__main__":
    test.main()
