import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.describe import rmm, altbig, hybrid
from nistoar.pdr.utils import read_json, write_json
from nistoar.pdr import constants as const

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data', 'rmm-test-archive', 'versions')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))

VER_DELIM  = const.RELHIST_EXTENSION
DSVER_DELIM = "/_v"

port = 9091
baseurl = "http://localhost:{0}/".format(port)
verpath_re = re.compile(VER_DELIM+r'/\d+\.\d+.\d+')
dsverpath_re = re.compile(DSVER_DELIM+r'/\d+\.\d+.\d+')

cachedir = None
def setup_cache():
    global cachedir
    ensure_tmpdir()
    cachedir = os.path.join(tmpdir(), "cache")
    os.mkdir(cachedir)
    for f in "mds2-2106-v1_5_0.json mds2-2106-v1_6_0.json".split():
        nerd = read_json(os.path.join(datadir, f))
        nerd['big'] = True
        if VER_DELIM in nerd.get('@id',''):
            nerd['@id'] = verpath_re.sub('', nerd['@id'])
        for cmp in nerd.get('components', []):
            if DSVER_DELIM in cmp.get('downloadURL',''):
                cmp['downloadURL'] = dsverpath_re.sub('', cmp['downloadURL'])
            if VER_DELIM in cmp.get('downloadURL',''):
                cmp['downloadURL'] = verpath_re.sub('', cmp['downloadURL'])
        write_json(nerd, os.path.join(cachedir, f))

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    os.system(cmd)
    time.sleep(0.5)

def stopService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_simsrv.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    startService()
    setup_cache()

def tearDownModule():
    global cachedir
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    stopService()
    rmtmpdir()

class TestMetadataClient(test.TestCase):

    def setUp(self):
        self.baseurl = baseurl
        self.cli = hybrid.MetadataClient(self.baseurl, cachedir)

    def test_describe_ark(self):
        data = self.cli.describe("ark:/88434/mds003r0x6")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        data = self.cli.describe("ark:/88434/mds2-2110")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2110')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2110')

    def test_no_cache(self):
        self.cli = hybrid.MetadataClient(self.baseurl)

        data = self.cli.describe("ark:/88434/mds2-2106", "1.6.0")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.6.0')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['version'], '1.6.0')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 7)
        self.assertTrue(not data.get('big'))
        dlus = [c['downloadURL'] for c in data.get('components', []) if 'downloadURL' in c]
        self.assertTrue(all(["/_v/" in u for u in dlus]))

    def test_describe_ark_version(self):
        data = self.cli.describe("ark:/88434/mds2-2106", "1.6.0")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.6.0')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['version'], '1.6.0')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 7)
        self.assertTrue(data.get('big'))
        dlus = [c['downloadURL'] for c in data.get('components', []) if 'downloadURL' in c]
        self.assertTrue(all(["/_v/" in u for u in dlus]))

        data = self.cli.describe("ark:/88434/mds2-2106", "1.5.0")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.5.0')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['version'], '1.5.0')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 6)
        self.assertTrue(data.get('big'))

        data = self.cli.describe("ark:/88434/mds2-2106")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['version'], '1.6.0')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 7)
        self.assertTrue(data.get('big'))
        dlus = [c['downloadURL'] for c in data.get('components', []) if 'downloadURL' in c]
        self.assertTrue(not any(["/_v/" in u for u in dlus]))

        data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.4")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.4')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)
        self.assertTrue(not data.get('big'))

        data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.1")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.1')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.1')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)
        self.assertTrue(not data.get('big'))

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.4")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.4')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)
        self.assertTrue(not data.get('big'))

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.1")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.1')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.1')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)
        self.assertTrue(not data.get('big'))

        with self.assertRaises(rmm.IDNotFound):
            data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.8")
        with self.assertRaises(rmm.IDNotFound):
            data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.8", "1.0.1")

    def test_describe_ediid(self):
        data = self.cli.describe("1E0F15DAAEFB84E4E0531A5706813DD8436")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        with self.assertRaises(rmm.IDNotFound):
            data = self.cli.describe("mds2-2110")

    def test_describe_ediid_version(self):
        data = self.cli.describe("1E651A532AFD8816E0531A570681A662439", "1.0.4")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.4')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')

        data = self.cli.describe("1E651A532AFD8816E0531A570681A662439", "1.0.1")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.1')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.1')

        with self.assertRaises(rmm.IDNotFound):
            data = self.cli.describe("1E651A532AFD8816E0531A570681A662439", "1.0.8")

    def test_describe_releases(self):
        data = self.cli.describe("ark:/88434/mds2-2110/pdr:v")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2110/pdr:v')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2110')
        self.assertEqual(data['version'], '1.0.1')
        self.assertEqual(len(data['hasRelease']), 2)

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(len(data['hasRelease']), 5)

    def test_search(self):
        data = self.cli.search()
        self.assertEqual(len(data), 7)

        ids = [d['ediid'] for d in data if 'ediid' in d]
        self.assertIn("ark:/88434/mds2-2106", ids)
        self.assertIn("ark:/88434/mds2-2107", ids)
        self.assertIn("ark:/88434/mds2-2110", ids)
        self.assertIn("ark:/88434/mds2-7223", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)
        
    def test_describe_file_component(self):
        data = self.cli.describe("ark:/88434/mds2-2106/cmps/NIST_NPL_InterlabData2019.csv")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/cmps/NIST_NPL_InterlabData2019.csv')
        self.assertEqual(data['version'], '1.6.0')
        self.assertIn('@context', data)
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        
        data = self.cli.describe("ark:/88434/mds2-2106/pdr:f/NIST_NPL_InterlabData2019.csv")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/cmps/NIST_NPL_InterlabData2019.csv')
        self.assertEqual(data['version'], '1.6.0')
        self.assertIn('@context', data)
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        
        data = self.cli.describe("ark:/88434/mds2-2106/pdr:v/1.4.0/pdr:f/NIST_NPL_InterlabData2019.csv")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.4.0/cmps/NIST_NPL_InterlabData2019.csv')
        self.assertEqual(data['version'], '1.4.0')
        self.assertIn('@context', data)
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        
        data = self.cli.describe("ark:/88434/mds2-2106/pdr:v/1.4.0/cmps/NIST_NPL_InterlabData2019.csv")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.4.0/cmps/NIST_NPL_InterlabData2019.csv')
        self.assertEqual(data['version'], '1.4.0')
        self.assertIn('@context', data)
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)

    def test_describe_part_component(self):
        data = self.cli.describe("ark:/88434/mds2-2106#doi:10.18434/M32106")
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106#doi:10.18434/M32106")
        self.assertEqual(data['version'], '1.6.0')
        self.assertEqual(data['isPartOf'], 'ark:/88434/mds2-2106')
        self.assertIn('@context', data)

        data = self.cli.describe("ark:/88434/mds00sxbvh#srd/nist-special-database-18")
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        self.assertEqual(data['@id'], "ark:/88434/mds00sxbvh#srd/nist-special-database-18")
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(data['isPartOf'], 'ark:/88434/mds00sxbvh')
        self.assertIn('@context', data)
        
        data = self.cli.describe("ark:/88434/mds2-2106/pdr:v/1.3.0#doi:10.18434/M32106")
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        self.assertEqual(data['@id'], "ark:/88434/mds2-2106/pdr:v/1.3.0#doi:10.18434/M32106")
        self.assertEqual(data['version'], '1.3.0')
        self.assertEqual(data['isPartOf'], 'ark:/88434/mds2-2106/pdr:v/1.3.0')
        self.assertIn('@context', data)

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.3#srd/nist-special-database-18")
        self.assertNotIn('components', data)
        self.assertNotIn('releaseHistory', data)
        self.assertNotIn('versionHistory', data)
        self.assertEqual(data['@id'], "ark:/88434/mds00sxbvh/pdr:v/1.0.3#srd/nist-special-database-18")
        self.assertEqual(data['version'], '1.0.3')
        self.assertEqual(data['isPartOf'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.3')
        self.assertIn('@context', data)
        

if __name__ == '__main__':
    test.main()


