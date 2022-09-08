import os, pdb, sys, json, requests, logging, time, re, hashlib, tempfile
import unittest as test

from nistoar.pdr.describe import altbig

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data', 'rmm-test-archive', 'versions')

loghdlr = None
rootlog = None
tmpdir = None
def setUpModule():
    global loghdlr
    global rootlog
    global tmpdir
    tmpdir = tempfile.TemporaryDirectory(prefix="_test_altbig.", dir=".")
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_altbig.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    global tmpdir
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestAltBigMetadataClient(test.TestCase):

    def setUp(self):
        self.cli = altbig.MetadataClient(datadir)

    def test_ctor(self):
        self.assertEqual(self.cli._root, datadir)
        self.assertTrue(not bool(self.cli._versions))

    def test_index_cache(self):
        self.assertTrue(not bool(self.cli._versions))
        self.cli._index_cache()
        self.assertTrue(bool(self.cli._versions))

        for id in "19A9D7193F868BDDE0531A57068151D2431 1E0F15DAAEFB84E4E0531A5706813DD8436 1E651A532AFD8816E0531A570681A662439 mds2-2106 mds2-2107 mds2-2110 mds2-7223 mds00qdrz9 mds00sxbvh mds003r0x6".split():
            self.assertIn(id, self.cli._versions)
        self.assertEqual(len(self.cli._versions), 10)        

        self.assertEqual(len(self.cli._versions['19A9D7193F868BDDE0531A57068151D2431']), 2)
        self.assertEqual(len(self.cli._versions['1E0F15DAAEFB84E4E0531A5706813DD8436']), 2)
        self.assertEqual(len(self.cli._versions['1E651A532AFD8816E0531A570681A662439']), 5)
        self.assertEqual(len(self.cli._versions['mds00qdrz9']), 2)
        self.assertEqual(len(self.cli._versions['mds003r0x6']), 2)
        self.assertEqual(len(self.cli._versions['mds00sxbvh']), 5)
        self.assertEqual(len(self.cli._versions['mds2-2106']), 8)
        self.assertEqual(len(self.cli._versions['mds2-2107']), 2)
        self.assertEqual(len(self.cli._versions['mds2-2110']), 3)
        self.assertEqual(len(self.cli._versions['mds2-7223']), 3)

    def test_exists(self):
        self.assertTrue(not self.cli._versions, "Index unintentionally created")

        self.assertTrue(self.cli.exists('mds2-2106', '1.5.0'))
        self.assertTrue(not self.cli._versions, "Index unintentionally created")

        self.assertTrue(self.cli.exists('mds2-2107'))
        self.assertTrue(self.cli._versions)

        self.assertTrue(self.cli.exists('19A9D7193F868BDDE0531A57068151D2431'))
        self.assertTrue(self.cli.exists('1E0F15DAAEFB84E4E0531A5706813DD8436'))
        self.assertTrue(self.cli.exists('1E651A532AFD8816E0531A570681A662439'))

        self.assertTrue(not self.cli.exists('mds2-2108'))
        self.assertTrue(self.cli.exists('mds2-7223'))

    def test_describe_ark(self):
        data = self.cli.describe("ark:/88434/mds003r0x6")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        data = self.cli.describe("ark:/88434/mds2-2110")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2110')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2110')

    def test_describe_no_ark(self):
        data = self.cli.describe("mds003r0x6")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        data = self.cli.describe("mds2-2110")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2110')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2110')

    def test_describe_ark_version(self):
        self.assertFalse(self.cli._versions, "Index unintentionally created")

        data = self.cli.describe("ark:/88434/mds2-2106", "1.6.0")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2106/pdr:v/1.6.0')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2106')
        self.assertEqual(data['version'], '1.6.0')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 7)
        self.assertFalse(self.cli._versions, "Index unintentionally created")

        data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.4")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.4')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)
        self.assertTrue(self.cli._versions)

        data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.1")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.1')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.1')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 2)

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.4")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.4')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.4')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 5)

        data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.1")
        self.assertEqual(data['@id'], 'ark:/88434/mds00sxbvh/pdr:v/1.0.1')
        self.assertEqual(data['ediid'], '1E651A532AFD8816E0531A570681A662439')
        self.assertEqual(data['version'], '1.0.1')
        self.assertEqual(len(data.get('releaseHistory',{}).get('hasRelease',[])), 2)

        with self.assertRaises(altbig.IDNotFound):
            data = self.cli.describe("ark:/88434/mds00sxbvh", "1.0.8")
        with self.assertRaises(altbig.IDNotFound):
            data = self.cli.describe("ark:/88434/mds00sxbvh/pdr:v/1.0.8", "1.0.1")

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





