import os, sys, pdb, requests, logging, time, shutil, json, tempfile, re
import unittest as test
from copy import deepcopy
from pathlib import Path

from nistoar.pdr.public.sim import db as sim
from nistoar.pdr.describe.rmm import MetadataClient as rmm
from nistoar.testing import *
# import tests.nistoar.pdr.describe.sim_describe_svc as desc

releasesets = rmm.COLL_RELEASES
versions = rmm.COLL_VERSIONS
records = rmm.COLL_LATEST

testdir = Path(__file__).resolve().parent
descdir = testdir.parents[1]/"describe"
datadir = descdir/'data'/'rmm-test-archive'

tmpdir = tempfile.TemporaryDirectory(prefix="_test_sim.")
def tearDownModule():
    tmpdir.cleanup()

class TestArchive(test.TestCase):

    def setUp(self):
        self.dir = Path(datadir)
        self.arch = sim.SimDb(self.dir)
        self.tmpdir = tempfile.TemporaryDirectory(prefix="archive.")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        def filefor(aipid):
            return self.arch.dir/coll/(aipid+".json")
        self.assertEqual(self.arch.dir, datadir)
        coll = records
        self.assertEqual(self.arch.records['ark:/88434/mds003r0x6'],
                         filefor('1E0F15DAAEFB84E4E0531A5706813DD8436'))
        self.assertEqual(self.arch.records['1E0F15DAAEFB84E4E0531A5706813DD8436'],
                         filefor('1E0F15DAAEFB84E4E0531A5706813DD8436'))
        self.assertTrue(os.path.isfile(self.arch.records['ark:/88434/mds003r0x6']))
        self.assertEqual(self.arch.records['ark:/88434/mds00qdrz9'],
                         filefor('19A9D7193F868BDDE0531A57068151D2431'))
        self.assertEqual(self.arch.records['ark:/88434/mds00sxbvh'],
                         filefor('1E651A532AFD8816E0531A570681A662439'))
        self.assertEqual(self.arch.records["ark:/88434/mds2-2106"],
                         filefor("mds2-2106"))
        self.assertEqual(self.arch.records["ark:/88434/mds2-2107"],
                         filefor("mds2-2107"))
        self.assertEqual(self.arch.records["ark:/88434/mds2-2110"],
                         filefor("mds2-2110"))
        self.assertEqual(self.arch.records["ark:/88434/mds2-7223"],
                         filefor("mds2-7223"))
        coll = releasesets
        self.assertEqual(self.arch.releasesets['ark:/88434/mds003r0x6/pdr:v'],
                         filefor('1E0F15DAAEFB84E4E0531A5706813DD8436'))
        self.assertEqual(self.arch.releasesets['ark:/88434/mds00qdrz9/pdr:v'],
                         filefor('19A9D7193F868BDDE0531A57068151D2431'))
        self.assertEqual(self.arch.releasesets['ark:/88434/mds00sxbvh/pdr:v'],
                         filefor('1E651A532AFD8816E0531A570681A662439'))
        self.assertEqual(self.arch.releasesets["ark:/88434/mds2-2106/pdr:v"],
                         filefor("mds2-2106"))
        self.assertEqual(self.arch.releasesets["ark:/88434/mds2-2107/pdr:v"],
                         filefor("mds2-2107"))
        self.assertEqual(self.arch.releasesets["ark:/88434/mds2-2110/pdr:v"],
                         filefor("mds2-2110"))
        self.assertEqual(self.arch.releasesets["ark:/88434/mds2-7223/pdr:v"],
                         filefor("mds2-7223"))
        self.assertIn("ark:/88434/mds00sxbvh/pdr:v/1.0.4", self.arch.versions)
        self.assertIn("ark:/88434/mds2-2106/pdr:v/1.2.0",  self.arch.versions)
        self.assertEqual(len(self.arch.versions), 18)

    def test_get_rec_from(self):
        self.assertIsNone(self.arch.get_rec_from(records, 'goober'))
        self.assertIn('title', self.arch.get_rec_from(records, '1E651A532AFD8816E0531A570681A662439'))
        self.assertIn('title', self.arch.get_rec_from(records, 'ark:/88434/mds00sxbvh'))
        self.assertIn('title', self.arch.get_rec_from(records, 'mds00sxbvh'))
        self.assertNotIn('hasRelease', self.arch.get_rec_from(records, 'mds00sxbvh'))
        self.assertIn('title', self.arch.get_rec_from(releasesets,
                                                      '1E651A532AFD8816E0531A570681A662439'))
        self.assertIn('title', self.arch.get_rec_from(releasesets, 'ark:/88434/mds00sxbvh/pdr:v'))
        self.assertIn('hasRelease', self.arch.get_rec_from(releasesets,
                                                      '1E651A532AFD8816E0531A570681A662439'))
        self.assertIn('hasRelease', self.arch.get_rec_from(releasesets, 'ark:/88434/mds00sxbvh/pdr:v'))

    def test_iter(self):
        recs = list(self.arch.iter())
        self.assertEqual(len(recs), 7)
        
        self.assertEqual(len(list(self.arch.iter(releasesets))), 7)
        self.assertEqual(len(list(self.arch.iter(versions))), 18)

    def test_ids(self):
        ids = self.arch.get_aipids_from();
        self.assertEqual(len(ids), 7)
        self.assertIn("mds2-2106", ids)
        self.assertIn("mds2-2107", ids)
        self.assertIn("mds2-2110", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)

        ids = self.arch.get_aipids_from(releasesets);
        self.assertEqual(len(ids), 7)
        self.assertIn("mds2-2106", ids)
        self.assertIn("mds2-2107", ids)
        self.assertIn("mds2-2110", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)

        ids = self.arch.get_aipids_from("versions");
        self.assertEqual(len(ids), 18)
        self.assertIn("mds2-2106-v1_2_0", ids)
        self.assertIn("mds2-2107-v1_0_0", ids)
        self.assertIn("mds2-2110-v1_0_1", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439-v1_0_4", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431-v1_0_0", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436-v1_0_0", ids)

    def test_add_rec(self):
        self.arch = sim.SimDb(self.tmpdir.name, False)
        aipid = '1E651A532AFD8816E0531A570681A662439'

        nerdf = datadir/versions/(aipid+'-v1_0_3.json')
        with open(nerdf) as fd:
            nerd = json.load(fd)
            nerd['@id'] = re.sub(r'/pdr:v/.*$', '', nerd['@id'])
        self.arch.add_rec(nerd)

        mdf = self.arch.dir/records/(aipid+".json")
        self.assertEqual(self.arch.records[aipid], mdf)
        self.assertEqual(self.arch.records[nerd['@id']], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")

        mdf = self.arch.dir/releasesets/(aipid+".json")
        self.assertEqual(self.arch.releasesets[aipid], mdf)
        self.assertEqual(self.arch.releasesets[nerd['@id']+"/pdr:v"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")
        self.assertEqual(len(rec['hasRelease']), 4)

        mdf = self.arch.dir/versions/(aipid+"-v1_0_3.json")
        self.assertNotIn(aipid, self.arch.versions)
        self.assertEqual(self.arch.versions[nerd['@id']+"/pdr:v/1.0.3"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")

        nerdf = datadir/versions/(aipid+'-v1_0_1.json')
        with open(nerdf) as fd:
            nerd = json.load(fd)
            nerd['@id'] = re.sub(r'/pdr:v/.*$', '', nerd['@id'])
        self.arch.add_rec(nerd)

        # later version not overwritten in records, releasesets
        mdf = self.arch.dir/records/(aipid+".json")
        self.assertEqual(self.arch.records[aipid], mdf)
        self.assertEqual(self.arch.records[nerd['@id']], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")

        mdf = self.arch.dir/releasesets/(aipid+".json")
        self.assertEqual(self.arch.releasesets[aipid], mdf)
        self.assertEqual(self.arch.releasesets[nerd['@id']+"/pdr:v"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")
        self.assertEqual(len(rec['hasRelease']), 4)

        mdf = self.arch.dir/versions/(aipid+"-v1_0_1.json")
        self.assertNotIn(aipid, self.arch.versions)
        self.assertEqual(self.arch.versions[nerd['@id']+"/pdr:v/1.0.1"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.1")

        mdf = self.arch.dir/versions/(aipid+"-v1_0_3.json")
        self.assertNotIn(aipid, self.arch.versions)
        self.assertEqual(self.arch.versions[nerd['@id']+"/pdr:v/1.0.3"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")

        nerdf = datadir/versions/(aipid+'-v1_0_4.json')
        with open(nerdf) as fd:
            nerd = json.load(fd)
            nerd['@id'] = re.sub(r'/pdr:v/.*$', '', nerd['@id'])
        self.arch.add_rec(nerd)

        mdf = self.arch.dir/records/(aipid+".json")
        self.assertEqual(self.arch.records[aipid], mdf)
        self.assertEqual(self.arch.records[nerd['@id']], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.4")

        mdf = self.arch.dir/releasesets/(aipid+".json")
        self.assertEqual(self.arch.releasesets[aipid], mdf)
        self.assertEqual(self.arch.releasesets[nerd['@id']+"/pdr:v"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.4")
        self.assertEqual(len(rec['hasRelease']), 5)

        mdf = self.arch.dir/versions/(aipid+"-v1_0_4.json")
        self.assertNotIn(aipid, self.arch.versions)
        self.assertEqual(self.arch.versions[nerd['@id']+"/pdr:v/1.0.4"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.4")

        mdf = self.arch.dir/versions/(aipid+"-v1_0_3.json")
        self.assertNotIn(aipid, self.arch.versions)
        self.assertEqual(self.arch.versions[nerd['@id']+"/pdr:v/1.0.3"], mdf)
        self.assertTrue(mdf.is_file())
        with open(mdf) as fd:
            rec = json.load(fd)
        self.assertEqual(rec['version'], "1.0.3")


    def test_readonly(self):
        self.arch = sim.SimDb(self.tmpdir.name, True)

        aipid = '1E651A532AFD8816E0531A570681A662439'
        nerdf = datadir/versions/(aipid+'-v1_0_3.json')
        with self.assertRaises(RuntimeError):
            self.arch.add_rec_from_file(nerdf)

        with open(nerdf) as fd:
            rec = json.load(fd)
        with self.assertRaises(RuntimeError):
            self.arch.add_to_coll(versions, rec)

        rec['@id'] = re.sub(r'/pdr:v/.*$', '', rec['@id'])
        with self.assertRaises(RuntimeError):
            self.arch.add_rec(rec)

        self.arch = sim.SimDb(self.tmpdir.name, False)
        self.arch.add_rec_from_file(nerdf)


if __name__ == '__main__':
    test.main()


