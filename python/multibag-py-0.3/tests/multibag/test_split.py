# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging, io
import tempfile, shutil
import unittest as test
from functools import cmp_to_key

import multibag.split as split
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

class TestSplitPlan(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
        shutil.rmtree(os.path.join(self.bagdir, "multibag"))
        os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

        # add a line to the bag-info.txt that will require encoding
        with io.open(os.path.join(self.bagdir,"bag-info.txt"),
                     'a', encoding='utf-8') as fd:
            fd.write(u"Funny-Characters: ")
            fd.write('ß')
            fd.write(u"\n")

        self.bag = split.asProgenitor(Bag(self.bagdir))
        self.plan = split.SplitPlan(self.bag)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertEqual(self.plan._manifests, [])
        self.assertIs(self.plan.progenitor, self.bag)

    def test_is_complete(self):
        self.assertFalse(self.plan.is_complete())

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertFalse(self.plan.is_complete())

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertTrue(self.plan.is_complete())

    def test_required(self):
        exp = "about.txt data/trial1.json data/trial2.json " + \
              "data/trial3/trial3a.json metadata/pod.json metadata/trial3"
        exp = exp.split()
        
        required = set(self.plan.required())
        for p in exp:
            self.assertIn(p, required)
        self.assertEqual(len(required), 6)

    def test_find_destination(self):
        self.assertIsNone(self.plan.find_destination("about.txt"))
        
        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)

        self.assertEqual(self.plan.find_destination("about.txt")['name'],
                         "goob_1.bag")
        self.assertEqual(self.plan.find_destination("metadata/trial3")['name'],
                         "goob_1.bag")
        self.assertEqual(self.plan.find_destination("data/trial2.json")['name'],
                         "goob_2.bag")
        self.assertIsNone(self.plan.find_destination("goober.txt"))
        

    def test_missing(self):
        exp = "about.txt data/trial1.json data/trial2.json " + \
              "data/trial3/trial3a.json metadata/pod.json metadata/trial3"
        exp = exp.split()
        
        missing = set(self.plan.missing())
        for p in exp:
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 6)

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        missing = set(self.plan.missing())
        for p in "data/trial1.json data/trial2.json data/trial3/trial3a.json".split():
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 3)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)
        missing = set(self.plan.missing())
        self.assertEqual(len(missing), 0)

    def test_complete_plan(self):
        willmiss = "data/trial1.json data/trial2.json data/trial3/trial3a.json"
        willmiss = willmiss.split()
        
        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertEqual(len(self.plan._manifests), 1)
        missing = set(self.plan.missing())
        for p in willmiss:
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 3)

        self.plan.complete_plan()
        self.assertEqual(len(self.plan._manifests), 2)
        self.assertGreater(self.plan._manifests[-1]['totalsize'], 0)
        self.assertEqual(self.plan._manifests[-1]['totalsize'], 208)
        for p in willmiss:
            self.assertIn(p, self.plan._manifests[-1]["contents"])
        self.assertEqual(len(self.plan._manifests[-1]["contents"]), 3)
        missing = set(self.plan.missing())
        self.assertEqual(len(missing), 0)

    def test_name_output_bags(self):

        class nameiter(object):
            lim = 10
            def __init__(self):
                self.nxt = 0
            def __iter__(self):
                return self
            def __next__(self):
                self.nxt += 1
                return "mbag_"+str(self.nxt-1)
            def next(self):
                return self.__next__()

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)

        ni = nameiter()
        self.plan.name_output_bags(ni)
        self.assertEqual(self.plan._manifests[0]['name'], "mbag_0")
        self.assertEqual(self.plan._manifests[1]['name'], "mbag_1")
        self.plan.name_output_bags(ni, True)
        self.assertEqual(self.plan._manifests[0]['name'], "mbag_3")
        self.assertEqual(self.plan._manifests[1]['name'], "mbag_2")

    def test_apply_iter(self):
        manifest1 = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest1)

        manifest2 = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest2)

        iter = self.plan.apply_iter(self.tempdir)
        mbag = next(iter)
        mbagdir = os.path.join(self.tempdir, "goob_1.bag")
        self.assertEqual(mbag, mbagdir)
        for member in manifest1['contents']:
            self.assertTrue(os.path.exists(os.path.join(mbagdir, member)))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bagit.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir,
                                                    "manifest-sha256.txt")))

        bag = Bag(mbagdir)
        self.assertTrue(bag.validate())
        self.assertTrue(bag.is_valid())
        self.assertNotIn('Bag-Size', bag.info)
        self.assertIn('Multibag-Source-Bag-Size', bag.info)
        self.assertIn('Bagging-Date', bag.info)

        mbag = next(iter)
        mbagdir = os.path.join(self.tempdir, "goob_2.bag")
        self.assertEqual(mbag, mbagdir)
        for member in manifest2['contents']:
            self.assertTrue(os.path.exists(os.path.join(mbagdir, member)))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bagit.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir,
                                                    "manifest-sha256.txt")))

        bag = Bag(mbagdir)
        self.assertTrue(bag.validate())
        self.assertTrue(bag.is_valid())

    def test_apply_iter_nopass(self):
        manifest1 = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest1)

        manifest2 = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest2)

        iter = self.plan.apply_iter(self.tempdir,
                                    info_nopass=['Bagging-Date', 'Bag-Size'])
        mbag = next(iter)
        mbagdir = os.path.join(self.tempdir, "goob_1.bag")
        self.assertEqual(mbag, mbagdir)
        for member in manifest1['contents']:
            self.assertTrue(os.path.exists(os.path.join(mbagdir, member)))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bagit.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir,
                                                    "manifest-sha256.txt")))

        bag = Bag(mbagdir)
        self.assertTrue(bag.validate())
        self.assertTrue(bag.is_valid())
        self.assertNotIn('Bag-Size', bag.info)
        self.assertNotIn('Multibag-Source-Bag-Size', bag.info)
        self.assertNotIn('Bagging-Date', bag.info)


class TestNeighborlySplitter(test.TestCase):

    files = {
        "/a":   14,
        "/b": 1598,
        "/c":  350,
        "/d":  350,
        "/e/a":    860,
        "/e/b":    860,
        "/e/c":    860,
        "/e/d/a":    1100,
        "/e/d/b":      58,
        "/e/d/c":      58,
        "/e/d/d":      58,
        "/e/d/3":      58,
        "/f/a":    860,
        "/f/b":    860,
        "/f/c":    860,
        "/f/d/a":    1100,
        "/f/d/b":      58,
        "/f/d/c":      58,
        "/f/d/d":      58,
        "/f/d/3":      58,
        "/g": 0,
        "/h": 10,
        "/bagit.txt": 43,
        "/fetch.txt":  0
    }

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        self.info = self.mkinfo(self.files)

    def mkinfo(self, files):
        out = []
        for f in files:
            out.append({ "path": f, "size": files[f], "name": f.split('/')[-1] })
        return out

    def test_is_special(self):
        self.spltr = split.NeighborlySplitter()
        specials = [n for n in self.files.keys() if self.spltr._is_special(n)]
        self.assertIn("/fetch.txt", specials)
        self.assertIn("/bagit.txt", specials)
        self.assertEqual(len(specials), 2)

    def test_cmp_by_size(self):
        self.spltr = split.NeighborlySplitter()
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        sz = 0
        for fi in reversed(self.info):
            self.assertGreaterEqual(fi['size'], sz)
            sz = fi['size']

    def test_apply_algorithm(self):
        self.spltr = split.NeighborlySplitter(2200, 2000)
        self.info = [f for f in self.info
                       if not self.spltr._is_special(f['path'])]
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        bag = ReadOnlyBag(self.bagdir)
        plan = split.SplitPlan(bag)
        self.spltr._apply_algorithm(self.info, plan)

        self.assertGreater(len(plan._manifests), 0)
        # self.assertEqual(len(plan._manifests), 1)

        mf = plan._manifests[0]
        self.assertIn("b", mf['contents'])
        self.assertIn("c", mf['contents'])
        self.assertIn("a", mf['contents'])
        self.assertIn("g", mf['contents'])
        self.assertIn("h", mf['contents'])
        # self.assertIn("e/d/3", mf['contents'])
        self.assertEqual(len(mf['contents']), 6)
        self.assertGreater(2200, mf['totalsize'])

        for i in range(6):
            self.assertGreater(2200, plan._manifests[i]['totalsize'])

    def test_plan(self):
        self.spltr = split.NeighborlySplitter(500)
        plan = self.spltr.plan(self.bagdir)
        self.assertTrue(plan.is_complete())
        mfs = list(plan.manifests())
        self.assertEqual(len(list(plan.manifests())), 3)
        self.assertIn("metadata/pod.json",        mfs[0]['contents'])
        self.assertEqual(len(mfs[0]['contents']), 1)
        self.assertEqual("samplembag_1.mbag",      mfs[0]['name'])
        
        self.assertIn("about.txt", mfs[1]['contents'])
        self.assertIn("data/trial1.json",         mfs[1]['contents'])
        self.assertIn("multibag/member-bags.tsv", mfs[1]['contents'])
        self.assertEqual(len(mfs[1]['contents']), 3)
        self.assertEqual("samplembag_2.mbag",      mfs[1]['name'])
        
        self.assertIn("multibag/file-lookup.tsv", mfs[2]['contents'])
        self.assertIn("data/trial2.json",         mfs[2]['contents'])
        self.assertIn("data/trial3/trial3a.json", mfs[2]['contents'])
        self.assertEqual(len(mfs[2]['contents']), 3)
        self.assertEqual("samplembag_3.mbag",      mfs[2]['name'])

    def test_split(self):
        from multibag.validate import HeadBagValidator, MemberBagValidator
        
        self.tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(self.tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            shutil.rmtree(os.path.join(self.bagdir, "multibag"))
            
            self.spltr = split.NeighborlySplitter(500)
            self.spltr.split(self.bagdir, self.tempdir)

            mbags = [d for d in os.listdir(self.tempdir) if d.endswith(".mbag")]
            self.assertIn("samplebag_1.mbag", mbags)
            self.assertIn("samplebag_2.mbag", mbags)
            self.assertIn("samplebag_3.mbag", mbags)
            self.assertEqual(len(mbags), 3)

            # run validator on output files!
            MemberBagValidator(os.path.join(self.tempdir,
                                            "samplebag_1.mbag")).ensure_valid()
            MemberBagValidator(os.path.join(self.tempdir,
                                            "samplebag_2.mbag")).ensure_valid()
            HeadBagValidator(os.path.join(self.tempdir,
                                          "samplebag_3.mbag")).ensure_valid()

        finally:
            shutil.rmtree(self.tempdir)
            
        

class TestWellPackedSplitter(test.TestCase):

    files = {
        "/a":   14,
        "/b": 1598,
        "/c":  350,
        "/d":  350,
        "/e/a":    860,
        "/e/b":    860,
        "/e/c":    860,
        "/e/d/a":    1100,
        "/e/d/b":      58,
        "/e/d/c":      58,
        "/e/d/d":      58,
        "/e/d/3":      58,
        "/f/a":    860,
        "/f/b":    860,
        "/f/c":    860,
        "/f/d/a":    1100,
        "/f/d/b":      58,
        "/f/d/c":      58,
        "/f/d/d":      58,
        "/f/d/3":      58,
        "/g": 0,
        "/h": 10,
        "/bagit.txt": 43,
        "/fetch.txt":  0
    }

    def mkinfo(self, files):
        out = []
        for f in files:
            out.append({ "path": f, "size": files[f], "name": f.split('/')[-1] })
        return out

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        self.spltr = split.WellPackedSplitter(2500)
        self.info = self.mkinfo(self.files)

    def test_apply_algorithm(self):
        self.info = [f for f in self.info
                       if not self.spltr._is_special(f['path'])]
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        bag = ReadOnlyBag(self.bagdir)
        plan = split.SplitPlan(bag)
        self.spltr._apply_algorithm(self.info, plan)

        self.assertGreater(len(plan._manifests), 0)
        # self.assertEqual(len(plan._manifests), 1)

        mf = plan._manifests[0]
        self.assertIn("b", mf['contents'])
        self.assertIn("e/a", mf['contents'])
        self.assertIn("a", mf['contents'])
        self.assertIn("g", mf['contents'])
        self.assertIn("h", mf['contents'])
        self.assertEqual(len(mf['contents']), 5)
        self.assertGreater(2500, mf['totalsize'])

        for i in range(5):
            self.assertGreater(2500, plan._manifests[i]['totalsize'])


class TestSimpleNamer(test.TestCase):

    def test_iter(self):
        niter = split.SimpleNamer("goober")
        self.assertEqual(niter.next(), "goober_1.mbag")
        self.assertEqual(niter.next(), "goober_2.mbag")
        self.assertEqual(niter.next(), "goober_3.mbag")
        self.assertEqual(niter.next(), "goober_4.mbag")
        self.assertEqual(niter.next(), "goober_5.mbag")



if __name__ == '__main__':
    test.main()
