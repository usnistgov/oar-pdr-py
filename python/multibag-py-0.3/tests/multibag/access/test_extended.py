# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test

from fs import open_fs

import multibag.access.extended as xtend
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "data")

def ishardlink(path):
    return os.stat(path).st_nlink > 1

class TestExtendedReadWritableBag(test.TestCase):

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        # self.bag = xtend.as_extended(Bag(self.bagdir))
        self.bag = xtend.ExtendedReadWritableBag(self.bagdir)

    def test_ctor(self):
        self.assertEqual(self.bag._bagdir, self.bagdir)
        self.assertTrue(isinstance(self.bag, xtend._ExtendedReadWritableMixin))
        self.assertTrue(isinstance(self.bag, xtend.ExtendedReadWritableBag))
        self.assertEqual(self.bag._bagname, "samplembag")

    def test_exists(self):
        self.assertTrue(self.bag.exists("bagit.txt"))
        self.assertTrue(self.bag.exists("data/trial1.json"))
        self.assertFalse(self.bag.exists("data/goober"))
        self.assertFalse(self.bag.exists("data/trial3/goober"))

        self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                    "data/../../../data")))
        self.assertFalse(self.bag.exists("data/../../../data"))

    def test_isdir(self):
        self.assertFalse(self.bag.isdir("bagit.txt"))
        self.assertTrue(self.bag.isdir("data"))
        self.assertFalse(self.bag.isdir("data/trial1.json"))
        self.assertFalse(self.bag.isdir("data/goober"))
        self.assertTrue(self.bag.isdir("data/trial3"))
        self.assertFalse(self.bag.isdir("data/trial3/goober"))

        self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                    "data/../../../data")))
        self.assertFalse(self.bag.isdir("data/../../../data"))

    def test_isfile(self):
        self.assertTrue(self.bag.isfile("bagit.txt"))
        self.assertFalse(self.bag.isfile("data"))
        self.assertTrue(self.bag.isfile("data/trial1.json"))
        self.assertFalse(self.bag.isfile("data/goober"))
        self.assertFalse(self.bag.isfile("data/trial3"))
        self.assertFalse(self.bag.isfile("data/trial3/goober"))

    def test_sizeof(self):
        self.assertEqual(self.bag.sizeof("data/trial1.json"), 69)
        self.assertGreater(self.bag.sizeof("data"), 0)
        with self.assertRaises(OSError):
            self.bag.sizeof("data/goober")

    def test_timesfor(self):
        times = self.bag.timesfor("data/trial1.json")
        self.assertIsNotNone(times)
        self.assertGreater(times.ctime, 0)
        self.assertGreater(times.mtime, 0)
        self.assertGreater(times.atime, 0)

        times = self.bag.timesfor("data")
        self.assertIsNotNone(times)
        self.assertGreater(times.ctime, 0)
        self.assertGreater(times.mtime, 0)
        self.assertGreater(times.atime, 0)

        with self.assertRaises(OSError):
            self.bag.timesfor("data/goober")

    def test_bag(self):
        # test that self.bag behaves like a bagit.Bag
        self.assertEqual(self.bag.algs, ["sha256"])
        self.assertEqual(list(self.bag.manifest_files()),
                         [os.path.join(self.bagdir, "manifest-sha256.txt")])

    def test_walk(self):
        contents = list(self.bag.walk())

        self.assertEqual(contents[0][0], "") # the bag's base dir
        dirs = contents[0][1]
        self.assertIn("data", dirs)
        self.assertIn("multibag", dirs)
        self.assertIn("metadata", dirs)
        self.assertEqual(len(dirs), 3)
        files = contents[0][2]
        self.assertIn("bagit.txt", files)
        self.assertIn("bag-info.txt", files)
        self.assertIn("manifest-sha256.txt", files)
        self.assertIn("fetch.txt", files)
        self.assertIn("about.txt", files)
        self.assertEqual(len(files), 5)

        sub = [t for t in contents if t[0] == "data"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertIn("trial3", dirs)
        self.assertEqual(len(dirs), 1)
        files = sub[2]
        self.assertIn("trial1.json", files)
        self.assertIn("trial2.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "multibag"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("member-bags.tsv", files)
        self.assertIn("file-lookup.tsv", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "metadata"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("pod.json", files)
        self.assertEqual(len(files), 1)

        sub = [t for t in contents if t[0] == os.path.join("data","trial3")]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("trial3a.json", files)
        self.assertEqual(len(files), 1)

        self.assertEqual(len(contents), 5)

    def test_walk_sub(self):
        contents = list(self.bag.walk("data/trial3"))
        self.assertEqual(contents, [("data/trial3", [], ['trial3a.json'])])

        contents = list(self.bag.walk("data"))
        for d in contents:
            d[2].sort()
        self.assertEqual(len(contents), 2)
        self.assertEqual(contents[0], ("data", ["trial3"],
                                       ['trial1.json', 'trial2.json']))
        self.assertEqual(contents[1], ("data/trial3", [], ['trial3a.json']))

    def test_nonstandard(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

            self.bag = xtend.as_extended(Bag(self.bagdir))

            contents = list(self.bag.nonstandard())
            self.assertIn("about.txt", contents)
            self.assertIn(os.path.join("metadata","pod.json"), contents)
            self.assertIn(os.path.join("metadata","trial3"), contents)
            self.assertIn(os.path.join("data","trial1.json"), contents)
            self.assertIn(os.path.join("data","trial2.json"), contents)
            self.assertIn(os.path.join("data","trial3/trial3a.json"), contents)
            self.assertIn(os.path.join("multibag","member-bags.tsv"), contents)
            self.assertIn(os.path.join("multibag","file-lookup.tsv"), contents)
            self.assertEqual(len(contents), 8)

        finally:
            shutil.rmtree(tempdir)

    def test_replicate(self):
        self.bag.replicate_with_hardlink = False
        tempdir = tempfile.mkdtemp()
        try:
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir,
                                                        "bagit.txt")))
            self.assertFalse(os.path.exists(os.path.join(tempdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", tempdir)
            self.assertTrue(os.path.exists(os.path.join(tempdir, "bagit.txt")))
            self.assertFalse(ishardlink(os.path.join(tempdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    
    def test_replicate_withlink(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

            self.bag = xtend.as_extended(Bag(self.bagdir))
            self.bag.replicate_with_hardlink = True
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir,
                                                        "bagit.txt")))
            outdir = os.path.join(tempdir, "otherbag")
            self.assertFalse(os.path.exists(os.path.join(outdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", outdir)
            self.assertTrue(os.path.exists(os.path.join(outdir, "bagit.txt")))
            self.assertTrue(ishardlink(os.path.join(outdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    
    def test_replicate_dir(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            path = os.path.join("metadata", "trial3")
            srcpath = os.path.join(self.bagdir, path)
            os.mkdir(srcpath)

            self.bag = xtend.as_extended(Bag(self.bagdir))
            self.bag.replicate_with_hardlink = True
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir, path)))
            outdir = os.path.join(tempdir, "otherbag")
            self.assertFalse(os.path.exists(os.path.join(outdir, path)))
                                                         
            self.bag.replicate(path, outdir)
            self.assertTrue(os.path.exists(os.path.join(outdir, path)))
            self.assertTrue(os.path.isdir(os.path.join(outdir, path)))

        finally:
            shutil.rmtree(tempdir)

    def test_is_head_multibag(self):
        self.assertTrue(self.bag.is_head_multibag())

    def test_calc_oxum(self):
        oxum = self.bag.calc_oxum()
        self.assertTrue(isinstance(oxum, tuple))
        self.assertEqual(len(oxum), 2)
        self.assertTrue(all([isinstance(x, int) for x in oxum]))
        self.assertEqual(oxum[1], 3)
        self.assertEqual(oxum[0], 208)
    
    def test_update_oxum(self):
        del self.bag.info['Payload-Oxum']

        oxum = self.bag.update_oxum()
        self.assertTrue(isinstance(oxum, tuple))
        self.assertEqual(len(oxum), 2)
        self.assertTrue(all([isinstance(x, int) for x in oxum]))
        self.assertEqual(oxum[1], 3)
        self.assertEqual(oxum[0], 208)
    
        self.assertEqual(self.bag.info['Payload-Oxum'], "208.3")

    def test_calc_bag_size(self):
        size = self.bag.calc_bag_size()
        self.assertTrue(isinstance(size, int))
        self.assertEqual(size, 20832)

    def test_update_bag_size(self):
        size = self.bag.update_bag_size()
        self.assertTrue(isinstance(size, int))
        self.assertEqual(size, 20832)

        self.assertEqual(self.bag.info['Bag-Size'], "20.83 kB")

class TestExtendReadOnlyBag(test.TestCase):

    def setUp(self):
        self.bagroot = os.path.join(datadir, "samplembag.zip")
        self.bag = xtend.as_extended(open_bag(self.bagroot))

    def test_ctor_via_ctor(self):
        self.bag = xtend.ExtendedReadOnlyBag(Path(open_fs('zip://'+self.bagroot),
                                                  'samplembag'))
        self.test_ctor()
        self.test_exists()

    def test_ctor(self):
        self.assertTrue(isinstance(self.bag, ReadOnlyBag))
        self.assertTrue(isinstance(self.bag, xtend._ExtendedReadOnlyMixin))
        self.assertTrue(isinstance(self.bag, xtend.ExtendedReadOnlyBag))
        self.assertTrue(isinstance(self.bag._root, Path))

    def test_exists(self):
        self.assertTrue(self.bag.exists("bagit.txt"))
        self.assertTrue(self.bag.exists("data/trial1.json"))
        self.assertFalse(self.bag.exists("data/goober"))
        self.assertFalse(self.bag.exists("data/trial3/goober"))

    def test_isdir(self):
        self.assertFalse(self.bag.isdir("bagit.txt"))
        self.assertTrue(self.bag.isdir("data"))
        self.assertFalse(self.bag.isdir("data/trial1.json"))
        self.assertFalse(self.bag.isdir("data/goober"))
        self.assertTrue(self.bag.isdir("data/trial3"))
        self.assertFalse(self.bag.isdir("data/trial3/goober"))

    def test_isfile(self):
        self.assertTrue(self.bag.isfile("bagit.txt"))
        self.assertFalse(self.bag.isfile("data"))
        self.assertTrue(self.bag.isfile("data/trial1.json"))
        self.assertFalse(self.bag.isfile("data/goober"))
        self.assertFalse(self.bag.isfile("data/trial3"))
        self.assertFalse(self.bag.isfile("data/trial3/goober"))

    def test_sizeof(self):
        self.assertEqual(self.bag.sizeof("data/trial1.json"), 69)
        self.assertEqual(self.bag.sizeof("data"), 0)
        with self.assertRaises(OSError):
            self.bag.sizeof("data/goober")

    def test_timesfor(self):
        times = self.bag.timesfor("data/trial1.json")
        self.assertIsNotNone(times)
        self.assertIsNone(times.ctime)
        self.assertIsNone(times.atime)
        self.assertGreater(times.mtime, 0)

        times = self.bag.timesfor("data")
        self.assertIsNotNone(times)
        self.assertIsNone(times.ctime)
        self.assertIsNone(times.atime)
        self.assertGreater(times.mtime, 0)

        with self.assertRaises(OSError):
            self.bag.timesfor("data/goober")

    def test_bag(self):
        # test that self.bag behaves like a bagit.Bag
        # Note that this is a little different, as bagit.Bag will include the
        # full path.  
        self.assertEqual(self.bag.algs, ["sha256"])
        self.assertEqual(list(self.bag.manifest_files()),["manifest-sha256.txt"])

    def test_walk(self):
        contents = list(self.bag.walk())

        self.assertEqual(contents[0][0], "") # the bag's base dir
        dirs = contents[0][1]
        self.assertIn("data", dirs)
        self.assertIn("multibag", dirs)
        self.assertIn("metadata", dirs)
        self.assertEqual(len(dirs), 3)
        files = contents[0][2]
        self.assertIn("bagit.txt", files)
        self.assertIn("bag-info.txt", files)
        self.assertIn("manifest-sha256.txt", files)
        self.assertIn("fetch.txt", files)
        self.assertIn("about.txt", files)
        self.assertIn("preserv.log", files)
        self.assertEqual(len(files), 6)

        sub = [t for t in contents if t[0] == "data"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertIn("trial3", dirs)
        self.assertEqual(len(dirs), 1)
        files = sub[2]
        self.assertIn("trial1.json", files)
        self.assertIn("trial2.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "multibag"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("member-bags.tsv", files)
        self.assertIn("file-lookup.tsv", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "metadata"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 3)
        self.assertIn("trial1.json", dirs)
        self.assertIn("trial2.json", dirs)
        self.assertIn("trial3", dirs)
        files = sub[2]
        self.assertIn("pod.json", files)
        self.assertIn("nerdm.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == os.path.join("data","trial3")]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("trial3a.json", files)
        self.assertEqual(len(files), 1)

        self.assertEqual(len(contents), 9)

    def test_walk_sub(self):
        contents = list(self.bag.walk("data/trial3"))
        self.assertEqual(contents, [("data/trial3", [], ['trial3a.json'])])

        contents = list(self.bag.walk("data"))
        self.assertEqual(len(contents), 2)
        self.assertEqual(contents[0], ("data", ["trial3"],
                                       ['trial1.json', 'trial2.json']))
        self.assertEqual(contents[1], ("data/trial3", [], ['trial3a.json']))

    def test_nonstandard(self):
        tempdir = tempfile.mkdtemp()

        contents = list(self.bag.nonstandard())
        self.assertIn("about.txt", contents)
        self.assertIn(os.path.join("metadata","pod.json"), contents)
        self.assertIn(os.path.join("data","trial1.json"), contents)
        self.assertIn(os.path.join("data","trial2.json"), contents)
        self.assertIn(os.path.join("data","trial3/trial3a.json"), contents)
        self.assertIn(os.path.join("multibag","member-bags.tsv"), contents)
        self.assertIn(os.path.join("multibag","file-lookup.tsv"), contents)
        self.assertEqual(len(contents), 13)

    def test_replicate(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.assertTrue(self.bag.exists("bagit.txt"))
            self.assertFalse(os.path.exists(os.path.join(tempdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", tempdir)
            self.assertTrue(os.path.exists(os.path.join(tempdir, "bagit.txt")))
            self.assertFalse(ishardlink(os.path.join(tempdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    




if __name__ == '__main__':
    test.main()
