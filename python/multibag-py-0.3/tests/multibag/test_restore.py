# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test

from fs import open_fs

import bagit
import multibag.restore as restore
import multibag.split as split
import multibag.amend as amend
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

def ishardlink(path):
    return os.stat(path).st_nlink > 1

class TestRestorer(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
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

        amenddir = os.path.join(self.tempdir, "amendment")
        os.mkdir(amenddir)
        with open(os.path.join(amenddir, "trial2.json"), 'w') as fd:
            fd.write('"Goober!"\n')
        with open(os.path.join(amenddir, "trial4.json"), 'w') as fd:
            fd.write('"Gomer!"\n')
        bagit.make_bag(amenddir, checksum=['sha256'])
        amend.amend_bag_with(os.path.join(self.tempdir, "samplebag_3.mbag"),
                             amenddir, "2.0")
        with open(os.path.join(amenddir, "multibag", "deleted.txt"), 'w') as fd:
            fd.write("data/trial1.json\n")

        self.v1 = os.path.join(self.tempdir, "samplebag_3.mbag")
        self.v2 = os.path.join(self.tempdir, "amendment")

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        dest = os.path.join(self.tempdir, "restored")
        rstr = restore.BagRestorer(self.v1, dest, self.tempdir)
        self.assertEqual(rstr.destination_bagdir, os.path.join(self.tempdir, "restored"))
        self.assertEqual(rstr.head_bag.path, self.v1)
        self.assertEqual(rstr.cache_dir, self.tempdir)
        self.assertFalse(rstr._inplace)

        rstr = restore.BagRestorer(self.v1, compdir=self.tempdir)
        self.assertEqual(rstr.destination_bagdir, self.v1)
        self.assertEqual(rstr.head_bag.path, self.v1)
        self.assertEqual(rstr.cache_dir, self.tempdir)
        self.assertTrue(rstr._inplace)

        rstr = restore.BagRestorer(self.v1, fetcher=lambda a,b: os.path.join(b,a))
        self.assertEqual(rstr.destination_bagdir, self.v1)
        self.assertEqual(rstr.head_bag.path, self.v1)
        self.assertEqual(rstr.cache_dir, os.path.join(self.v1, "multibag", "_membercache"))

        with self.assertRaises(OSError):
            rstr = restore.BagRestorer("goober")
        with self.assertRaises(restore.MultibagError):
            rstr = restore.BagRestorer(os.path.join(self.tempdir, "samplebag_2.mbag"))
            
    def test_create_dest_bag(self):
        dest = os.path.join(self.tempdir, "restored")
        rstr = restore.BagRestorer(self.v1, dest, self.tempdir)
        self.assertEqual(rstr.destination_bagdir, os.path.join(self.tempdir, "restored"))
        self.assertEqual(rstr.head_bag.path, self.v1)
        self.assertEqual(rstr.cache_dir, self.tempdir)
        self.assertFalse(rstr._inplace)
        self.assertFalse(os.path.exists(rstr.destination_bagdir))

        rstr._create_dest_bag()
        self.assertTrue(os.path.exists(rstr.destination_bagdir))
        self.assertFalse(os.path.exists(os.path.join(rstr.destination_bagdir, "multibag")))

        rstr = restore.BagRestorer(self.v1, fetcher=lambda a,b: os.path.join(b,a))
        self.assertEqual(rstr.destination_bagdir, self.v1)
        self.assertEqual(rstr.head_bag.path, self.v1)
        self.assertEqual(rstr.cache_dir, os.path.join(self.v1, "multibag", "_membercache"))
        self.assertTrue(os.path.exists(rstr.destination_bagdir))
        self.assertFalse(os.path.exists(os.path.join(rstr.destination_bagdir, "multibag", "_membercache")))

        rstr._create_dest_bag()
        self.assertTrue(os.path.exists(rstr.destination_bagdir))
        self.assertTrue(os.path.exists(os.path.join(rstr.destination_bagdir, "multibag", "_membercache")))
        
    def test_find_member_bag(self):
        os.system("cd %s; zip -qr samplebag_3.mbag.zip samplebag_3.mbag" % self.tempdir)
        shutil.rmtree(os.path.join(self.tempdir, "samplebag_3.mbag"))

        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertEqual(rstr.find_member_bag("samplebag_1.mbag"),
                         os.path.join(self.tempdir, "samplebag_1.mbag"))
        self.assertEqual(rstr.find_member_bag("samplebag_3.mbag"),
                         os.path.join(self.tempdir, "samplebag_3.mbag.zip"))
        
    def test_fetch_member_bag(self):
        os.system("cd %s; zip -qr samplebag_3.mbag.zip samplebag_3.mbag" % self.tempdir)
        self.assertTrue(os.path.isfile(os.path.join(self.tempdir, "samplebag_3.mbag.zip")))
        shutil.rmtree(os.path.join(self.tempdir, "samplebag_3.mbag"))
        self.assertTrue(not os.path.isdir(os.path.join(self.tempdir, "samplebag_3.mbag")))

        def ftchr(bag, todir):
            zipd = os.path.join(self.tempdir, bag+".zip")
            if os.path.exists(zipd):
                os.system("cd %s; unzip -q %s" % (todir, zipd))
                return os.path.join(todir, os.path.splitext(os.path.basename(zipd))[0])
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir, fetcher=ftchr)

        found = rstr._fetch_member_bag("samplebag_3.mbag")
        self.assertEqual(found, os.path.join(self.tempdir, "samplebag_3.mbag"))
        self.assertTrue(os.path.isdir(os.path.join(self.tempdir, "samplebag_3.mbag")))

    def test_get_member_bag(self):
        lts = os.path.join(self.tempdir, "remote")
        os.mkdir(lts)
        os.system("cd %s; zip -qr %s/samplebag_3.mbag.zip samplebag_3.mbag" % (self.tempdir, lts))
        self.assertTrue(os.path.isfile(os.path.join(lts, "samplebag_3.mbag.zip")))
        shutil.rmtree(os.path.join(self.tempdir, "samplebag_3.mbag"))
        self.assertTrue(not os.path.isdir(os.path.join(self.tempdir, "samplebag_3.mbag")))

        def ftchr(bag, todir):
            zipd = os.path.join(self.tempdir, "remote", bag+".zip")
            if os.path.exists(zipd):
                os.system("cd %s; unzip -q %s" % (todir, zipd))
                return os.path.join(todir, os.path.splitext(os.path.basename(zipd))[0])
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir, fetcher=ftchr)

        found = rstr.get_member_bag("samplebag_1.mbag")
        self.assertEqual(found, os.path.join(self.tempdir, "samplebag_1.mbag"))
        found = rstr.get_member_bag("samplebag_3.mbag")
        self.assertEqual(found, os.path.join(self.tempdir, "samplebag_3.mbag"))
        self.assertTrue(os.path.isdir(os.path.join(self.tempdir, "samplebag_3.mbag")))

    def test_restore_member(self):
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))

        with self.assertRaises(OSError):
            rstr.restore_member("sample_3.mbag")
            
        rstr.restore_member("samplebag_3.mbag")
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3", "trial3a.json")))

        with open(os.path.join(self.v2,"data","trial2.json")) as fd:
            content = fd.read()
        self.assertEqual('"Goober!"\n', content)

    def test_restore_member_overwrite(self):
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))

        rstr.restore_member("samplebag_3.mbag", overwrite=True)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3", "trial3a.json")))

        with open(os.path.join(self.v2,"data","trial2.json")) as fd:
            content = fd.read()
        self.assertNotEqual('"Goober!"\n', content)
        self.assertTrue(content.startswith('{'))
        
    def test_restore_member_from_zip(self):
        os.system("cd %s; zip -qr samplebag_3.mbag.zip samplebag_3.mbag" % self.tempdir)
        shutil.rmtree(os.path.join(self.tempdir, "samplebag_3.mbag"))

        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))

        with self.assertRaises(OSError):
            rstr.restore_member("sample_3.mbag")
            
        rstr.restore_member("samplebag_3.mbag")
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial3", "trial3a.json")))

        with open(os.path.join(self.v2,"data","trial2.json")) as fd:
            content = fd.read()
        self.assertEqual('"Goober!"\n', content)

    def test_restore_fetch(self):
        with open(os.path.join(self.tempdir, "samplebag_1.mbag", "fetch.txt"), 'w') as fd:
            fd.write("https://example.com/u1 5 u1\n")
            fd.write("https://example.com/u2 5 u2\n")
        with open(os.path.join(self.tempdir, "samplebag_3.mbag", "fetch.txt"), 'w') as fd:
            fd.write("https://example.com/u1.r 5 u1\n")
            fd.write("https://example.com/u3 5 u3\n")
            fd.write("https://example.com/u4 5 u4\n")
        with open(os.path.join(self.tempdir, "amendment", "fetch.txt"), 'w') as fd:
            fd.write("https://example.com/u1.r2 5 u1\n")
            fd.write("https://example.com/u3.r 5 u3\n")
            fd.write("https://example.com/u5 5 u5\n")
        
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))

        rstr.restore_fetch()
        self.assertTrue(os.path.isfile(os.path.join(rstr._destdir, "fetch.txt")))
        with open(os.path.join(rstr._destdir, "fetch.txt")) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 5)
        self.assertEqual(lines[0], "https://example.com/u1.r2 5 u1\n")
        self.assertEqual(lines[1], "https://example.com/u2 5 u2\n")
        self.assertEqual(lines[2], "https://example.com/u3.r 5 u3\n")
        self.assertEqual(lines[3], "https://example.com/u4 5 u4\n")
        self.assertEqual(lines[4], "https://example.com/u5 5 u5\n")

        rstr = restore.BagRestorer(self.v1, compdir=self.tempdir)
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))

        rstr.restore_fetch()
        self.assertTrue(os.path.isfile(os.path.join(rstr._destdir, "fetch.txt")))
        with open(os.path.join(rstr._destdir, "fetch.txt")) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "https://example.com/u1.r 5 u1\n")
        self.assertEqual(lines[1], "https://example.com/u2 5 u2\n")
        self.assertEqual(lines[2], "https://example.com/u3 5 u3\n")
        self.assertEqual(lines[3], "https://example.com/u4 5 u4\n")

    def test_restore(self):
        restored = os.path.join(self.tempdir, "restored")
        rstr = restore.BagRestorer(self.v1, restored, compdir=self.tempdir)
        self.assertTrue(not os.path.exists(restored))

        rstr.restore()
        self.assertTrue(os.path.isdir(restored))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial1.json")))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial2.json")))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial3", "trial3a.json")))
        self.assertTrue(not os.path.isfile(os.path.join(restored,"data","trial4.json")))

        restoredbag = open_bag(restored)
        restoredbag.validate()
        
        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(self.v2))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial1.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial4.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))
        
        rstr.restore()
        self.assertTrue(os.path.isdir(self.v2))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial3", "trial3a.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial4.json")))
        self.assertTrue(not os.path.isfile(os.path.join(self.v2,"data","trial1.json")))
        with open(os.path.join(self.v2,"data","trial2.json")) as fd:
            content = fd.read()
        self.assertEqual('"Goober!"\n', content)

        restoredbag = open_bag(self.v2)
        restoredbag.validate()
        
    def test_restore_zipd(self):
        os.system("cd %s; zip -qr samplebag_3.mbag.zip samplebag_3.mbag" % self.tempdir)
        shutil.rmtree(os.path.join(self.tempdir, "samplebag_3.mbag"))

        restored = os.path.join(self.tempdir, "restored")
        rstr = restore.BagRestorer(self.v1+".zip", restored, compdir=self.tempdir)
        self.assertTrue(not os.path.exists(restored))

        rstr.restore()
        self.assertTrue(os.path.isdir(restored))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial1.json")))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial2.json")))
        self.assertTrue(os.path.isfile(os.path.join(restored,"data","trial3", "trial3a.json")))
        self.assertTrue(not os.path.isfile(os.path.join(restored,"data","trial4.json")))

        restoredbag = open_bag(self.v2)
        restoredbag.validate()

        rstr = restore.BagRestorer(self.v2, compdir=self.tempdir)
        self.assertTrue(os.path.exists(self.v2))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial1.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.exists(os.path.join(self.v2,"data","trial4.json")))
        self.assertTrue(not os.path.exists(os.path.join(self.v2,"data","trial3")))
        
        rstr.restore()
        self.assertTrue(os.path.isdir(self.v2))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial2.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial3", "trial3a.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.v2,"data","trial4.json")))
        self.assertTrue(not os.path.isfile(os.path.join(self.v2,"data","trial1.json")))
        with open(os.path.join(self.v2,"data","trial2.json")) as fd:
            content = fd.read()
        self.assertEqual('"Goober!"\n', content)

        restoredbag = open_bag(self.v2)
        restoredbag.validate()
        

if __name__ == '__main__':
    test.main()
        
