# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, tempfile, shutil
import unittest as test
from functools import cmp_to_key

import multibag.amend as amend
import multibag.access.bagit as bagit
import multibag.access.multibag as mbag
import multibag.validate as valid8
from multibag.constants import CURRENT_VERSION, CURRENT_REFERENCE
import multibag.testing.mkdata as mkdata

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

class TestSingleMutlibagMaker(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
        shutil.rmtree(os.path.join(self.bagdir, "multibag"))
        bag = bagit.Bag(self.bagdir)
        rmtag = []
        for tag in bag.info:
            if tag.startswith('Multibag-'):
                rmtag.append(tag)
        for tag in rmtag:
            del bag.info[tag]
        bag.save()
                
        self.mkr = amend.SingleMultibagMaker(self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertEqual(self.mkr.bagdir, self.bagdir)
        self.assertEqual(self.mkr.bag.multibag_tag_dir, "multibag")

        self.mkr = amend.SingleMultibagMaker(self.bagdir, "goober")
        self.assertEqual(self.mkr.bag.multibag_tag_dir, "goober")

    def test_update_info(self):
        # test assumption
        bag = bagit.Bag(self.bagdir)
        for tag in bag.info:
            self.assertFalse(tag.startswith('Multibag-'))

        self.mkr.update_info()
        bag = bagit.Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         amend.CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "4.875 kB")

    def test_write_member_bags(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_member_bags()
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0].strip(), os.path.basename(self.bagdir))

    def test_write_member_bags_alt(self):
        self.mkr = amend.SingleMultibagMaker(self.bagdir, "goober")
        mbdir = os.path.join(self.bagdir,"goober")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_member_bags("doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))
        
        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertEqual(len(lines), 1)
        parts = lines[0].strip().split("\t")
        self.assertEqual(parts[0], os.path.basename(self.bagdir))
        self.assertEqual(parts[1], "doi:XXXX/11111")

    def test_write_file_lookup(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'file-lookup.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_file_lookup()
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        bagn = os.path.basename(self.bagdir)
        lu = {}
        with open(mbfile) as fd:
            i = 0
            for line in fd:
                parts = line.strip().split('\t')
                i += 1
                self.assertEqual(len(parts), 2,
                   "Expecting each line from file-lookup.tsv to have 2 fields: "+
                                 line.strip())
                self.assertEqual(parts[1], bagn)
                self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                            parts[0])))
                self.assertTrue(parts[0].startswith("data/"))
        self.assertEqual(i, 3)

        self.mkr.write_file_lookup("metadata about.txt junk.json data/trial1.json".split())
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertIn("about.txt\t"+bagn+"\n", lines)
        self.assertIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertNotIn("junk.json\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 5)

        self.mkr.write_file_lookup("data metadata/pod.json".split(),
                                   "data/trial1.json metadata".split(),
                                   trunc=True)
        with open(mbfile) as fd:
            lines = fd.readlines()
        self.assertNotIn("data/trial1.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial2.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial3/trial3a.json\t"+bagn+"\n", lines)
        self.assertIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 3)

    def test_convert(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        flfile = os.path.join(mbdir,'file-lookup.tsv')
        bagn = os.path.basename(self.bagdir)
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.convert("1.5", "doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))

        # test for member-bags.tsv
        self.assertTrue(os.path.exists(mbfile))
        with open(mbfile) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 1)
        parts = lines[0].strip().split("\t")
        self.assertEqual(parts[0], bagn)
        self.assertEqual(parts[1], "doi:XXXX/11111")

        # test for file-lookup.tsv
        self.assertTrue(os.path.exists(flfile))
        with open(flfile) as fd:
            lines = fd.readlines()
        self.assertIn("data/trial1.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial2.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial3/trial3a.json\t"+bagn+"\n", lines)
        self.assertNotIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertNotIn("about.txt\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 3)

        # test info tag data
        bag = bagit.Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         amend.CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.5")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "5.171 kB")

    def test_convert_new(self):
        # create the data
        self.bagdir = os.path.join(self.tempdir, "sampledata")
        self.assertTrue(not os.path.isdir(self.bagdir))
        dm = mkdata.DatasetMaker(self.bagdir,
                                 { 'totalsize': 15, 'totalfiles': 3,
                                   'files': [{
                                       'totalsize': 10, 'totalfiles': 2
                                   }], 'dirs': [{
                                       'totalsize': 5, 'totalfiles': 1
                                   }]
                                 })
        dm.fill()
        self.assertTrue(os.path.isdir(self.bagdir))

        # turn it into a bag
        bag = bagit.make_bag(self.bagdir)
        self.assertTrue(bag.validate())

        mbdir = os.path.join(self.bagdir,'multibag')
        self.assertTrue(not os.path.exists(mbdir))

        # convert it to a multibag
        self.mkr = amend.SingleMultibagMaker(self.bagdir)
        self.mkr.convert("1.5", "doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))

        # validate it as a head bag
        valid8.validate_headbag(self.bagdir)

    def test_make_single_multibag(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        flfile = os.path.join(mbdir,'file-lookup.tsv')
        bagn = os.path.basename(self.bagdir)
        self.assertTrue(not os.path.exists(mbdir))

        amend.make_single_multibag(self.bagdir, "1.5", "doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))

        # test for member-bags.tsv
        self.assertTrue(os.path.exists(mbfile))
        with open(mbfile) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 1)
        parts = lines[0].strip().split("\t")
        self.assertEqual(parts[0], bagn)
        self.assertEqual(parts[1], "doi:XXXX/11111")

        # test for file-lookup.tsv
        self.assertTrue(os.path.exists(flfile))
        with open(flfile) as fd:
            lines = fd.readlines()
        self.assertIn("data/trial1.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial2.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial3/trial3a.json\t"+bagn+"\n", lines)
        self.assertNotIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertNotIn("about.txt\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 3)

        # test info tag data
        bag = bagit.Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         amend.CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.5")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "5.171 kB")

        

class TestAmender(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.amendment = os.path.join(self.tempdir, "updatebag")
        os.mkdir(self.amendment)
        srcfile = os.path.join(datadir, "samplembag", "data", "trial1.json")
        shutil.copy(srcfile, self.amendment)
        subdir = os.path.join(self.amendment, "trial3")
        os.mkdir(subdir)
        shutil.copy(srcfile, subdir)

        bagit.make_bag(self.amendment)

        self.amendee = os.path.join(datadir, "samplembag.zip")
        self.amender = amend.Amender(self.amendee, self.amendment)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertTrue(isinstance(self.amender._oldhead, bagit.Bag))
        self.assertTrue(self.amender._oldhead.is_head_multibag)
        self.assertTrue(isinstance(self.amender._newhead, mbag.HeadBag))
        self.assertIsNone(self.amender._pid)
        self.assertIsNone(self.amender._comm)
        self.assertEqual(self.amender._info, [])

        self.amender = amend.Amender(self.amendee, self.amendment, "foo://goob")
        self.assertTrue(isinstance(self.amender._oldhead, bagit.Bag))
        self.assertTrue(self.amender._oldhead.is_head_multibag)
        self.assertTrue(isinstance(self.amender._newhead, mbag.HeadBag))
        self.assertEqual(self.amender._pid, "foo://goob")
        self.assertIsNone(self.amender._comm)
        self.assertEqual(self.amender._info, [])

        self.amender = amend.Amender(self.amendee, self.amendment, "foo://goob",
                                     "this is version 3")
        self.assertTrue(isinstance(self.amender._oldhead, bagit.Bag))
        self.assertTrue(self.amender._oldhead.is_head_multibag)
        self.assertTrue(isinstance(self.amender._newhead, mbag.HeadBag))
        self.assertEqual(self.amender._pid, "foo://goob")
        self.assertEqual(self.amender._comm, "this is version 3")
        self.assertEqual(self.amender._info, [])

        info = ["a","b"]
        self.amender = amend.Amender(self.amendee, self.amendment, "foo://goob",
                                     "this is version 2", info)
        self.assertTrue(isinstance(self.amender._oldhead, bagit.Bag))
        self.assertTrue(self.amender._oldhead.is_head_multibag)
        self.assertTrue(isinstance(self.amender._newhead, mbag.HeadBag))
        self.assertEqual(self.amender._pid, "foo://goob")
        self.assertEqual(self.amender._comm, "this is version 2")
        self.assertEqual(self.amender._info, ["a","b"])
        info[1] = "c"
        self.assertEqual(self.amender._info, ["a","b"])
        
    def test_init_member_bags(self):
        membagsfile = os.path.join(self.amender._newheaddir,"multibag",
                                   "member-bags.tsv")
        self.assertTrue(not os.path.exists(membagsfile))

        self.amender._init_multibag_info()
        self.amender._init_member_bags()
        self.assertTrue(not os.path.exists(membagsfile))
        self.assertEqual(self.amender._newhead.member_bag_names, ["samplembag"])

    def test_init_member_bags2(self):
        # test when the amendee is not natively a head bag
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag", "data")
        shutil.copytree(src, self.amendee)
        bagit.make_bag(self.amendee)
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        membagsfile = os.path.join(self.amender._newheaddir,"multibag",
                                   "member-bags.tsv")
        self.assertTrue(not os.path.exists(membagsfile))

        self.amender._init_multibag_info()
        self.amender._init_member_bags()
        self.assertTrue(not os.path.exists(membagsfile))
        self.assertEqual(self.amender._newhead.member_bag_names, ["gooberbag"])

    def test_init_member_bags3(self):
        # test when the amendment happens to be head-bag conformant
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag", "data")
        shutil.copytree(src, self.amendee)
        bagit.make_bag(self.amendee)
        amend.make_single_multibag(self.amendment)
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        membagsfile = os.path.join(self.amender._newheaddir,"multibag",
                                   "member-bags.tsv")
        self.assertTrue(os.path.exists(membagsfile))

        self.amender._init_multibag_info()
        self.amender._init_member_bags()
        self.assertTrue(not os.path.exists(membagsfile))
        self.assertEqual(self.amender._newhead.member_bag_names, ["gooberbag"])

    def test_init_file_lookup(self):
        lufile = os.path.join(self.amender._newheaddir,"multibag",
                              "file-lookup.tsv")
        self.assertTrue(not os.path.exists(lufile))

        self.amender._init_multibag_info()
        self.amender._init_file_lookup()
        self.assertTrue(not os.path.exists(lufile))
        self.assertEqual(self.amender._newhead.lookup_file("data/trial1.json"),
                         "samplembag")
        
    def test_init_file_lookup2(self):
        # test when the amendee is not natively a head bag
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag", "data")
        shutil.copytree(src, self.amendee)
        bagit.make_bag(self.amendee)
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        lufile = os.path.join(self.amender._newheaddir,"multibag",
                              "file-lookup.tsv")
        self.assertTrue(not os.path.exists(lufile))

        self.amender._init_multibag_info()
        self.amender._init_file_lookup()
        self.assertTrue(not os.path.exists(lufile))
        self.assertEqual(self.amender._newhead.lookup_file("data/trial1.json"),
                         "gooberbag")
        
    def test_init_file_lookup3(self):
        # test when the amendment happens to be head-bag conformant
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag", "data")
        shutil.copytree(src, self.amendee)
        bagit.make_bag(self.amendee)
        amend.make_single_multibag(self.amendment)
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        lufile = os.path.join(self.amender._newheaddir,"multibag",
                              "file-lookup.tsv")
        self.assertTrue(os.path.exists(lufile))

        self.amender._init_multibag_info()
        self.amender._init_file_lookup()
        self.assertTrue(not os.path.exists(lufile))
        self.assertEqual(self.amender._newhead.lookup_file("data/trial1.json"),
                         "gooberbag")

    def test_init_multibag_info(self):
        self.assertNotIn('Multibag-Head-Deprecates', self.amender._newhead.info)
        self.assertNotIn('Multibag-Version', self.amender._newhead.info)
        
        self.amender._init_multibag_info()
        self.assertEqual(self.amender._newhead.info.get('Multibag-Version'),
                                                        amend.CURRENT_VERSION)
        self.assertEqual(
            self.amender._newhead.info.get('Multibag-Head-Deprecates'),
            ["1.0"])

    def test_init_multibag_info2(self):
        # test when src has a deprecation
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag")
        shutil.copytree(src, self.amendee)
        bag = bagit.Bag(self.amendee)
        bag.info['Multibag-Head-Deprecates'] = "0.5"
        bag.save()
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        self.assertNotIn('Multibag-Head-Deprecates', self.amender._newhead.info)
        
        self.amender._init_multibag_info()
        self.assertEqual(
            self.amender._newhead.info.get('Multibag-Head-Deprecates'),
            ["0.5", "1.0"])

    def test_init_multibag_info3(self):
        # test when src has deprecations
        self.amendee = os.path.join(self.tempdir, "gooberbag")
        src = os.path.join(datadir, "samplembag")
        shutil.copytree(src, self.amendee)
        bag = bagit.Bag(self.amendee)
        bag.info['Multibag-Head-Deprecates'] = ["0.1", "0.5"]
        bag.save()
        self.amender = amend.Amender(self.amendee, self.amendment)
        
        self.assertNotIn('Multibag-Head-Deprecates', self.amender._newhead.info)
        
        self.amender._init_multibag_info()
        self.assertEqual(
            self.amender._newhead.info.get('Multibag-Head-Deprecates'),
            ["0.1", "0.5", "1.0"])

    def test_init_from_amendee(self):
        
        membagsfile = os.path.join(self.amender._newheaddir,"multibag",
                                   "member-bags.tsv")
        self.assertTrue(not os.path.exists(membagsfile))
        lufile = os.path.join(self.amender._newheaddir,"multibag",
                              "file-lookup.tsv")
        self.assertTrue(not os.path.exists(lufile))
        self.assertNotIn('Multibag-Head-Deprecates', self.amender._newhead.info)

        self.amender.init_from_amendee()

        self.assertTrue(not os.path.exists(membagsfile))
        self.assertEqual(self.amender._newhead.member_bag_names, ["samplembag"])
        self.assertTrue(not os.path.exists(lufile))
        self.assertEqual(self.amender._newhead.lookup_file("data/trial1.json"),
                         "samplembag")
        self.assertEqual(
            self.amender._newhead.info.get('Multibag-Head-Deprecates'),
            ["1.0"])

    def test_add_amending_bag(self):
        xtrabag = os.path.join(self.tempdir, "gooberbag")
        os.mkdir(xtrabag)
        srcfile = os.path.join(datadir, "samplembag", "data", "trial2.json")
        shutil.copy(srcfile, xtrabag)
        bagit.make_bag(xtrabag)

        self.amender.init_from_amendee()
        self.amender.add_amending_bag(xtrabag, pid="foo://goob", comment="Ya")
        self.assertEqual(self.amender._newhead.member_bag_names,
                         ["samplembag", "gooberbag"])
        self.assertEqual(self.amender._newhead.member_bags()[1].uri,
                         "foo://goob")
        self.assertEqual(self.amender._newhead.lookup_file("data/trial1.json"),
                         "samplembag")
        self.assertEqual(self.amender._newhead.lookup_file("data/trial2.json"),
                         "gooberbag")

    def test_finalize(self):
        self.amender.init_from_amendee()
        self.amender.finalize("3.1")

        bag = bagit.open_bag(self.amendment)
        self.assertEqual(bag.info.get('Multibag-Version'), CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), 'multibag')
        self.assertEqual(bag.info.get('Multibag-Head-Version'), '3.1')
        self.assertEqual(bag.info.get('Multibag-Head-Deprecates'), '1.0')

        tagfile = os.path.join(self.amendment,"multibag","member-bags.tsv")
        with open(tagfile) as fd:
            names = [line.strip() for line in fd]
        self.assertEquals(names, ["samplembag", "updatebag"])

        tagfile = os.path.join(self.amendment,"multibag","file-lookup.tsv")
        lu = {}
        nol = 0
        with open(tagfile) as fd:
            for line in fd:
                names = line.strip().split('\t')
                nol += 1
                lu[names[0]] = names[1]

        self.assertEqual(lu.get('data/trial1.json'), 'updatebag')
        self.assertEqual(lu.get('data/trial2.json'), 'samplembag')
        self.assertEqual(lu.get('data/trial3/trial3a.json'), 'samplembag')
        self.assertEqual(lu.get('data/trial3/trial1.json'), 'updatebag')
        self.assertEqual(nol, len(lu))

        # validate it as a headbag
        valid8.validate_headbag(self.amendment)
        
    def test_amend_bag_with(self):
        xtrabag1 = os.path.join(self.tempdir, "gooberbag1")
        os.mkdir(xtrabag1)
        srcfile = os.path.join(datadir, "samplembag", "data", "trial2.json")
        shutil.copy(srcfile, xtrabag1)
        bagit.make_bag(xtrabag1)
        
        xtrabag2 = os.path.join(self.tempdir, "gooberbag2")
        os.mkdir(xtrabag2)
        srcfile = os.path.join(datadir, "samplembag", "data", "trial2.json")
        shutil.copy(srcfile, os.path.join(xtrabag2, "trial4.json"))
        bagit.make_bag(xtrabag2)

        amend.amend_bag_with(self.amendee, self.amendment, "2",
                             xtrabag1, xtrabag2)

        bag = bagit.open_bag(self.amendment)
        self.assertEqual(bag.info.get('Multibag-Version'), CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), 'multibag')
        self.assertEqual(bag.info.get('Multibag-Head-Version'), '2')
        self.assertEqual(bag.info.get('Multibag-Head-Deprecates'), '1.0')

        tagfile = os.path.join(self.amendment,"multibag","member-bags.tsv")
        with open(tagfile) as fd:
            names = [line.strip() for line in fd]
            self.assertEquals(names, ["samplembag", "gooberbag1",
                                      "gooberbag2", "updatebag"])

        tagfile = os.path.join(self.amendment,"multibag","file-lookup.tsv")
        lu = {}
        nol = 0
        with open(tagfile) as fd:
            for line in fd:
                names = line.strip().split('\t')
                nol += 1
                lu[names[0]] = names[1]

        self.assertEqual(lu.get('data/trial1.json'), 'updatebag')
        self.assertEqual(lu.get('data/trial2.json'), 'gooberbag1')
        self.assertEqual(lu.get('data/trial4.json'), 'gooberbag2')
        self.assertEqual(lu.get('data/trial3/trial3a.json'), 'samplembag')
        self.assertEqual(lu.get('data/trial3/trial1.json'), 'updatebag')
        self.assertEqual(nol, len(lu))

        # validate it as a headbag
        valid8.validate_headbag(self.amendment)
        
        
        
        

if __name__ == '__main__':
    test.main()
