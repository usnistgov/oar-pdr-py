# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test
from collections import OrderedDict

import fs
from fs import open_fs

import multibag.access.multibag as mb
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag
from multibag.constants import CURRENT_VERSION, CURRENT_REFERENCE

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
samplembag = os.path.join(datadir, "samplembag")

class TestMemberInfo(test.TestCase):

    def test_ctor(self):
        mi = mb.MemberInfo("goob er")
        self.assertEqual(mi.name, "goob er")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("g urn", "ivo://ncsa.vo/gurn")
        self.assertEqual(mi.name, "g urn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("fooman", "ivo://ncsa.vo/fooman", "Hey!")
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertEqual(mi.comment, "Hey!")
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("barman", "ivo://ncsa.vo/barman", "You!",
                           "goob=gurn", "blueberry", "https://example.com")
        self.assertEqual(mi.name, "barman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/barman")
        self.assertEqual(mi.comment, "You!")
        self.assertEqual(mi.info,
                         ["goob=gurn", "blueberry", "https://example.com"])

    def test_parse_line_03(self):
        # note that the purpose for TSV format is to support spaces in names

        line = "goob er\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "goob er")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "g urn\tivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "g urn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])

        line = "fooman\tivo://ncsa.vo/fooman\t# Hey!\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertEqual(mi.comment, "Hey!")
        self.assertEqual(mi.info, [])

        line = "fooman\tivo://ncsa.vo/fooman\t Hey!\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [" Hey!"])
        
        line = "barman\tivo://ncsa.vo/barman\tgoob=gurn\tblueberry\thttps://example.com\t# You!"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "barman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/barman")
        self.assertEqual(mi.comment, "You!")
        self.assertEqual(mi.info,
                         ["goob=gurn", "blueberry", "https://example.com"])

        line = "\n"
        with self.assertRaises(mb.MultibagError):
            mi = mb.MemberInfo.parse_line_03(line)
        
    def test_parse_line_02(self):

        line = "goober\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "goober")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "goob er\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "goob")
        self.assertEqual(mi.uri, "er")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "gurn ivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "gurn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "g urn ivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "g")
        self.assertEqual(mi.uri, "urn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])

        line = "\n"
        with self.assertRaises(mb.MultibagError):
            mi = mb.MemberInfo.parse_line_02(line)

    def test_parse_file(self):
        mbf = os.path.join(samplembag, "multibag", "member-bags.tsv")
        lu = OrderedDict()
        with open(mbf) as fd:
            for line in fd:
                if line.split():
                    mi = mb.MemberInfo.parse_line_03(line)
                    lu[mi.name] = mi

        self.assertEqual(list(lu.keys()), ["samplembag"])
        self.assertIsNone(lu["samplembag"].uri)
        self.assertIsNone(lu["samplembag"].comment)
        self.assertEqual(lu["samplembag"].info, [])
        
        
class TestParseFileLookup(test.TestCase):

    def test_parse_file_lookup_line_03(self):
        # note that the purpose for TSV format is to support spaces in names

        line = "data/goo ber.txt\theadbag\n"
        pair = mb.parse_file_lookup_line_03(line)
        self.assertEqual(pair, ("data/goo ber.txt", "headbag"))

    def test_parse_group_directory_line(self):
        
        line = "data/goober.txt headbag\n"
        pair = mb.parse_group_directory_line(line)
        self.assertEqual(pair, ("data/goober.txt", "headbag"))

        line = "data/goo ber.txt\theadbag\n"
        pair = mb.parse_group_directory_line(line)
        self.assertEqual(pair, ("data/goo", "ber.txt\theadbag"))

    def test_parse_file(self):
        mbf = os.path.join(samplembag, "multibag", "file-lookup.tsv")
        lu = OrderedDict()
        with open(mbf) as fd:
            for line in fd:
                if line.split():
                    mi = mb.parse_file_lookup_line_03(line)
                    lu[mi[0]] = mi[1]

        files = list(lu.keys())
        self.assertEqual(files, [
            "data/trial1.json", "data/trial2.json", "data/trial3/trial3a.json",
            "metadata/pod.json", "metadata/nerdm.json"
        ])
        for f in files[:4]:
            self.assertEqual(lu[f], "samplembag")
        self.assertEqual(lu[files[-1]], "samplembag2")

class TestFunctions(test.TestCase):

    def test_parse_deleted_line_04(self):
        line = "data/goo ber.txt\n"
        self.assertEqual(mb.parse_deleted_line_04(line), "data/goo ber.txt")
    
class TestReadOnlyHeadBag(test.TestCase):

    def setUp(self):
        self.bagfile = os.path.join(datadir, "samplembag.zip")
        self.fs = fs.zipfs.ZipFS(self.bagfile)
        self.path = Path(self.fs, "samplembag", "samplembag.zip:samplembag/")
        self.bag = mb.ReadOnlyHeadBag(self.path)
        
    def test_ctor(self):
        self.assertTrue(isinstance(self.bag, mb.HeadBagReadMixin))
        self.assertTrue(self.bag.is_readonly)
        self.assertTrue(self.bag.is_head_multibag())

    def test_version(self):
        self.assertEqual(self.bag.head_version, "1.0")

    def test_profile_version(self):
        self.assertEqual(self.bag.profile_version, "0.4")

    def test_multibag_tag_dir(self):
        self.assertEqual(self.bag.multibag_tag_dir, "multibag")
        self.bag.info['Multibag-Tag-Directory'] = 'goober'
        self.assertEqual(self.bag.multibag_tag_dir, "goober")
        del self.bag.info['Multibag-Tag-Directory']
        self.assertEqual(self.bag.multibag_tag_dir, "multibag")

    def test_missing_req_item(self):
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = []
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = ['1', '2']
        self.assertEqual(self.bag._get_required_info_item('Goober-Name'), '2')

    def test_iter_member_bags(self):
        mis = list(self.bag.iter_member_bags())
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])
        
    def test_member_bags(self):
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])

        self.bag._memberbags.append(mb.MemberInfo("goob"))
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 2)
        mis = self.bag.member_bags(True)
        self.assertEqual(len(mis), 1)
        
        
    def test_member_bag_names(self):
        names = self.bag.member_bag_names
        self.assertEqual(names, ['samplembag'])

    def test_iter_file_lookup(self):
        lu = OrderedDict(self.bag.iter_file_lookup())
        files = list(lu.keys())
        self.assertEqual(files, [
            "data/trial1.json", "data/trial2.json", "data/trial3/trial3a.json",
            "metadata/pod.json", "metadata/nerdm.json"
        ])
        for f in files[:4]:
            self.assertEqual(lu[f], "samplembag")
        self.assertEqual(lu[files[-1]], "samplembag2")

    def test_lookup_file(self):
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("metadata/nerdm.json"),
                         "samplembag2")
        self.bag._filelu["data/trial1.json"] = "samplembag2"
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/trial1.json", True),
                         "samplembag")

    def test_files_in_member(self):
        self.assertEqual(self.bag.files_in_member("samplembag"),
  "data/trial1.json data/trial2.json data/trial3/trial3a.json metadata/pod.json"
                         .split())
        self.assertEqual(self.bag.files_in_member("samplembag2"),
                         ["metadata/nerdm.json"])

    def test_iter_deleted(self):
        files = list(self.bag.iter_deleted())
        self.assertEqual(files, [])

class TestReadWriteHeadBag(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
        self.bag = mb.HeadBag(self.bagdir)

    def clear_multibag(self):
        mbdir = os.path.join(self.bagdir, "multibag")
        if os.path.isdir(mbdir):
            shutil.rmtree(mbdir)
        self.bag = mb.HeadBag(self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertTrue(isinstance(self.bag, mb.HeadBagReadMixin))
        self.assertTrue(isinstance(self.bag, mb.HeadBagUpdateMixin))
        self.assertFalse(self.bag.is_readonly)
        self.assertTrue(self.bag.is_head_multibag())

    # replicating all read tests from TestReadOnlyHeadBag

    def test_version(self):
        self.assertEqual(self.bag.head_version, "1.0")

    def test_profile_version(self):
        self.assertEqual(self.bag.profile_version, "0.4")

    def test_multibag_tag_dir(self):
        self.assertEqual(self.bag.multibag_tag_dir, "multibag")
        self.bag.info['Multibag-Tag-Directory'] = 'goober'
        self.assertEqual(self.bag.multibag_tag_dir, "goober")
        del self.bag.info['Multibag-Tag-Directory']
        self.assertEqual(self.bag.multibag_tag_dir, "multibag")

    def test_missing_req_item(self):
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = []
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = ['1', '2']
        self.assertEqual(self.bag._get_required_info_item('Goober-Name'), '2')

    def test_iter_member_bags(self):
        mis = list(self.bag.iter_member_bags())
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])
        
    def test_member_bags(self):
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])

        self.bag._memberbags.append(mb.MemberInfo("goob"))
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 2)
        mis = self.bag.member_bags(True)
        self.assertEqual(len(mis), 1)
        
        
    def test_member_bag_names(self):
        names = self.bag.member_bag_names
        self.assertEqual(names, ['samplembag'])

    def test_iter_file_lookup(self):
        lu = OrderedDict(self.bag.iter_file_lookup())
        files = list(lu.keys())
        self.assertEqual(files, [
            "data/trial1.json", "data/trial2.json", "data/trial3/trial3a.json",
            "metadata/pod.json", "metadata/nerdm.json"
        ])
        for f in files[:4]:
            self.assertEqual(lu[f], "samplembag")
        self.assertEqual(lu[files[-1]], "samplembag2")

    def test_lookup_file(self):
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("metadata/nerdm.json"),
                         "samplembag2")
        self.bag._filelu["data/trial1.json"] = "samplembag2"
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/trial1.json", True),
                         "samplembag")

    def test_iter_deleted(self):
        files = list(self.bag.iter_deleted())
        self.assertEqual(files, [])

    # now the write interface

    def test_set_multibag_tag_dir(self):
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'multibag')
        self.assertEqual(self.bag.multibag_tag_dir, 'multibag')
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                    'member-bags.tsv')))

        self.bag.set_multibag_tag_dir('multibag', True)
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'multibag')
        self.assertEqual(self.bag.multibag_tag_dir, 'multibag')
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                    'member-bags.tsv')))

        self.bag.set_multibag_tag_dir('goob', False)
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'goob')
        self.assertEqual(self.bag.multibag_tag_dir, 'goob')
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                    'member-bags.tsv')))

        self.bag.set_multibag_tag_dir('multibag', False)
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'multibag')
        self.assertEqual(self.bag.multibag_tag_dir, 'multibag')
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                    'member-bags.tsv')))

        self.bag.set_multibag_tag_dir('goob', True)
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'goob')
        self.assertEqual(self.bag.multibag_tag_dir, 'goob')
        self.assertFalse(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                     'member-bags.tsv')))
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'goob',
                                                    'member-bags.tsv')))

        os.mkdir(os.path.join(self.bagdir, "gurn"))
        with self.assertRaises(RuntimeError):
            self.bag.set_multibag_tag_dir('gurn', True)
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'goob',
                                                    'member-bags.tsv')))

        self.bag.set_multibag_tag_dir('multibag', True)
        self.assertEqual(self.bag.info['Multibag-Tag-Directory'], 'multibag')
        self.assertEqual(self.bag.multibag_tag_dir, 'multibag')
        self.assertTrue(os.path.isfile(os.path.join(self.bagdir,'multibag',
                                                    'member-bags.tsv')))

    def test_ensure_tagdir(self):
        tagdir = os.path.join(self.bagdir, "multibag")
        self.assertTrue(os.path.isdir(tagdir))

        self.bag.ensure_tagdir()
        self.assertTrue(os.path.isdir(tagdir))

        self.clear_multibag()
        self.assertTrue(not os.path.exists(tagdir))
        self.bag.ensure_tagdir()
        self.assertTrue(os.path.isdir(tagdir))

        self.clear_multibag()
        self.assertTrue(not os.path.exists(tagdir))
        self.bag.set_multibag_tag_dir("goob")
        tagdir = os.path.join(self.bagdir, "goob")
        self.assertTrue(not os.path.exists(tagdir))
        self.bag.ensure_tagdir()
        self.assertTrue(os.path.exists(tagdir))

    def test_add_member_bag(self):
        self.assertEqual(self.bag.member_bag_names, ["samplembag"])
        self.bag.add_member_bag("samplembag2")
        self.assertEqual(self.bag.member_bag_names, ["samplembag","samplembag2"])
        self.bag.add_member_bag("foo3", "ivo://blah/goob")
        self.assertEqual(self.bag.member_bag_names,
                         ["samplembag","samplembag2", "foo3"])
        self.assertEqual(self.bag.member_bags()[-1].uri, "ivo://blah/goob")

        # test adding a member bag when there are member bags saved but not
        # cached in memory
        self.bag.save_member_bags()
        self.bag._memberbags = None
        self.assertEqual(self.bag.member_bag_names,
                         ["samplembag","samplembag2", "foo3"])
        self.bag._memberbags = None
        self.bag.add_member_bag("foo4")
        self.assertEqual(self.bag.member_bag_names,
                         ["samplembag","samplembag2", "foo3", "foo4"])

        self.clear_multibag()
        self.assertEqual(self.bag.member_bag_names, [])
        self.bag.add_member_bag("samplembag2")
        self.assertEqual(self.bag.member_bag_names, ["samplembag2"])

    def test_set_member_bags(self):
        self.assertEqual(self.bag.member_bag_names, ["samplembag"])

        out = [ mb.MemberInfo("foo3", "ivo://blah/goob") ]
        out += self.bag.member_bags()
        self.bag.set_member_bags(out)
        self.assertEqual(self.bag.member_bag_names, ["foo3","samplembag"])

    def test_save_member_bags(self):
        self.assertEqual(self.bag.member_bag_names, ["samplembag"])
        self.bag.add_member_bag("samplembag2")
        self.assertEqual(self.bag.member_bag_names, ["samplembag","samplembag2"])

        self.bag.save_member_bags()
        with open(os.path.join(self.bagdir, "multibag","member-bags.tsv")) as fd:
            names = [line.strip() for line in fd]
        self.assertEqual(names, ["samplembag","samplembag2"])
        
        self.clear_multibag()
        self.bag.add_member_bag("samplembag2")
        self.assertEqual(self.bag.member_bag_names, ["samplembag2"])
        self.bag.save_member_bags()
        with open(os.path.join(self.bagdir, "multibag","member-bags.tsv")) as fd:
            names = [line.strip() for line in fd]
        self.assertEqual(names, ["samplembag2"])

    def test_add_file_lookup(self):
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("data/trial2.json"), "samplembag")
        self.assertIsNone(self.bag.lookup_file("data/gurn/goober.json"))

        self.bag.add_file_lookup("data/gurn/goober.json", "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("data/trial2.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("data/gurn/goober.json"),
                         "samplembag2")
        
        self.bag.add_file_lookup("data/trial2.json", "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("data/trial2.json"), "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/gurn/goober.json"),
                         "samplembag2")

    def test_remove_file_lookup(self):
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.bag.remove_file_lookup("data/trial1.json")
        self.assertIsNone(self.bag.lookup_file("data/trial1.json"))
        
        self.assertEqual(self.bag.lookup_file("data/trial1.json", reread=True),
                         "samplembag")
        self.bag.remove_file_lookup("data/trial1.json")
        self.assertIsNone(self.bag.lookup_file("data/trial1.json"))
        self.bag.save_file_lookup()
        self.assertIsNone(self.bag.lookup_file("data/trial1.json", reread=True))

    def test_save_file_lookup(self):
        self.clear_multibag()
        tagdir = os.path.join(self.bagdir, "multibag")
        lufile = os.path.join(tagdir, "file-lookup.tsv")
        self.assertTrue(not os.path.exists(lufile))

        self.bag.save_file_lookup()
        self.assertTrue(not os.path.exists(lufile))

        self.bag.add_file_lookup("data/trial1.json", "samplembag")
        self.bag.add_file_lookup("data/gurn/goober.json", "samplembag2")

        self.bag.save_file_lookup()
        self.assertTrue(os.path.exists(lufile))
        
        with open(lufile) as fd:
            items = [line.strip().split('\t') for line in fd]
        self.assertEqual(items, [
            [ "data/trial1.json", "samplembag" ],
            [ "data/gurn/goober.json", "samplembag2" ]
        ])

    def test_set_deleted(self):
        tagdir = os.path.join(self.bagdir, "multibag")
        delfile = os.path.join(tagdir, "deleted.txt")
        self.assertTrue(not os.path.exists(delfile))

        dels = self.bag.deleted_paths()
        self.assertEqual(len(dels), 0)

        self.bag.set_deleted("data/trial1.json")
        dels = self.bag.deleted_paths()
        self.assertEqual(dels, ["data/trial1.json"])
        
        self.bag.set_deleted("data/trial1.json")
        self.assertEqual(dels, ["data/trial1.json"])

        self.bag.set_deleted("data/trial2.json")
        dels = self.bag.deleted_paths()
        self.assertIn("data/trial1.json", dels)
        self.assertIn("data/trial2.json", dels)
        self.assertEqual(len(dels), 2)

    def test_unset_deleted(self):
        dels = self.bag.deleted_paths()
        self.assertEqual(len(dels), 0)
        self.bag.set_deleted("data/trial1.json")
        self.bag.set_deleted("data/trial2.json")
        dels = self.bag.deleted_paths()
        self.assertIn("data/trial1.json", dels)
        self.assertIn("data/trial2.json", dels)

        self.bag.unset_deleted("data/trial1.json")
        self.assertEqual(self.bag.deleted_paths(), ["data/trial2.json"])
        self.bag.unset_deleted("goober")
        self.assertEqual(self.bag.deleted_paths(), ["data/trial2.json"])
        self.bag.unset_deleted("data/trial2.json")
        self.assertEqual(self.bag.deleted_paths(), [])
        

    def test_save_deleted(self):
        tagdir = os.path.join(self.bagdir, "multibag")
        delfile = os.path.join(tagdir, "deleted.txt")
        self.assertTrue(not os.path.exists(delfile))

        self.bag.save_deleted()
        self.assertTrue(not os.path.exists(delfile))

        self.bag.set_deleted("data/trial1.json")
        self.assertTrue(not os.path.exists(delfile))
        self.bag.save_deleted()
        self.assertTrue(os.path.exists(delfile))
        
        with open(delfile) as fd:
            items = [line.strip() for line in fd]
        self.assertEqual(items, ["data/trial1.json"])
        dels = self.bag.deleted_paths()
        self.assertEqual(dels, ["data/trial1.json"])

        self.bag.set_deleted("data/trial2.json")
        self.assertTrue(os.path.exists(delfile))
        self.bag.save_deleted()
        self.assertTrue(os.path.exists(delfile))
        
        with open(delfile) as fd:
            items = [line.strip() for line in fd]
        self.assertIn("data/trial1.json", items)
        self.assertIn("data/trial2.json", items)
        self.assertEqual(len(items), 2)

        dels = self.bag.deleted_paths()
        self.assertIn("data/trial1.json", dels)
        self.assertIn("data/trial2.json", dels)
        self.assertEqual(len(dels), 2)

    def test_format_bytes(self):
        self.assertEqual(self.bag._format_bytes(108), "108 B")
        self.assertEqual(self.bag._format_bytes(34569), "34.57 kB")
        self.assertEqual(self.bag._format_bytes(9834569), "9.835 MB")
        self.assertEqual(self.bag._format_bytes(19834569), "19.83 MB")
        self.assertEqual(self.bag._format_bytes(14419834569), "14.42 GB")

    def test_update_info(self):
        bag = Bag(self.bagdir)
        rmtag = []
        for tag in bag.info:
            if tag.startswith('Multibag-'):
                rmtag.append(tag)
        for tag in rmtag:
            del bag.info[tag]
        bag.save()
        self.clear_multibag()

        # test assumptions
        for tag in bag.info:
            self.assertFalse(tag.startswith('Multibag-'))

        self.bag.update_info()
        bag = Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "4.875 kB")

        self.bag.update_info("1.0", "1.1")
        bag = Bag(self.bagdir)

        self.assertEqual(bag.info.get('Multibag-Version'), "1.1")
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.0")
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")
        self.assertEqual(bag.info.get('Multibag-Reference'), CURRENT_REFERENCE)

        del bag.info['Internal-Sender-Description']
        bag.save()
        self.clear_multibag()
        self.bag.update_info()
        bag = Bag(self.bagdir)
        
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.0")
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")
        
        self.assertFalse(isinstance(bag.info.get('Internal-Sender-Description'),
                                    list))
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description'))

    def test_remove_member_bag(self):
        self.assertIn("samplembag", self.bag.member_bag_names)
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("data/trial2.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("metadata/nerdm.json"),
                         "samplembag2")

        self.bag.remove_member_bag("samplembag")

        self.assertNotIn("samplembag", self.bag.member_bag_names)
        self.assertIsNone(self.bag.lookup_file("data/trial1.json"))
        self.assertIsNone(self.bag.lookup_file("data/trial2.json"))
        self.assertEqual(self.bag.lookup_file("metadata/nerdm.json"),
                         "samplembag2")

    def test_update_for_member(self):
        self.clear_multibag()
        membagdir = os.path.join(datadir, 'samplembag')
        membag = open_bag(membagdir)
        
        self.bag.update_for_member(membag)
        self.assertEqual(self.bag.member_bag_names, ['samplembag'])
        self.assertEqual(self.bag.lookup_file("data/trial2.json"), 'samplembag')
        self.assertEqual(self.bag.lookup_file("data/trial3/trial3a.json"),
                         'samplembag')
        self.assertIsNone(self.bag.lookup_file("metadata/pod.json"))
        

        

if __name__ == '__main__':
    test.main()
