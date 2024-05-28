# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate.member as bagv
import multibag.validate.base as val
from multibag.access.bagit import open_bag

datadir=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                     "access", "data")

class TestMemberBagValidator(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)


    def test_validate_bagname(self):
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertTrue(results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), "sample\tbag ")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 2)
        self.assertTrue(not results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), " samplebag")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "2.1b-name-wsp")
        self.assertTrue(not results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), "sample\tbag")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "2.1a-name-TAB")
        self.assertTrue(not results.ok())

    def test_validate_as_nonhead(self):
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_as_nonhead()
        self.assertEqual(results.count_applied(), 0) # because its a head bag
        self.assertTrue(results.ok())

        del valid8r.bag.info['Multibag-Head-Version']
        results = valid8r.validate_as_nonhead()
        self.assertEqual(results.count_applied(), 3)
        self.assertEqual(results.count_failed(), 2)
        self.assertTrue(not results.ok())

        valid8r.bag.info['Multibag-Head-Deprecates'] = "0.1"
        results = valid8r.validate_as_nonhead()
        self.assertEqual(results.count_applied(), 3)
        self.assertEqual(results.count_failed(), 3)
        self.assertTrue(not results.ok())

        shutil.rmtree(os.path.join(self.bagdir, "multibag"))
        valid8r = bagv.MemberBagValidator(self.bagdir)
        del valid8r.bag.info['Multibag-Head-Version']
        del valid8r.bag.info['Multibag-Tag-Directory']
        results = valid8r.validate_as_nonhead()
        self.assertEqual(results.count_applied(), 3)
        self.assertTrue(results.ok())
        
    def test_validate(self):
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate()
        self.assertEqual(results.count_applied(), 2)
        self.assertTrue(results.ok())

    def test_is_valid(self):
        valid8r = bagv.MemberBagValidator(self.bagdir)
        self.assertTrue(valid8r.is_valid())

    def test_validate_func(self):
        bagv.validate(self.bagdir)

if __name__ == '__main__':
    test.main()
    

