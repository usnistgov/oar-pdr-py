# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate.bag as bagv
import multibag.validate.base as val
from multibag.access.bagit import open_bag

datadir=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                     "access", "data")

class TestBagValidator(test.TestCase):

    def test_validate(self):
        bagdir = os.path.join(datadir, "samplembag")
        valid8r = bagv.BagValidator(bagdir)

        results = valid8r.validate(val.ALL)
        self.assertEqual(results.count_applied(), 1)
        self.assertEqual(results.count_failed(), 0)
        self.assertTrue(results.ok())

        self.assertTrue(valid8r.is_valid())
        valid8r.ensure_valid()

    def test_validate_zip(self):
        bagdir = os.path.join(datadir, "samplembag.zip")
        valid8r = bagv.BagValidator(bagdir)

        results = valid8r.validate(val.ALL)
        self.assertEqual(results.count_applied(), 1)
        self.assertEqual(results.count_failed(), 0)
        self.assertTrue(results.ok())

        self.assertTrue(valid8r.is_valid())
        valid8r.ensure_valid()

    def test_invalidate(self):
        tempdir = tempfile.mkdtemp()
        try:
            bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), bagdir)
            os.rename(os.path.join(bagdir, "data"), os.path.join(bagdir, "goob"))
            valid8r = bagv.BagValidator(bagdir)

            results = valid8r.validate(val.ALL)
            self.assertEqual(results.count_applied(), 1)
            self.assertEqual(results.count_failed(), 1)
            self.assertTrue(not results.ok())

            self.assertTrue(not valid8r.is_valid())
            with self.assertRaises(val.MultibagValidationError):
                valid8r.ensure_valid()

        finally:
            shutil.rmtree(tempdir)



        

if __name__ == '__main__':
    test.main()
    

