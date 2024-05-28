# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test

import multibag.constants as cnsts

class TestVersion(test.TestCase):

    def test_ctor(self):
        ver = cnsts.Version("3.3.5.0")
        self.assertEqual(ver._vs, "3.3.5.0")
        self.assertEqual(ver.fields, [3, 3, 5, 0])

        ver = cnsts.Version((3, 3, 5, 0))
        self.assertEqual(ver._vs, "3.3.5.0")
        self.assertEqual(ver.fields, (3, 3, 5, 0))

    def testEQ(self):
        ver = cnsts.Version("3.3.0")
        self.assertEqual(ver, cnsts.Version("3.3.0"))
        self.assertTrue(ver == "3.3.0")
        self.assertFalse(ver == "3.3.1")
        self.assertFalse(ver == "1.3")

    def testNE(self):
        ver = cnsts.Version("3.3.0")
        self.assertNotEqual(ver, cnsts.Version("3.3.2"))
        self.assertFalse(ver != "3.3.0")
        self.assertTrue(ver != "3.3.1")
        self.assertTrue(ver != "1.3")

    def testGE(self):
        ver = cnsts.Version("3.3.0")
        self.assertTrue(ver >= "3.2.0")
        self.assertTrue(ver >= "3.3.0")
        self.assertTrue(ver >= "1.3")

        self.assertFalse(ver >= "5.3")
        self.assertFalse(ver >= cnsts.Version("5.3"))

    def testGT(self):
        ver = cnsts.Version("3.3.0")
        self.assertTrue(ver > "3.2.0")
        self.assertTrue(ver > "1.3")

        self.assertFalse(ver > "3.3.0")
        self.assertFalse(ver >= "5.3")
        self.assertFalse(ver >= cnsts.Version("5.3"))

    def testLE(self):
        ver = cnsts.Version("3.3.0")
        self.assertTrue(ver <= "3.5.0")
        self.assertTrue(ver <= "3.3.1")
        self.assertTrue(ver <= "3.3.0")
        self.assertTrue(ver <= "5.3")

        self.assertFalse(ver <= "1.3")
        self.assertFalse(ver <= cnsts.Version("2.3"))

    def testLT(self):
        ver = cnsts.Version("3.3.0")
        self.assertTrue(ver < "3.5.0")
        self.assertTrue(ver < "3.3.1")
        self.assertTrue(ver < "5.3")

        self.assertFalse(ver < "3.3.0")
        self.assertFalse(ver < "1.3")
        self.assertFalse(ver < cnsts.Version("2.3"))





if __name__ == '__main__':
    test.main()
