import os, pdb
import warnings as warn
import unittest as test

from nistoar.base import SystemInfoMixin
import nistoar.pdr as pdr

class MySystemInfo(SystemInfoMixin):
    def __init__(self):
        super(MySystemInfo, self).__init__("My System", "mine", "subsys", "sub", "dev")

class TestSystemInfo(test.TestCase):

    def test_version(self):
        si = MySystemInfo()
        self.assertEqual(si.system_version, "dev")
        self.assertGreater(len(si.system_version), 1)

    def test_sysname(self):
        si = MySystemInfo()
        self.assertEqual(si.system_name, "My System")

    def test_subsysname(self):
        si = MySystemInfo()
        self.assertEqual(si.subsystem_name, "subsys")

    def test_sysabbrev(self):
        si = MySystemInfo()
        self.assertEqual(si.system_abbrev, "mine")

    def test_subsysabbrev(self):
        si = MySystemInfo()
        self.assertEqual(si.subsystem_abbrev, "sub")

class TestPDRSystem(test.TestCase):

    def test_version(self):
        si = pdr.PDRSystem()
        self.assertNotEqual(si.system_version, "dev")
        self.assertGreater(len(si.system_version), 1)

    def test_sysname(self):
        si = pdr.PDRSystem()
        self.assertEqual(si.system_name, "Public Data Repository")

    def test_subsysname(self):
        si = pdr.PDRSystem()
        self.assertEqual(si.subsystem_name, "")

    def test_sysabbrev(self):
        si = pdr.PDRSystem()
        self.assertEqual(si.system_abbrev, "PDR")

    def test_subsysabbrev(self):
        si = pdr.PDRSystem()
        self.assertEqual(si.subsystem_abbrev, "")




if __name__ == '__main__':
    test.main()
    
