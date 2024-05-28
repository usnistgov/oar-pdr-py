from unittest import TestLoader, TestSuite

def additional_tests():
    from . import (test_bagit_imported, test_bagit_fs, 
                   test_extended, test_multibag)

    suites = [TestLoader().loadTestsFromModule(m[1])
                     for m in globals().items() if m[0].startswith("test_")]
    return TestSuite(suites)
