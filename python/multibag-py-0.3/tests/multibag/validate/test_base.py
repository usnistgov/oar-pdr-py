# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate.base as val
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag
from multibag.constants import CURRENT_VERSION

datadir=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                     "access", "data")

class TestValidationIssue(test.TestCase):

    def test_ctor(self):
        issue = val.ValidationIssue("A1.1")

        self.assertEqual(issue.profile_version, CURRENT_VERSION)
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.ERROR)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "")
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1",
                                    spec="Life must self replicate.",
                                    passed=False)

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1",
                                    spec="Life must self replicate.",
                                    passed=False)

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1", 
                                    spec="Life must self replicate.",
                                    comments=["little", "green"])

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 2)
        self.assertEqual(issue.comments[0], "little")
        self.assertEqual(issue.comments[1], "green")

    def test_description(self):
        
        issue = val.ValidationIssue("A1.1")
        self.assertEqual(issue.summary, "PASSED: multibag 0.4 A1.1")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = val.ValidationIssue("A1.1", profver="3.1",
                                    spec="Life must self-replicate")
        self.assertEqual(issue.summary,
                         "PASSED: multibag 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = val.ValidationIssue("A1.1", profver="3.1",
                                    spec="Life must self-replicate", 
                                    passed=False, comments=["Little", "green"])
        self.assertEqual(issue.summary,
                         "ERROR: multibag 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue),
                    "ERROR: multibag 3.1 A1.1: Life must self-replicate (Little)")
        self.assertEqual(issue.description,
        "ERROR: multibag 3.1 A1.1: Life must self-replicate\n   Little\n   green")

class TestValidationResults(test.TestCase):

    def setUp(self):
        self.res = val.ValidationResults("Life", val.PROB)

    def test_ctor(self):
        self.assertEqual(self.res.target, "Life")
        self.assertEqual(self.res.want, 3)
        self.assertTrue(self.res.want & val.ERROR)
        self.assertTrue(self.res.want & val.WARN)
        self.assertFalse(self.res.want & val.REC)
        self.assertEqual(self.res.results[val.ERROR], [])
        self.assertEqual(self.res.results[val.WARN], [])
        self.assertEqual(self.res.results[val.REC], [])
        
    def test_applied(self):
        self.res.results[val.ERROR] = "a b c".split()
        self.res.results[val.WARN] = "d e".split()
        self.res.results[val.REC] = "f".split()

        self.assertEqual(self.res.applied(), "a b c d e f".split())
        self.assertEqual(self.res.applied(val.ALL), "a b c d e f".split())
        self.assertEqual(self.res.applied(val.ERROR), "a b c".split())
        self.assertEqual(self.res.applied(val.WARN), "d e".split())
        self.assertEqual(self.res.applied(val.REC), "f".split())
        self.assertEqual(self.res.applied(val.PROB), "a b c d e".split())

        self.assertEqual(self.res.count_applied(), 6)
        self.assertEqual(self.res.count_applied(val.ALL), 6)
        self.assertEqual(self.res.count_applied(val.ERROR), 3)
        self.assertEqual(self.res.count_applied(val.WARN), 2)
        self.assertEqual(self.res.count_applied(val.REC), 1)
        self.assertEqual(self.res.count_applied(val.PROB), 5)

    def test_passed_failed(self):
        self.res.results[val.ERROR] = [
            val.ValidationIssue("1", val.ERROR, passed=False),
            val.ValidationIssue("2", val.ERROR, passed=True),
            val.ValidationIssue("3", val.ERROR, passed=False)
        ]
        self.res.results[val.REC] = [
            val.ValidationIssue("4", val.REC, passed=True),
            val.ValidationIssue("5", val.REC, passed=False),
            val.ValidationIssue("6", val.REC, passed=True)
        ]

        self.assertEqual([i.label for i in self.res.failed()],
                         "1 3 5".split())
        self.assertEqual(self.res.count_failed(), 3)
        self.assertEqual([i.label for i in self.res.passed()],
                         "2 4 6".split())
        self.assertEqual(self.res.count_passed(), 3)

        self.assertEqual([i.label for i in self.res.failed(val.ERROR)],
                         "1 3".split())
        self.assertEqual(self.res.count_failed(val.ERROR), 2)
        self.assertEqual([i.label for i in self.res.passed(val.ERROR)],
                         "2".split())
        self.assertEqual(self.res.count_passed(val.ERROR), 1)

        self.assertEqual([i.label for i in self.res.failed(val.REC)],
                         "5".split())
        self.assertEqual(self.res.count_failed(val.REC), 1)
        self.assertEqual([i.label for i in self.res.passed(val.REC)],
                         "4 6".split())
        self.assertEqual(self.res.count_passed(val.REC), 2)

        self.assertEqual([i.label for i in self.res.failed(val.WARN)],[])
        self.assertEqual(self.res.count_failed(val.WARN), 0)
        self.assertEqual([i.label for i in self.res.passed(val.WARN)],[])
        self.assertEqual(self.res.count_passed(val.WARN), 0)

        self.assertEqual([i.label for i in self.res.failed(val.PROB)],
                         "1 3".split())
        self.assertEqual(self.res.count_failed(val.PROB), 2)
        self.assertEqual([i.label for i in self.res.passed(val.PROB)],
                         "2".split())
        self.assertEqual(self.res.count_passed(val.PROB), 1)


    def test_ok(self):
        self.res.results[val.ERROR] = [
            val.ValidationIssue("1", val.ERROR, passed=True),
            val.ValidationIssue("2", val.ERROR, passed=True),
            val.ValidationIssue("3", val.ERROR, passed=True)
        ]
        self.res.results[val.REC] = [
            val.ValidationIssue("4", val.REC, passed=True),
            val.ValidationIssue("5", val.REC, passed=False),
            val.ValidationIssue("6", val.REC, passed=True)
        ]

        self.assertTrue(self.res.ok())
        self.res.want = val.ALL
        self.assertFalse(self.res.ok())

    def test_add_issue(self):
        self.assertEqual(self.res.count_applied(), 0)

        self.res._err(val.ValidationIssue("I must stay awake"), True, "Good job!")
        self.assertEqual(self.res.count_applied(), 1)
        self.assertEqual(self.res.count_passed(), 1)
        self.assertEqual(self.res.count_applied(val.ERROR), 1)

        self.res._warn(val.ValidationIssue("I must pay attention"), False,"Up here!")
        self.assertEqual(self.res.count_applied(), 2)
        self.assertEqual(self.res.count_passed(), 1)
        self.assertEqual(self.res.count_failed(), 1)
        self.assertEqual(self.res.count_applied(val.ERROR), 1)
        self.assertEqual(self.res.count_applied(val.WARN), 1)

        self.res._rec(val.ValidationIssue("I must be polite"), True,"Aw!")
        self.assertEqual(self.res.count_applied(), 3)
        self.assertEqual(self.res.count_passed(), 2)
        self.assertEqual(self.res.count_failed(), 1)
        self.assertEqual(self.res.count_applied(val.ERROR), 1)
        self.assertEqual(self.res.count_applied(val.WARN), 1)
        self.assertEqual(self.res.count_applied(val.REC), 1)


class TestValidator(test.TestCase):

    def setUp(self):
        self.valid8r = val.Validator("Life")

    def test_ctor(self):
        self.assertEqual(self.valid8r.target, "Life")

    def test_validate(self):
        results = self.valid8r.validate()
        self.assertEqual(results.count_applied(), 0)
        self.assertTrue(results.ok())

        results = val.ValidationResults("Life", val.PROB)
        results.results[val.ERROR] = [
            val.ValidationIssue("1", val.ERROR, passed=False)
        ]
        results = self.valid8r.validate(results=results)
        self.assertEqual(results.count_applied(), 1)
        self.assertTrue(not results.ok())

    def test_is_valid(self):
        self.assertTrue(self.valid8r.is_valid())

    def test_ensure_valid(self):
        self.valid8r.ensure_valid()



        

if __name__ == '__main__':
    test.main()
    

