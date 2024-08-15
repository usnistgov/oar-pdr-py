import os, sys, pdb, json
import unittest as test

import nistoar.pdr.utils.validate as base

class TestValidationTest(test.TestCase):

    def test_ctor(self):
        test = base.ValidationTest("Life", "3.1", "A1.1")

        self.assertEqual(test.profile, "Life")
        self.assertEqual(test.profile_version, "3.1")
        self.assertEqual(test.label, "A1.1")
        self.assertEqual(test.type, test.ERROR)
        self.assertEqual(test.specification, "")

        test = base.ValidationTest("Life", "3.1", "A1.1", base.REC,
                                    spec="Life must self replicate.")

        self.assertEqual(test.profile, "Life")
        self.assertEqual(test.profile_version, "3.1")
        self.assertEqual(test.label, "A1.1")
        self.assertEqual(test.specification, "Life must self replicate.")


class TestValidationIssue(test.TestCase):

    def test_ctor(self):
        issue = base.ValidationIssue("Life", "3.1", "A1.1")

        self.assertEqual(issue.profile, "Life")
        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.ERROR)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "")
        self.assertEqual(len(issue.comments), 0)

        issue = base.ValidationIssue("Life", "3.1", "A1.1", base.REC,
                                     spec="Life must self replicate.",
                                     passed=False)

        self.assertEqual(issue.profile, "Life")
        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 0)

        issue = base.ValidationIssue("Life", "3.1", "A1.1", base.REC,
                                     spec="Life must self replicate.",
                                     passed=False)

        self.assertEqual(issue.profile, "Life")
        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(len(issue.comments), 0)

        issue = base.ValidationIssue("Life", "3.1", "A1.1", base.REC,
                                     spec="Life must self replicate.",
                                     comments=["little", "green"])

        self.assertEqual(issue.profile, "Life")
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
        
        issue = base.ValidationIssue("Life", "3.1", "A1.1")
        self.assertEqual(issue.summary, "PASSED: Life 3.1 A1.1")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = base.ValidationIssue("Life", "3.1", "A1.1",
                                     spec="Life must self-replicate")
        self.assertEqual(issue.summary,
                         "PASSED: Life 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = base.ValidationIssue("Life", "3.1", "A1.1",
                                     spec="Life must self-replicate", 
                                     passed=False, comments=["Little", "green"])
        self.assertEqual(issue.summary,
                         "ERROR: Life 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue),
                     "ERROR: Life 3.1 A1.1: Life must self-replicate (Little)")
        self.assertEqual(issue.description,
           "ERROR: Life 3.1 A1.1: Life must self-replicate\n  Little\n  green")

    def test_from_test(self):
        test = base.ValidationTest("Life", "3.1", "A1.1", base.REC,
                                    spec="Life must self replicate.")

        issue = base.ValidationIssue.from_test(test, True, comments="good job!")
        issue.add_comment("participation award")
        
        self.assertEqual(issue.profile, "Life")
        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 2)
        self.assertEqual(issue.comments[0], "good job!")
        self.assertEqual(issue.comments[1], "participation award")


class TestValidationResults(test.TestCase):

    def test_ctor(self):
        res = base.ValidationResults("mythumb")
        self.assertEqual(res.target, "mythumb")
        self.assertEqual(res.want, res.ALL)
        self.assertEqual(res.applied(), [])
        self.assertEqual(res.count_applied(), 0)
        self.assertEqual(res.applied(res.ERROR), [])
        self.assertEqual(res.count_applied(res.ERROR), 0)
        self.assertEqual(res.failed(), [])
        self.assertEqual(res.count_failed(), 0)
        self.assertEqual(res.failed(res.WARN), [])
        self.assertEqual(res.count_failed(res.WARN), 0)
        self.assertEqual(res.passed(), [])
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.passed(res.REC), [])
        self.assertEqual(res.count_passed(res.PROB), 0)
        self.assertTrue(res.ok())

    def test_add_applied(self):
        res = base.ValidationResults("mythumb")
        self.assertEqual(res.target, "mythumb")
        self.assertEqual(res.want, res.ALL)

        req1 = base.ValidationTest("Life", "3.1", "A1.1", base.REQ,
                                    spec="Life must convert energy to entropy.")
        rec1 = base.ValidationTest("Life", "3.1", "A1.2", base.REC,
                                    spec="Life should self replicate.")

        res._add_applied(req1, True)
        res._add_applied(rec1, False, comments="infertile")

        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(len(res.applied()), 2)
        tests = res.applied()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[1].label, "A1.2")
        self.assertTrue(not tests[1].passed())

        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(len(res.passed()), 1)
        tests = res.passed()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[0].comments, ())

        self.assertEqual(res.count_failed(), 1)
        self.assertEqual(len(res.failed()), 1)
        tests = res.failed()
        self.assertEqual(tests[0].label, "A1.2")
        self.assertTrue(tests[0].failed())
        self.assertEqual(tests[0].comments, ("infertile",))

        self.assertFalse(res.ok())

    def test_want(self):
        res = base.ValidationResults("mythumb", want=base.REQ)
        self.assertEqual(res.target, "mythumb")
        self.assertEqual(res.want, res.REQ)

        req1 = base.ValidationTest("Life", "3.1", "A1.1", base.REQ,
                                    spec="Life must convert energy to entropy.")
        rec1 = base.ValidationTest("Life", "3.1", "A1.2", base.REC,
                                    spec="Life should self replicate.")

        res._add_applied(req1, True)
        res._add_applied(rec1, False, comments="infertile")

        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(len(res.applied()), 2)
        tests = res.applied()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[1].label, "A1.2")
        self.assertTrue(not tests[1].passed())

        self.assertTrue(res.ok())


class LifeValidator(base.ValidatorBase):

    profile = ("Life", "3.1")

    def test_converts_energy(self, target, want=base.ALL, results=None, *kw):
        out = results
        if not out:
            out = base.ValidationResults(target.get("name", "unkn"))

        t = self._err("A1.1", "Life must convert energy to entropy.")
        out._add_applied(t, "convert" in target)

        return out

    def test_replicates(self, target, want=base.ALL, results=None, *kw):
        out = results
        if not out:
            out = base.ValidationResults(target.get("name", "unkn"))

        t = self._err("A1.2", "Life should self-replicate.")
        t = out._add_applied(t, "replicate" in target)
        if t.failed():
            t.add_comment("infertile")

        return out

    def _target_name(self, target):
        return "Maude"

class TestValidator(test.TestCase):

    def test_validate(self):
        target = set("convert replicate".split())
        val = LifeValidator({})

        res = val.validate(target, targetname="harold")
        self.assertEqual(res.target, "harold")
        
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(len(res.applied()), 2)
        tests = res.applied()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[1].label, "A1.2")
        self.assertTrue(tests[1].passed())

        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(len(res.passed()), 2)
        tests = res.passed()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[0].comments, ())
        self.assertEqual(tests[1].label, "A1.2")
        self.assertTrue(tests[1].passed())
        self.assertEqual(tests[1].comments, ())

        self.assertEqual(res.count_failed(), 0)
        self.assertEqual(len(res.failed()), 0)

        self.assertTrue(res.ok())


    def test_validate_fail(self):
        target = set("convert".split())
        val = LifeValidator({})

        res = val.validate(target)
        self.assertEqual(res.target, "Maude")
        
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(len(res.applied()), 2)
        tests = res.applied()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[1].label, "A1.2")
        self.assertTrue(not tests[1].passed())

        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(len(res.passed()), 1)
        tests = res.passed()
        self.assertEqual(tests[0].label, "A1.1")
        self.assertTrue(tests[0].passed())
        self.assertEqual(tests[0].comments, ())

        self.assertEqual(res.count_failed(), 1)
        self.assertEqual(len(res.failed()), 1)
        tests = res.failed()
        self.assertEqual(tests[0].label, "A1.2")
        self.assertTrue(tests[0].failed())
        self.assertEqual(tests[0].comments, ("infertile",))

        self.assertFalse(res.ok())

    def test_validate_exec_fail(self):
        target = set("convert".split())
        val = LifeValidator({})

        res = val.validate(40)
        self.assertEqual(res.target, "Maude")
        
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(len(res.applied()), 2)
        tests = res.applied()
        self.assertEqual(tests[0].label, "test_converts_energy execution failure")
        self.assertTrue(tests[0].failed())
        self.assertEqual(len(tests[0].comments), 1)
        self.assertEqual(tests[1].label, "test_replicates execution failure")
        self.assertTrue(tests[1].failed())
        self.assertEqual(len(tests[1].comments), 1)
        
        


if __name__ == '__main__':
    test.main()
    

