import os, sys, pdb, json, logging, re
import unittest as test
from copy import deepcopy

from nistoar.testing import *
from nistoar.web import utils



class TestFunctions(test.TestCase):

    def test_is_content_type(self):
        self.assertTrue(utils.is_content_type("goob/gurn"))
        self.assertTrue(utils.is_content_type("text/plain"))
        self.assertTrue(utils.is_content_type("application/jsonld+json"))

        self.assertFalse(utils.is_content_type("html"))
        self.assertFalse(utils.is_content_type("text"))
        self.assertFalse(utils.is_content_type("datacite"))

    def test_match_accept(self):
        self.assertEqual(utils.match_accept("text/plain", "text/plain"), "text/plain")
        self.assertEqual(utils.match_accept("text/*", "text/plain"), "text/plain")
        self.assertEqual(utils.match_accept("text/plain", "text/*"), "text/plain")
        self.assertEqual(utils.match_accept("text/*", "text/*"), "text/*")
        self.assertIsNone(utils.match_accept("text/plain", "text/json"))

    def test_acceptable(self):
        self.assertEqual(utils.acceptable("text/plain", ["application/json", "text/plain", "text/*"]),
                         "text/plain")
        self.assertEqual(utils.acceptable("text/*", ["application/json","text/plain","text/*","text/json"]),
                         "text/plain")
        self.assertEqual(utils.acceptable("text/*", ["application/json","text/*","text/json","text/plain"]),
                         "text/*")
        self.assertIsNone(utils.acceptable("app/html", ["application/json","text/*","text/json"]))

    def test_order_accepts(self):
        ordrd = utils.order_accepts("text/html, application/xml;q=0.9, application/xhtml+xml, */*;q=0.8")
        self.assertEqual(ordrd, "text/html application/xhtml+xml application/xml */*".split())

        self.assertEqual(utils.order_accepts(["text/html,application/xhtml+xml",
                                              "application/xml;q=0.9",
                                                "*/*;q=0.5"]),
                         "text/html application/xhtml+xml application/xml */*".split())



if __name__ == '__main__':
    test.main()
