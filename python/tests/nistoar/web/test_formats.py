import os, sys, pdb, json, logging, re
import unittest as test
from copy import deepcopy

from nistoar.testing import *
from nistoar.web import formats as fmts

class TestFormat(test.TestCase):

    def test_Format(self):
        fmt = fmts.Format("goob", "goob/gurn")
        self.assertEqual(fmt.name, "goob")
        self.assertEqual(fmt.ctype, "goob/gurn")
        self.assertEqual(fmt, fmts.Format("goob", "goob/gurn"))


class TestFormatSupport(test.TestCase):

    def setUp(self):
        self.sprtd = fmts.XHTMLSupport()

    def test_support(self):
        goob = fmts.Format("goob", "goob/gurn")
        self.sprtd.support(goob)
        self.assertEqual(self.sprtd.match("goob"), goob)
        self.assertIsNone(self.sprtd.match("goob/gurn"))

        with self.assertRaises(ValueError):
            self.sprtd.support(goob, ["goober/gurn"], False, True)

        self.sprtd.support(goob, ["goober/gurn", "application/gurn"])
        self.assertEqual(self.sprtd.match("goob"), goob)
        self.assertEqual(self.sprtd.match("goober/gurn"), fmts.Format("goob", "goober/gurn"))
        self.assertEqual(self.sprtd.match("application/gurn"), fmts.Format("goob", "application/gurn"))

        with self.assertRaises(ValueError):
            self.sprtd.support(fmts.Format("bill", "people/firstname"), ["goober/gurn"], False, True)

    def test_match(self):
        def html(ct="text/html"):
            return fmts.Format("html", ct)
        self.assertEqual(self.sprtd.match("html"), html())
        self.assertEqual(self.sprtd.match("application/html"), html("application/html"))
        self.assertEqual(self.sprtd.match("text/html"), html("text/html"))
        self.assertEqual(self.sprtd.match("application/xhtml"), html("application/xhtml"))
        self.assertEqual(self.sprtd.match("application/xhtml+xml"), html("application/xhtml+xml"))
        self.assertIsNone(self.sprtd.match("text"))

        text = fmts.Format("text", "text/plain")
        fmts.TextSupport.add_support(self.sprtd)
        self.assertEqual(self.sprtd.match("text"), text)
        self.assertEqual(self.sprtd.match("text/plain"), text)

        self.assertEqual(self.sprtd.match("*/*"), html())
        self.assertEqual(self.sprtd.match("text/*"), html())

    def test_default_format(self):
        self.assertEqual(self.sprtd.default_format(), fmts.Format("html", "text/html"))
        fmts.TextSupport.add_support(self.sprtd, asdefault=True)
        self.assertEqual(self.sprtd.default_format(), fmts.Format("text", "text/plain"))
        
    def test_select_format(self):
        html = fmts.Format("html", "text/html")
        text = fmts.Format("text", "text/plain")

        # content negotiation only
        fmt = self.sprtd.select_format([], "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format([], "application/html text/plain text/html application/pdf".split())
        self.assertEqual(fmt, fmts.Format("html", "application/html"))
        with self.assertRaises(fmts.Unacceptable):
            fmt = self.sprtd.select_format([], "text/plain text/postscript application/pdf".split())

        # format request only
        fmt = self.sprtd.select_format("text/plain html datacite".split(), [])
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("text/plain datacite application/html".split(), [])

        self.assertEqual(fmt, fmts.Format("html", "application/html"))
        with self.assertRaises(fmts.UnsupportedFormat):
            fmt = self.sprtd.select_format("text/plain application/json datacite".split(), [])

        # both
        fmt = self.sprtd.select_format("html text".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("application/xhtml+xml application/json html text".split(),
                                       "text/plain text/html application/xhtml+xml application/pdf".split())
        self.assertEqual(fmt, fmts.Format("html", "application/xhtml+xml"))
        with self.assertRaises(fmts.UnsupportedFormat):
            fmt = self.sprtd.select_format("text/plain application/json datacite".split(),
                                           "text/plain text/html application/pdf".split())
        with self.assertRaises(fmts.Unacceptable):
            fmt = self.sprtd.select_format("text/plain application/xhtml+xml datacite".split(),
                                           "text/plain application/pdf".split())

        self.assertIsNone(self.sprtd.select_format(None, []))

        fmts.TextSupport.add_support(self.sprtd, asdefault=True)
        
        # content negotiation only
        fmt = self.sprtd.select_format([], "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format([], "text/plain text/postscript application/pdf".split())
        self.assertEqual(fmt, text)
        with self.assertRaises(fmts.Unacceptable):
            fmt = self.sprtd.select_format([], "text/postscript application/pdf".split())

        # format request only
        fmt = self.sprtd.select_format("text/plain html datacite".split(), [])
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text datacite application/html".split(), [])
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text/plain application/json datacite".split(), [])
        self.assertEqual(fmt, text)
        with self.assertRaises(fmts.UnsupportedFormat):
            fmt = self.sprtd.select_format("application/json datacite".split(), [])

        # both
        fmt = self.sprtd.select_format("html text".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("text html".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("application/xhtml+xml application/json html text".split(),
                                       "text/plain text/html application/xhtml+xml application/pdf".split())
        self.assertEqual(fmt, fmts.Format("html", "application/xhtml+xml"))
        fmt = self.sprtd.select_format("html application/xhtml+xml application/json text".split(),
                                       "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("text html application/xhtml+xml application/json text".split(),
                                       "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text/plain application/json datacite".split(),
                                       "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text/plain application/xhtml+xml datacite".split(),
                                       "text/plain application/pdf".split())
        self.assertEqual(fmt, text)
        with self.assertRaises(fmts.UnsupportedFormat):
            fmt = self.sprtd.select_format("pdf datacite".split(),
                                           "text/plain application/pdf".split())
        with self.assertRaises(fmts.Unacceptable):
            fmt = self.sprtd.select_format("application/xhtml+xml datacite".split(),
                                           "text/plain application/pdf".split())




if __name__ == '__main__':
    test.main()

