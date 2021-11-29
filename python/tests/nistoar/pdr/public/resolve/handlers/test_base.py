import os, sys, pdb, json, logging, re
import unittest as test
from copy import deepcopy

from nistoar.testing import *
from nistoar.pdr.public.resolve.handlers import base as handler

class TestFunctions(test.TestCase):

    def test_is_content_type(self):
        self.assertTrue(handler.is_content_type("goob/gurn"))
        self.assertTrue(handler.is_content_type("text/plain"))
        self.assertTrue(handler.is_content_type("application/jsonld+json"))

        self.assertFalse(handler.is_content_type("html"))
        self.assertFalse(handler.is_content_type("text"))
        self.assertFalse(handler.is_content_type("datacite"))

    def test_order_accepts(self):
        ordrd = handler.order_accepts("text/html, application/xml;q=0.9, application/xhtml+xml, */*;q=0.8")
        self.assertEqual(ordrd, "text/html application/xhtml+xml application/xml */*".split())

        self.assertEqual(handler.order_accepts(["text/html,application/xhtml+xml",
                                                "application/xml;q=0.9",
                                                "*/*;q=0.5"]),
                         "text/html application/xhtml+xml application/xml */*".split())

    def test_Format(self):
        fmt = handler.Format("goob", "goob/gurn")
        self.assertEqual(fmt.name, "goob")
        self.assertEqual(fmt.ctype, "goob/gurn")
        self.assertEqual(fmt, handler.Format("goob", "goob/gurn"))

class TestFormatSupport(test.TestCase):

    def setUp(self):
        self.sprtd = handler.XHTMLSupport()

    def test_support(self):
        goob = handler.Format("goob", "goob/gurn")
        self.sprtd.support(goob)
        self.assertEqual(self.sprtd.match("goob"), goob)
        self.assertIsNone(self.sprtd.match("goob/gurn"))

        with self.assertRaises(ValueError):
            self.sprtd.support(goob, ["goober/gurn"], False, True)

        self.sprtd.support(goob, ["goober/gurn", "application/gurn"])
        self.assertEqual(self.sprtd.match("goob"), goob)
        self.assertEqual(self.sprtd.match("goober/gurn"), goob)
        self.assertEqual(self.sprtd.match("application/gurn"), goob)

        with self.assertRaises(ValueError):
            self.sprtd.support(handler.Format("bill", "people/firstname"), ["goober/gurn"], False, True)

    def test_match(self):
        html = handler.Format("html", "text/html")
        self.assertEqual(self.sprtd.match("html"), html)
        self.assertEqual(self.sprtd.match("application/html"), html)
        self.assertEqual(self.sprtd.match("text/html"), html)
        self.assertEqual(self.sprtd.match("application/xhtml"), html)
        self.assertEqual(self.sprtd.match("application/xhtml+xml"), html)
        self.assertIsNone(self.sprtd.match("text"))

        text = handler.Format("text", "text/plain")
        handler.TextSupport.add_support(self.sprtd)
        self.assertEqual(self.sprtd.match("text"), text)
        self.assertEqual(self.sprtd.match("text/plain"), text)

        self.assertEqual(self.sprtd.match("*/*"), html)
        self.assertEqual(self.sprtd.match("text/*"), html)

    def test_default_format(self):
        self.assertEqual(self.sprtd.default_format(), handler.Format("html", "text/html"))
        handler.TextSupport.add_support(self.sprtd, asdefault=True)
        self.assertEqual(self.sprtd.default_format(), handler.Format("text", "text/plain"))
        
    def test_select_format(self):
        html = handler.Format("html", "text/html")
        text = handler.Format("text", "text/plain")

        # content negotiation only
        fmt = self.sprtd.select_format([], "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format([], "application/html text/plain text/html application/pdf".split())
        self.assertEqual(fmt, handler.Format("html", "application/html"))
        with self.assertRaises(handler.Unacceptable):
            fmt = self.sprtd.select_format([], "text/plain text/postscript application/pdf".split())

        # format request only
        fmt = self.sprtd.select_format("text/plain html datacite".split(), [])
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("text/plain datacite application/html".split(), [])
        self.assertEqual(fmt, handler.Format("html", "application/html"))
        with self.assertRaises(handler.UnsupportedFormat):
            fmt = self.sprtd.select_format("text/plain application/json datacite".split(), [])

        # both
        fmt = self.sprtd.select_format("html text".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("application/xhtml+xml application/json html text".split(),
                                       "text/plain text/html application/xhtml+xml application/pdf".split())
        self.assertEqual(fmt, handler.Format("html", "application/xhtml+xml"))
        with self.assertRaises(handler.UnsupportedFormat):
            fmt = self.sprtd.select_format("text/plain application/json datacite".split(),
                                           "text/plain text/html application/pdf".split())
        with self.assertRaises(handler.Unacceptable):
            fmt = self.sprtd.select_format("text/plain application/xhtml+xml datacite".split(),
                                           "text/plain application/pdf".split())

        self.assertIsNone(self.sprtd.select_format(None, []))

        handler.TextSupport.add_support(self.sprtd, asdefault=True)
        
        # content negotiation only
        fmt = self.sprtd.select_format([], "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format([], "text/plain text/postscript application/pdf".split())
        self.assertEqual(fmt, text)
        with self.assertRaises(handler.Unacceptable):
            fmt = self.sprtd.select_format([], "text/postscript application/pdf".split())

        # format request only
        fmt = self.sprtd.select_format("text/plain html datacite".split(), [])
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text datacite application/html".split(), [])
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("text/plain application/json datacite".split(), [])
        self.assertEqual(fmt, text)
        with self.assertRaises(handler.UnsupportedFormat):
            fmt = self.sprtd.select_format("application/json datacite".split(), [])

        # both
        fmt = self.sprtd.select_format("html text".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, html)
        fmt = self.sprtd.select_format("text html".split(), "text/plain text/html application/pdf".split())
        self.assertEqual(fmt, text)
        fmt = self.sprtd.select_format("application/xhtml+xml application/json html text".split(),
                                       "text/plain text/html application/xhtml+xml application/pdf".split())
        self.assertEqual(fmt, handler.Format("html", "application/xhtml+xml"))
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
        with self.assertRaises(handler.UnsupportedFormat):
            fmt = self.sprtd.select_format("pdf datacite".split(),
                                           "text/plain application/pdf".split())
        with self.assertRaises(handler.Unacceptable):
            fmt = self.sprtd.select_format("application/xhtml+xml datacite".split(),
                                           "text/plain application/pdf".split())

class TestReady(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def setUp(self):
        self.hdlr = None
        self.resp = []

    def gethandler(self, path, env):
        return handler.Ready(path, env, self.start)

    def test_set_response(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr._code, 0)
        self.assertEqual(self.hdlr._msg, "unknown status")
        self.hdlr.set_response(388, "Mojo")
        self.assertEqual(self.hdlr._code, 388)
        self.assertEqual(self.hdlr._msg, "Mojo")
        self.assertEqual(self.resp, [])

        self.hdlr.end_headers()
        self.assertEqual(self.resp[0], "388 Mojo")

    def test_add_header(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(len(self.hdlr._hdr), 0)
        
        self.hdlr.add_header("Authorization", "Bearer Key")
        self.hdlr.add_header("Goober", "Gurn")
        self.hdlr.add_header("Goober", "Gomer")
        self.assertEqual(len(self.hdlr._hdr), 3)
        self.assertEqual(self.hdlr._hdr.get_all("Authorization"), ["Bearer Key"])
        self.assertEqual(self.hdlr._hdr.get_all("Goober"), ["Gurn", "Gomer"])

        self.hdlr.end_headers()
        self.assertEqual(self.resp[0], "0 unknown status")
        self.assertEqual(self.resp[1], "Authorization: Bearer Key")
        self.assertEqual(self.resp[2], "Goober: Gurn")
        self.assertEqual(self.resp[3], "Goober: Gomer")

    def test_ordered_formats(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.ordered_formats(), [])

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html"
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.ordered_formats(), ["text", "html"])

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=datacite"
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.ordered_formats(), ["datacite"])

    def test_ordered_accepts(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html",
            'HTTP_ACCEPT': 'text/html, */*;q=0.8, application/xhtml+xml, application/xml;q=0.9'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.ordered_accepts(),
                         "text/html application/xhtml+xml application/xml */*".split())
        
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "a=b&format=text&goob=gurn&format=html",
            'HTTP_ACCEPT': 'text/html'
        }
        self.hdlr = self.gethandler('', req)
        self.assertEqual(self.hdlr.ordered_accepts(), "text/html".split())
        
    def test_send_ok(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.send_ok()
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "200 OK")
        self.resp = []

        body = self.hdlr.send_ok("PERFECT", "It's all good.")
        self.assertEqual(body, ["It's all good."])
        self.assertEqual(self.resp[0], "200 PERFECT")
        self.resp = []

        body = self.hdlr.send_ok("Got it", '"It is all good."', 201, "text/json")
        self.assertEqual(body, ['"It is all good."'])
        self.assertEqual(self.resp[0], "201 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_send_ok_head(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.send_ok("Got it", '"It is all good."', 201, "text/json", ashead=True)
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "201 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_send_error(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.send_error(200, "OK")
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "200 OK")
        self.resp = []

        body = self.hdlr.send_error(400, "Icky input")
        self.assertEqual(body, [])
        self.assertEqual(self.resp[0], "400 Icky input")
        self.resp = []

        body = self.hdlr.send_error(499, "Got it", '"It is all good."', "text/json")
        self.assertEqual(body, ['"It is all good."'])
        self.assertEqual(self.resp[0], "499 Got it")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/json")
        self.assertEqual([h for h in self.resp if 'Content-Length:' in h][0],
                         "Content-Length: 17")
        self.resp = []

    def test_handle(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 552)
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=text",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 25)
        self.assertEqual("\n".join(body), "Resolver service is ready")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "406 Not Acceptable")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        self.assertEqual("\n".join(body), "format parameter is inconsistent with Accept header")
        self.resp = []

        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'HTTP_ACCEPT': "application/xhtml"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: application/xhtml")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 552)
        self.resp = []


    def test_handle_head(self):
        req = {
            'REQUEST_METHOD': "HEAD",
            'PATH_INFO': '/'
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/html")
        cthdr = [h for h in self.resp if 'Content-Length:' in h][0]
        self.assertEqual(int(re.sub(r'^.*: ', '', cthdr)), 552)
        self.assertEqual("\n".join(body), "")
        self.resp = []

        req = {
            'REQUEST_METHOD': "HEAD",
            'PATH_INFO': '/',
            'QUERY_STRING': "format=html",
            'HTTP_ACCEPT': "text/plain"
        }
        self.hdlr = self.gethandler('', req)

        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "406 Not Acceptable")
        self.assertEqual([h for h in self.resp if 'Content-Type:' in h][0],
                         "Content-Type: text/plain")
        self.assertEqual("\n".join(body), "")
        self.resp = []


        
        


if __name__ == '__main__':
    test.main()
