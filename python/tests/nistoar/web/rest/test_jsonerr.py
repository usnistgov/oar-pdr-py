import os, sys, pdb, json, logging, re, tempfile
import unittest as test
from copy import deepcopy
from urllib.parse import parse_qs
from collections import OrderedDict

from nistoar.testing import *
from nistoar.web.rest import Handler, jsonerr

tmpdir = tempfile.TemporaryDirectory(prefix="_test_jsonerror.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_nsd.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class PoorHandler(Handler, jsonerr.ErrorHandler):

    def do_GET(self, path, ashead=False, format=None):
        params = {}
        qstr = self._env.get('QUERY_STRING')
        if qstr:
            params = parse_qs(qstr)
            for key in params:
                params[key] = params[key][-1]

        code = int(params.get('code', 550))
        if 'code' in params:
            del params['code']
        reason = params.get('reason', "Not specified")
        if 'reason' in params:
            del params['reason']
        message = params.get('message')
        if 'message' in params:
            del params['message']
        ct = params.get('ct', "app/json")
        if 'ct' in params:
            del params['ct']

        return self.send_error_obj(code, reason, message, params, ashead, ct)

class TestErrorHandler(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2data(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.hdlr = None
        self.resp = []

    def gethandler(self, path, env):
        return PoorHandler(path, env, self.start)

    def test_send_error_obj(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': '/',
            'QUERY_STRING': "code=400&reason=Not+valid&message=request+is+indecent&id=me&auth=me"
        }
        self.hdlr = self.gethandler('', req)
        body = self.hdlr.handle()
        self.assertIn("400 ", self.resp[0])
        ct = [h for h in self.resp if "Content-Type:" in h]
        self.assertEqual(len(ct), 1)
        self.assertTrue("app/json" in ct[0])
        resp = self.body2data(body)
        self.assertEqual(resp.get("http:status"), 400)
        self.assertEqual(resp.get("http:reason"), "Not valid")
        self.assertEqual(resp.get("oar:message"), "request is indecent")
        self.assertEqual(resp.get("id"), "me")
        self.assertEqual(resp.get("auth"), "me")

        


        
                         
if __name__ == '__main__':
    test.main()
