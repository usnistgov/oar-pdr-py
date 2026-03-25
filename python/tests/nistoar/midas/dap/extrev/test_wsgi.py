"""
test review subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path
from copy import deepcopy
from collections import OrderedDict
from io import StringIO

from nistoar.midas.dap.extrev import wsgi
from nistoar.midas.dap.service import mds3
from nistoar.base import config as cfgmod
from nistoar.midas.dap.nerdstore.inmem import InMemoryResourceStorage
from nistoar.midas.dbio.inmem import InMemoryDBClient
from nistoar.midas.dbio.mongo import MongoDBClient
from nistoar.midas.dbio.fsbased import FSBasedDBClient
from nistoar.midas.dbio import status
from nistoar.pdr.utils.prov import Agent

tmpdir = tempfile.TemporaryDirectory(prefix="_test_review.")
testdir = Path(__file__).parents[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_review.log"))
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

datadir = os.path.join(tmpdir.name, "dbfiles")

class TestLegacyNPSFeedbackHandler(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2dict(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.cfg = {
            'dbio': {
                "factory": "fsbased",
                "db_root_dir": datadir,
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "mds3"
                    }
                }
            },
            'dap_service': {
                "doi_naan": "10.88888",
                "nerdstorage": {
#                    "type": "fsbased",
#                    "store_dir": os.path.join(tmpdir.name, "nrdstore")
                    "type": "inmem",
                },
                "default_responsible_org": {
                    "@type": "org:Organization",
                    "@id": mds3.NIST_ROR,
                    "title": "NIST"
                },
                "reviewer_ids": [ "npsop" ]
            },
            "authentication": {
                "auth_key": "secret",
                "user": "npsop",
                "client": "nps",
                "raise_on_anonymous": True
            }
        }
        self.log = logging.getLogger()

        if not os.path.exists(datadir):
            os.mkdir(datadir)
        
        self.app = wsgi.ExternalReviewApp(self.cfg)
        self.resp = []
        self.rootpath = "/"

    def tearDown(self):
        if os.path.isdir(datadir):
            shutil.rmtree(datadir)

    def create_service(self):
        user = Agent("unittest", Agent.AUTO, "test", Agent.PUBLIC)
        return self.app.svcfact.create_service_for(user)

    def create_record(self):
        svc = self.create_service()
        prec = svc.create_record("testrec")
        svc._set_review_permissions(prec)
        prec.save()
        return prec.id

    def test_handle(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'HTTP_AUTHORIZATION': "Bearer: secret"
        }
        who = Agent("unittest", Agent.AUTO, "npsop", Agent.PUBLIC)
        body = self.app.handle_path_request("", req, self.start, who)

        # nothings there yet
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp, [])

        # try again after record creation
        id = self.create_record()
        body = self.app.handle_path_request("", req, self.start, who)

        # nothing is open
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp, [])

        # start a review
        # self.create_service().apply_external_review(id, "nps", "requested", id)
        self.resp = []
        path = id
        post = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
            'HTTP_AUTHORIZATION': "Bearer: secret",
            'wsgi.input': StringIO(json.dumps({"reviewReason": "initial"}))
        }
        body = self.app.handle_path_request(id, post, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['@id'], id)
        self.assertEqual(resp['system'], "nps1")
        self.assertEqual(resp['phase'], "in progress")
        self.assertFalse(resp.get('feedback'))
        
        # recheck get
        self.resp = []
        body = self.app.handle_path_request("", req, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(len(resp), 1)
        self.assertEqual(resp[0]['@id'], id)
        self.assertEqual(resp[0]['system'], "nps1")
        self.assertEqual(resp[0]['phase'], "in progress")

        # get the specific review
        self.resp = []
        path = id
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
            'HTTP_AUTHORIZATION': "Bearer: secret"
        }
        body = self.app.handle_path_request(id, req, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['@id'], id)
        self.assertEqual(resp['system'], "nps1")

        # request feedback
        self.resp = []
        post['wsgi.input'] = StringIO(json.dumps({"reviewResponse": False}))
        body = self.app.handle_path_request(id, post, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['@id'], id)
        self.assertEqual(resp['system'], "nps1")
        self.assertEqual(resp['phase'], "paused")
        self.assertEqual(len(resp['feedback']), 1)
        fb = resp['feedback'][0]
        self.assertEqual(fb['type'], "req")
        self.assertTrue(fb['description'].startswith("Visit NPS"))

        svc = self.create_service()
        svc.submit(id)

        # now approve
        self.resp = []
        post['wsgi.input'] = StringIO(json.dumps({"reviewResponse": True}))
        body = self.app.handle_path_request(id, post, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['@id'], id)
        self.assertEqual(resp['system'], "nps1")
        self.assertEqual(resp['phase'], "approved")
        self.assertEqual(resp['feedback'], [])

        # recheck get
        self.resp = []
        body = self.app.handle_path_request(id, req, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(resp['@id'], id)
        self.assertEqual(resp['system'], "nps1")
        self.assertEqual(resp['phase'], "approved")
        self.assertEqual(resp['feedback'], [])

        self.resp = []
        body = self.app.handle_path_request("", req, self.start, who)
        self.assertIn("200 ", self.resp[0])
        resp=self.body2dict(body)
        self.assertEqual(len(resp), 0)

                         
if __name__ == '__main__':
    test.main()



