import os, pdb, sys, json, requests, logging, time, re, hashlib
from pathlib import Path
from io import StringIO
import unittest as test

from nistoar.testing import *
from nistoar.pdr.publish.service import wsgi
import nistoar.pdr.preserve.bagit.builder as bldr
from nistoar.pdr.publish import prov
from nistoar.pdr import utils

datadir = Path(__file__).parents[3] / 'preserve' / 'data'

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_pdp.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    rmtmpdir()

tstag = prov.PubAgent("test", prov.PubAgent.AUTO, "tester")
ncnrag = prov.PubAgent("ncnr", prov.PubAgent.AUTO, "tester")

class TestPDPWSGI(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.tf = Tempfiles()
        self.workdir = self.tf.mkdir("pdp0")
        self.mintdir = self.tf.mkdir("idregs")
        self.bagparent = Path(self.workdir) / 'sipbags'
        bgrcfg = {
            "bag_builder": {
                "validate_id": True,
                "init_bag_info": {
                    'NIST-BagIt-Version': "X.3",
                    "Organization-Address": ["100 Bureau Dr.",
                                             "Gaithersburg, MD 20899"]
                },
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.7"
            },
            "finalize": {},
            "doi_naan": "10.18434",
            "repo_base_url": "https://test.pdr.net/"
        }
        
        pdp0cfg = {
            "working_dir": self.workdir,
            "clients": {
                "ncnr": {
                    "default_shoulder": "ncnr0",
                    "localid_provider": True,
                    "auth_key": "NCNRdev"
                },
                "default": {
                    "default_shoulder": "pdp0",
                    "localid_provider": False,
                    "auth_key": "MIDASdev"
                }
            },
            "shoulders": {
                "ncnr0": {
                    "allowed_clients": [ "ncnr" ],
                    "bagger": {
                        "override_config_for": "pdp0",
                        "factory_function": "nistoar.pdr.publish.service.pdp.PDPBaggerFactory"
                    },
                    "id_minter": {
                        "naan": "88434",
                        "based_on_sipid": True,
                        "sequence_start": 21
                    }
                },
                "pdp0": {
                    "allowed_clients": [ "test" ],
                    "bagger": bgrcfg,
                    "id_minter": {
                        "naan": "88434",
                        "sequence_start": 17
                    }
                }
            }
        }

        self.cfg = {
            'authorized': [
                {
                    "auth_key": "NCNRTOKEN",
                    "user":     "gurn",
                    "group":    "ncnr"
                },
                {
                    "auth_key": "DRAFTTOKEN",
                    "user":     "draft",
                    "group":    "test"
                }
            ],
            'conventions': {
                'pdp0': pdp0cfg
            }
        }
        self.app = wsgi.app(self.cfg)
        self.resp = []

    def tearDown(self):
        self.tf.clean()

    def test_authenticate(self):
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/pdp0',
            'HTTP_AUTHORIZATION': "Bearer NCNRTOKEN"
        }
        who = self.app.authenticate(req)
        self.assertIsNotNone(who)
        self.assertEqual(who.actor, "gurn")
        self.assertEqual(who.group, "ncnr")
        self.assertEqual(who.actor_type, "auto")

        req['HTTP_X_OAR_USER'] = "tester"
        req['HTTP_AUTHORIZATION'] = "Bearer DRAFTTOKEN"
        who = self.app.authenticate(req)
        self.assertIsNotNone(who)
        self.assertEqual(who.actor, "tester")
        self.assertEqual(who.group, "test")
        self.assertEqual(who.actor_type, "user")

        req['HTTP_AUTHORIZATION'] = "DRAFTTOKEN"
        who = self.app.authenticate(req)
        self.assertIsNone(who)

        del req['HTTP_AUTHORIZATION']
        who = self.app.authenticate(req)
        self.assertIsNone(who)

    def test_ready(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer DRAFTTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ['Publishing service is up.\n'])
        
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/'
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ['Publishing service is up.\n'])

    def test_unauthorized(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0'
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("401 ", self.resp[0])

    def test_create_res(self):
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

        nerd = utils.read_json(str(datadir / 'simplesip' / '_nerdm.json'))
        del nerd['@id']
        
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/pdp0',
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer DRAFTTOKEN"
        }
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("201 ", self.resp[0])

        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["pdr:status"], 'pending')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertTrue((self.bagparent / "pdp0-0017").is_dir())

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0',
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer DRAFTTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ['[\n  "pdp0-0017"\n]'])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0',
            'HTTP_AUTHORIZATION': "Bearer NCNRTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ["[]"])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0/pdp0-0017',
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer NCNRTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0/pdp0-0017',
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer DRAFTTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["pdr:status"], 'pending')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        # Now publish it
        self.resp = []
        req = {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': '/pdp0/pdp0-0017',
            'QUERY_STRING': "action=publish",
            'HTTP_X_OAR_USER': "tester",
            'HTTP_AUTHORIZATION': "Bearer DRAFTTOKEN"
        }
        body = self.tostr( self.app.handle_request(req, self.start) )
        self.assertIn("200 ", self.resp[0])
        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["pdr:status"], 'published')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        # bag dir was cleaned up
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())




    

                         
if __name__ == '__main__':
    test.main()
        
        
