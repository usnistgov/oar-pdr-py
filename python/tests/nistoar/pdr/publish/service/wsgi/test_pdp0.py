import os, pdb, sys, json, requests, logging, time, re, hashlib
from pathlib import Path
from io import StringIO
import unittest as test

from nistoar.testing import *
from nistoar.pdr.publish.service.wsgi import pdp0
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

class TestPDP0App(test.TestCase):

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
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.6"
            },
            "finalize": {},
            "repo_base_url": "https://test.pdr.net/"
        }
        
        self.cfg = {
            "convention": "pdp0",
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
        self.app = pdp0.PDP0App(rootlog, self.cfg)
        self.resp = []

    def tearDown(self):
        self.tf.clean()

    def test_ctor(self):
        self.assertEqual(self.app._name, "pdp0")
        self.assertTrue(self.app.svc)
        self.assertTrue(self.app.statuscfg['cachedir'])

    def test_create_handler(self):
        req = {
            'REQUEST_METHOD': 'HEAD',
            'PATH_INFO': '/',
        }
        hdlr = self.app.create_handler(req, self.start, '/', tstag)
        self.assertIs(hdlr._app, self.app)
        self.assertIsNone(hdlr._reqrec)
        self.assertEqual(hdlr._env.get('PATH_INFO'), '/')

    def test_get_action(self):
        req = {
            'REQUEST_METHOD': 'HEAD',
            'PATH_INFO': '/'
        }
        hdlr = self.app.create_handler(req, self.start, '/', tstag)
        self.assertEqual(hdlr.get_action(), "")

        req = {
            'REQUEST_METHOD': 'HEAD',
            'PATH_INFO': '/',
            'QUERY_STRING': "action=publish"
        }
        hdlr = self.app.create_handler(req, self.start, '/', tstag)
        self.assertEqual(hdlr.get_action(), "publish")

        req = {
            'REQUEST_METHOD': 'HEAD',
            'PATH_INFO': '/',
            'QUERY_STRING': "goob=gurn&action=finalize"
        }
        hdlr = self.app.create_handler(req, self.start, '/', tstag)
        self.assertEqual(hdlr.get_action(), "finalize")

    def test_not_found(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/ncnr0:goob',
        }
        body = self.app.handle_path_request(req, self.start, who=ncnrag)
        self.assertIn("404 ", self.resp[0])
        self.assertEqual(body, [])

        self.resp = []
        body = self.app.handle_path_request(req, self.start, who=tstag)
        self.assertIn("404 ", self.resp[0])
        self.assertEqual(body, [])
        
    def test_no_open_sips(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
        }
        body = self.app.handle_path_request(req, self.start, who=ncnrag)
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, [b"[]"])

        self.resp = []
        body = self.app.handle_path_request(req, self.start, who=tstag)
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, [b"[]"])

    def test_create_res(self):
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

        nerd = utils.read_json(str(datadir / 'simplesip' / '_nerdm.json'))
        del nerd['@id']
        
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/',
        }
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
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
            'PATH_INFO': '/',
        }
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ['["pdp0-0017"]'])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
        }
        body = self.tostr( self.app.handle_path_request(req, self.start, who=ncnrag) )
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(body, ["[]"])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0-0017',
        }
        body = self.tostr( self.app.handle_path_request(req, self.start, who=ncnrag) )
        self.assertIn("404 ", self.resp[0])

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/pdp0-0017',
        }
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
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
            'PATH_INFO': '/pdp0-0017',
            'QUERY_STRING': "action=publish"
        }
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
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

    def test_create_publish(self):
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

        nerd = utils.read_json(str(datadir / 'simplesip' / '_nerdm.json'))
        del nerd['@id']
        
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/',
            'QUERY_STRING': "action=publish"
        }
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
        self.assertIn("201 ", self.resp[0])

        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["pdr:status"], 'published')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

    def test_create_finalize(self):
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

        nerd = utils.read_json(str(datadir / 'simplesip' / '_nerdm.json'))
        del nerd['@id']
        
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/',
            'QUERY_STRING': "action=finalize"
        }
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_path_request(req, self.start, who=tstag) )
        self.assertIn("201 ", self.resp[0])

        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["pdr:status"], 'finalized')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertTrue((self.bagparent / "pdp0-0017").is_dir())

    def test_update(self):
        self.assertFalse((self.bagparent / "pdp0-0017").is_dir())

        nerd = utils.read_json(str(datadir / 'simplesip' / '_nerdm.json'))
        del nerd['@id']
        
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': '/ncnr0:33411'
        }
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_path_request(req, self.start, who=ncnrag) )
        self.assertIn("200 ", self.resp[0])

        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-33411pv")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:33411")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-33411pv")
        self.assertEqual(bnerd["pdr:status"], 'pending')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertNotIn("testing", bnerd["keyword"])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertTrue((self.bagparent / "ncnr0:33411").is_dir())

        nerd['accessLevel'] = "restricted public"
        nerd['keyword'].append("testing")
        req['wsgi.input'] = StringIO(json.dumps(nerd))
        body = self.tostr( self.app.handle_path_request(req, self.start, who=ncnrag) )
        self.assertIn("200 ", self.resp[0])

        bnerd = json.loads("\n".join(body))
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-33411pv")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:33411")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-33411pv")
        self.assertEqual(bnerd["pdr:status"], 'pending')
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertIn("testing", bnerd["keyword"])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertTrue((self.bagparent / "ncnr0:33411").is_dir())

    

                         
if __name__ == '__main__':
    test.main()
        
        
