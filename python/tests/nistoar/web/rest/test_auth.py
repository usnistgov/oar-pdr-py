import os, sys, pdb, json, logging, re, tempfile
import unittest as test
from copy import deepcopy
import jwt

from nistoar.testing import *
from nistoar.web import rest
from nistoar.web.rest.base import make_agent_from_nistoar_claimset, Unauthenticated

tmpdir = tempfile.TemporaryDirectory(prefix="_test_rest.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_rest.log"))
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
    tmpdir.cleanup()

class TestAuthFuncs(test.TestCase):

    def test_make_nistoar_agent_from_claimset(self):
        info = {"sub": "fed@nist.gov"}
        who = make_agent_from_nistoar_claimset("midas", info, rootlog, ["dmptool"])
        self.assertEqual(who.vehicle, "midas")
        self.assertEqual(who.agent_class, "nist")
        self.assertEqual(who.actor, "fed")
        self.assertEqual(who.delegated, ("dmptool",))
        self.assertIsNone(who.get_prop("email"))

        info = {"subject": "fed@nist.gov"}
        who = make_agent_from_nistoar_claimset("midas", info, rootlog, ["dmptool"])
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.vehicle, "midas")
        self.assertEqual(who.agent_class, "public")
        self.assertEqual(who.delegated, ("dmptool",))
        self.assertIsNone(who.get_prop("email"))

        info = {"sub": "fed", "userEmail": "fed@nist.gov", "OU": "61"}
        who = make_agent_from_nistoar_claimset("midas", info, rootlog, ["dmptool"])
        self.assertEqual(who.vehicle, "midas")
        self.assertEqual(who.agent_class, "nist")
        self.assertEqual(who.actor, "fed")
        self.assertEqual(who.delegated, ("dmptool",))
        self.assertEqual(who.get_prop("email"), "fed@nist.gov")
        self.assertEqual(who.get_prop("OU"), "61")

        who = make_agent_from_nistoar_claimset("midas", info, rootlog, ["dmptool"], "dmp")
        self.assertEqual(who.vehicle, "midas")
        self.assertEqual(who.agent_class, "dmp")
        self.assertEqual(who.actor, "fed")
        self.assertEqual(who.delegated, ("dmptool",))
        self.assertEqual(who.get_prop("email"), "fed@nist.gov")
        self.assertEqual(who.get_prop("OU"), "61")

    def test_authenticate_via_jwt(self):
        config = { "key": "XXXXX", "algorithm": "HS256", "require_expiration": False,
                   'client_agents': {'gurn': ['goob', 'gomer']},
                   'raise_on_anonymous': True, 'raise_on_invalid': True }
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp'
        }

        with self.assertRaises(Unauthenticated):
            rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")

        del config['raise_on_anonymous']
        who = rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "public")
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.delegated, ('goob', 'gomer',))

        req['HTTP_AUTHORIZATION'] = "Bearer goober"  # bad token
        req['HTTP_OAR_CLIENT_ID'] = 'ark:/88434/tl0-0001'
        with self.assertRaises(Unauthenticated):
            rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")

        config['raise_on_invalid'] = False
        who = rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "invalid")
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.delegated, ("goob", "gomer",))

        token = jwt.encode({"sub": "fed@nist.gov"}, config['key'], algorithm="HS256")
        req['HTTP_AUTHORIZATION'] = "Bearer "+token
        who = rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "nist")
        self.assertEqual(who.actor, "fed")
        self.assertEqual(who.delegated, ("goob", "gomer",))
        self.assertIsNone(who.get_prop("email"))

        token = jwt.encode({"sub": "fed", "userEmail": "fed@nist.gov", "OU": "61"},
                           config['key'], algorithm="HS256")
        req['HTTP_AUTHORIZATION'] = "Bearer "+token
        who = rest.authenticate_via_jwt("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "nist")
        self.assertEqual(who.actor, "fed")
        self.assertEqual(who.delegated, ("goob", "gomer",))
        self.assertEqual(who.get_prop("email"), "fed@nist.gov")
        self.assertEqual(who.get_prop("OU"), "61")

    def test_authenticate_via_authkey(self):
        config = { "authorized": [{"auth_key": "XXXXX", "user": "oarop", "client": "dmptool"}],
                   'client_agents': {'gurn': ['goob', 'gomer']},
                   'raise_on_anonymous': True, 'raise_on_invalid': True }
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp'
        }
        with self.assertRaises(Unauthenticated):
            rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")

        config['raise_on_anonymous'] = False
        who = rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "public")
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.delegated, ('goob', 'gomer',))
        self.assertIsNone(who.get_prop("email"))

        req['HTTP_AUTHORIZATION'] = "Bearer goober"   # bad key
        with self.assertRaises(Unauthenticated):
            rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")

        del config['raise_on_invalid']
        who = rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "invalid")
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.delegated, ('goob', 'gomer',))

        req['HTTP_AUTHORIZATION'] = "XXXXX"
        who = rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "public")
        self.assertEqual(who.actor, "anonymous")
        self.assertEqual(who.delegated, ('goob', 'gomer',))
        self.assertIsNone(who.get_prop("email"))

        req['HTTP_AUTHORIZATION'] = "Bearer XXXXX"
        who = rest.authenticate_via_authkey("midas", req, config, rootlog, ['goob', 'gomer'], "dmptool")
        self.assertEqual(who.agent_class, "dmptool")
        self.assertEqual(who.actor, "oarop")
        self.assertEqual(who.delegated, ("goob", "gomer",))
        self.assertIsNone(who.get_prop("email"))
        
        
        
        
    

if __name__ == '__main__':
    test.main()
        
        
