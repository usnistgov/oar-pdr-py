import os, json, pdb, logging, tempfile
from collections import OrderedDict
from io import StringIO
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.wsgi import wsgiapp as app
from nistoar.pdr.publish import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_wsgiapp.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_wsgiapp.log"))
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

nistr = prov.PubAgent("midas", prov.PubAgent.USER, "nstr1")

class TestAbout(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2dict(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.data = {
            "goob": "gurn",
            "foo": {
                "bar": [1, 2, 3]
            }
        }
        self.app = app.About(rootlog, self.data)
        self.resp = []

    def test_ctor(self):
        self.assertEqual(sorted(list(self.app.data.keys())), "foo goob message".split())
        self.assertEqual(self.app.data['message'], "Service is available")
        for key in self.data.keys():
            self.assertEqual(self.app.data[key], self.data[key])

    def test_add_stuff(self):
        self.assertNotIn("hairdos",  self.app.data)
        self.assertNotIn("services", self.app.data)
        self.assertNotIn("versions", self.app.data)

        self.app.add_component("hairdos", "bob", { "color": "brunette" })
        self.app.add_component("hairdos", "beehive", { "color": "blond" })
        self.assertIn("hairdos", self.app.data)
        self.assertEqual(sorted(list(self.app.data["hairdos"].keys())), ["beehive", "bob"])
        self.assertEqual(self.app.data["hairdos"]["bob"], { "color": "brunette" })
        self.assertEqual(self.app.data["hairdos"]["beehive"], { "color": "blond" })
        self.app.add_component("hairdos", "beehive", { "color": "red" })
        self.assertEqual(self.app.data["hairdos"]["beehive"], { "color": "red" })

        self.app.add_service("dmp", {"title": "DMP svc"})
        self.assertIn("hairdos", self.app.data)
        self.assertIn("services", self.app.data)
        self.assertNotIn("versions", self.app.data)
        self.assertEqual(self.app.data["services"]["dmp"], { "title": "DMP svc" })

        self.app.add_service("dap", [1, 2, 3])        
        self.assertEqual(self.app.data["services"]["dmp"], { "title": "DMP svc" })
        self.assertEqual(self.app.data["services"]["dap"], [1, 2, 3])

        self.app.add_version("pdr0", "internal")
        self.assertIn("hairdos", self.app.data)
        self.assertIn("services", self.app.data)
        self.assertIn("versions", self.app.data)
        self.assertEqual(self.app.data["services"]["dmp"], { "title": "DMP svc" })
        self.assertEqual(self.app.data["services"]["dap"], [1, 2, 3])
        self.assertEqual(self.app.data["versions"]["pdr0"], "internal")
        self.app.add_version("pdr0", False)
        self.assertEqual(self.app.data["versions"]["pdr0"], False)

    def test_get(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/'
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])

        data = self.body2dict(body)
        self.assertEqual(sorted(list(self.app.data.keys())), "foo goob message".split())
        self.assertEqual(data['message'], "Service is available")
        for key in self.data.keys():
            self.assertEqual(data[key], self.data[key])
        
        self.data['message'] = "Services are ready"
        self.app = app.About(rootlog, self.data)
        self.resp = []
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertEqual(sorted(list(self.app.data.keys())), "foo goob message".split())
        self.assertEqual(self.app.data['message'], "Services are ready")
        for key in self.data.keys():
            self.assertEqual(self.app.data[key], self.data[key])

    def test_notfound(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas'
        }
        body = self.app(req, self.start)
        self.assertIn("404 ", self.resp[0])


class TestSubAppFactory(test.TestCase):

    def setUp(self):
        self.config = {
            "about": {
                "title": "MIDAS Authoring Services",
                "describedBy": "https://midas3.nist.gov/midas/apidocs",
                "href": "http://midas3.nist.gov/midas/dmp"
            },
            "services": {
                "dmp": {
                    "about": {
                        "message": "DMP Service is available",
                        "title": "Data Management Plan (DMP) Authoring API",
                        "describedBy": "https://midas3.nist.gov/midas/apidocs",
                        "href": "http://midas3.nist.gov/midas/dmp"
                    },
                    "foo": "but",
                    "gurn": "goob",
                    "default_convention": "mdm1",
                    "conventions": {
                        "mdm1": {
                            "about": {
                                "title": "Data Management Plan (DMP) Authoring API (mdm1 convention)",
                                "describedBy": "https://midas3.nist.gov/midas/apidocs/dmp/mdm1",
                                "href": "http://midas3.nist.gov/midas/dmp/mdm1",
                                "version": "mdm1"
                            },
                            "foo": "bar",
                            "ab": 2
                        },
                        "mdm2": {
                            "about": {
                                "title": "Data Management Plan (DMP) Authoring API (mdm2 convention)",
                                "describedBy": "https://midas3.nist.gov/midas/apidocs/dmp/mdm2",
                                "href": "http://midas3.nist.gov/midas/dmp/mdm2",
                                "version": "mdm2"
                            },
                            "type": "dmp/mdm1"
                        }
                    }
                },
                "dap": {
                    "about": {
                        "message": "DAP Service is available",
                        "title": "Data Asset Publication (DAP) Authoring API",
                        "describedBy": "https://midas3.nist.gov/midas/apidocs/dap",
                        "href": "http://midas3.nist.gov/midas/dap"
                    },
                    "project_name": "drafts",
                    "type": "dmp/mdm1"
                },
                "pyu": {
                    "about": {
                        "describedBy": "https://midas3.nist.gov/midas/apidocs/pyu",
                        "href": "http://midas3.nist.gov/midas/pyu"
                    }
                }
            }
        }
        self.fact = app.SubAppFactory(self.config, app._MIDASSubApps)

    def test_ctor_register(self):
        self.assertTrue(bool(self.fact.cfg))
        self.assertTrue(bool(self.fact.subapps))
        self.assertIn("dmp/mdm1", self.fact.subapps)
        self.assertNotIn("dap", self.fact.subapps)

        self.fact.register_subapp("dap", app._MIDASSubApps["dmp/mdm1"])
        self.assertIn("dmp/mdm1", self.fact.subapps)
        self.assertIn("dap", self.fact.subapps)
        self.assertIs(self.fact.subapps["dmp/mdm1"], self.fact.subapps["dap"])

    def test_config_for_convention(self):
        cfg = self.fact.config_for_convention("dmp", "mdm1")
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg["gurn"], "goob")
        self.assertEqual(cfg["ab"], 2)
        self.assertEqual(cfg["foo"], "bar")
        self.assertEqual(cfg["default_convention"], "mdm1")
        self.assertEqual(cfg["about"]["version"], "mdm1")
        self.assertEqual(cfg["type"], "dmp/mdm1")
        self.assertEqual(cfg["project_name"], "dmp")
        self.assertNotIn("conventions", cfg)
        
        cfg = self.fact.config_for_convention("dmp", "def")
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg["gurn"], "goob")
        self.assertEqual(cfg["ab"], 2)
        self.assertEqual(cfg["foo"], "bar")
        self.assertEqual(cfg["default_convention"], "mdm1")
        self.assertEqual(cfg["about"]["version"], "mdm1")
        self.assertEqual(cfg["type"], "dmp/mdm1")
        self.assertEqual(cfg["project_name"], "dmp")
        self.assertNotIn("conventions", cfg)
        
        cfg = self.fact.config_for_convention("dmp", "")
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg["gurn"], "goob")
        self.assertEqual(cfg["ab"], 2)
        self.assertEqual(cfg["foo"], "bar")
        self.assertEqual(cfg["default_convention"], "mdm1")
        self.assertEqual(cfg["about"]["version"], "mdm1")
        self.assertEqual(cfg["type"], "dmp/mdm1")
        self.assertEqual(cfg["project_name"], "dmp")
        self.assertNotIn("conventions", cfg)
        
        cfg = self.fact.config_for_convention("dmp", "mdm2")
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg["gurn"], "goob")
        self.assertNotIn("ab", cfg)
        self.assertEqual(cfg["foo"], "but")
        self.assertEqual(cfg["default_convention"], "mdm1")
        self.assertEqual(cfg["about"]["version"], "mdm2")
        self.assertEqual(cfg["type"], "dmp/mdm1")
        self.assertEqual(cfg["project_name"], "dmp")
        self.assertNotIn("conventions", cfg)
        
        cfg = self.fact.config_for_convention("dmp", "mdm2", "hank")
        self.assertEqual(cfg["foo"], "but")
        self.assertEqual(cfg["project_name"], "dmp")
        self.assertEqual(cfg["type"], "hank")

        cfg = self.fact.config_for_convention("dap", None)
        self.assertEqual(cfg["type"], "dmp/mdm1")
        self.assertEqual(cfg["project_name"], "drafts")
        self.assertIn("about", cfg)
        self.assertNotIn("conventions", cfg)
        
        cfg = self.fact.config_for_convention("project", "")
        self.assertIsNone(cfg)

        cfg = self.fact.config_for_convention("pyu", "def")
        self.assertIn("about", cfg)
        self.assertEqual(cfg["project_name"], "pyu")
        self.assertEqual(cfg["type"], "pyu/def")
        

    def test_create_subapp(self):
        subapp = self.fact.create_subapp(rootlog, app.DEF_DBIO_CLIENT_FACTORY_CLASS({}),
                                         {"project_name": "pj", "type": "dmp/mdm1", "a": "b"})
        self.assertTrue(subapp)
        self.assertTrue(isinstance(subapp, app.prj.MIDASProjectApp))
        self.assertEqual(subapp.cfg["a"], "b")
        self.assertEqual(subapp._name, "pj")

        with self.assertRaises(KeyError):
            self.fact.create_subapp(rootlog, app.DEF_DBIO_CLIENT_FACTORY_CLASS({}),
                                    {"project_name": "pj", "a": "b"}, "dap")
        with self.assertRaises(app.ConfigurationException):
            self.fact.create_subapp(rootlog, app.DEF_DBIO_CLIENT_FACTORY_CLASS({}),
                                    {"project_name": "pj", "a": "b"})

    def test_create_suite(self):
        subapps = self.fact.create_suite(rootlog, app.DEF_DBIO_CLIENT_FACTORY_CLASS({}))
        self.assertTrue(subapps)
        self.assertTrue(isinstance(subapps["dmp/mdm1"], app.prj.MIDASProjectApp))
        self.assertTrue(isinstance(subapps["dmp/mdm2"], app.prj.MIDASProjectApp))
        self.assertTrue(isinstance(subapps["dap/def"], app.prj.MIDASProjectApp))
        self.assertTrue(isinstance(subapps[""], app.About))
        self.assertTrue(isinstance(subapps["dmp"], app.About))
        self.assertTrue(isinstance(subapps["dap"], app.About))
        self.assertNotIn("pyu/def", subapps)
        self.assertNotIn("pyu", subapps)

        self.assertIn("message", subapps[""].data)
        self.assertIn("services", subapps[""].data)
        self.assertIn("href", subapps[""].data)

class TestMIDASApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def body2dict(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.resp = []
        self.config = {
            "dbio": { },
            "about": {
                "title": "MIDAS Authoring Services",
                "describedBy": "https://midas3.nist.gov/midas/apidocs",
                "href": "http://midas3.nist.gov/midas/dmp"
            },
            "services": {
                "dmp": {
                    "about": {
                        "message": "DMP Service is available",
                        "title": "Data Management Plan (DMP) Authoring API",
                        "describedBy": "https://midas3.nist.gov/midas/apidocs",
                        "href": "http://midas3.nist.gov/midas/dmp"
                    },
                    "broker": {
                        "clients": {
                            "midas": {
                                "default_shoulder": "mdm1"
                            },
                            "default": {
                                "default_shoulder": "mdm0"
                            }
                        }
                    },
                    "dbio": {
                        "default_convention": "mdm1",
                        "superusers": [ "rlp" ],
                        "allowed_project_shoulders": ["mdm1", "spc1"],
                        "default_shoulder": "mdm0"
                    },
                    "conventions": {
                        "mdm1": {
                            "about": {
                                "title": "Data Management Plan (DMP) Authoring API (mdm1 convention)",
                                "describedBy": "https://midas3.nist.gov/midas/apidocs/dmp/mdm1",
                                "href": "http://midas3.nist.gov/midas/dmp/mdm1",
                                "version": "mdm1"
                            }
                        },
                        "mdm2": {
                            "about": {
                                "title": "Data Management Plan (DMP) Authoring API (mdm2 convention)",
                                "describedBy": "https://midas3.nist.gov/midas/apidocs/dmp/mdm2",
                                "href": "http://midas3.nist.gov/midas/dmp/mdm2",
                                "version": "mdm2"
                            },
                            "type": "dmp/mdm2"
                        }
                    }
                },
                "dap": {
                    "about": {
                        "message": "DAP Service is available",
                        "title": "Data Asset Publication (DAP) Authoring API",
                        "describedBy": "https://midas3.nist.gov/midas/apidocs/dap",
                        "href": "http://midas3.nist.gov/midas/dap"
                    },
                    "project_name": "drafts",
                    "type": "dmp/mdm1",
                    "broker": {
                        "clients": {
                            "default": {
                                "default_shoulder": "mds3"
                            }
                        }
                    },
                    "dbio": {
                        "default_convention": "mds3",
                        "superusers": [ "rlp" ],
                        "allowed_project_shoulders": ["mds3", "pdr0"],
                        "default_shoulder": "mds3"
                    },
                },
                "pyu": {
                    "about": {
                        "describedBy": "https://midas3.nist.gov/midas/apidocs/pyu",
                        "href": "http://midas3.nist.gov/midas/pyu"
                    }
                }
            }
        }
        self.clifact = inmem.InMemoryDBClientFactory({})
        self.app = app.MIDASApp(self.config, self.clifact)
        self.data = self.clifact._db

    def test_ctor(self):
        self.assertEqual(self.app.base_ep, ['midas'])
        self.assertIn("dmp/mdm1", self.app.subapps)
        self.assertIn("dmp/mdm1", self.app.subapps)
        self.assertNotIn("dmp/mdm2", self.app.subapps)
        self.assertIn("dap/def", self.app.subapps)
        self.assertIn("dap", self.app.subapps)
        self.assertIn("dmp", self.app.subapps)
        self.assertIn("", self.app.subapps)
        self.assertNotIn("pyu/def", self.app.subapps)
        self.assertNotIn("pyu", self.app.subapps)

        self.assertTrue(self.app.subapps["dmp/mdm1"]._dbfact)
    
        self.assertEqual(self.data["dmp"], {})
        self.assertEqual(self.data["draft"], {})

    def test_about_suite(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas'
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body2dict(body)
        self.assertTrue(data["message"], "Service is available")
        self.assertIn("https://", data["describedBy"])
        self.assertIn("http://", data["href"])
        self.assertIn("services", data)
        self.assertIn("dmp", list(data['services'].keys()))
        self.assertIn("dap", list(data['services'].keys()))
        self.assertEqual(len(data["services"]), 2)
        self.assertNotIn("versions", data)
        
    def test_about_dmp(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp/'
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body2dict(body)
        self.assertTrue(data["message"], "Service is available")
        self.assertIn("https://", data["describedBy"])
        self.assertIn("http://", data["href"])
        self.assertEqual(list(data['versions'].keys()), ["mdm1"])
        self.assertEqual(len(data["versions"]), 1)
        self.assertNotIn("services", data)
        
    def test_about_dap(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dap/'
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body2dict(body)
        self.assertTrue(data["message"], "Service is available")
        self.assertIn("https://", data["describedBy"])
        self.assertIn("http://", data["href"])
        self.assertIn("versions", data)
        self.assertEqual(list(data['versions'].keys()), ["def"])
        self.assertNotIn("services", data)
        
    def test_dmp(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp/mdm1'
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(data, [])

        self.resp = []
        inp = {
            "name": "gary",
            "data": {
                "color": "red"
            }
        }
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/midas/dmp/mdm1',
            'wsgi.input': StringIO(json.dumps(inp))
        }
        body = self.app(req, self.start)
        self.assertIn("201 ", self.resp[0])
        data = self.body2dict(body)

        self.assertEqual(data["name"], "gary")
        self.assertEqual(data["data"], {"color": "red"})
        self.assertEqual(data["id"], "mdm0:0001")
        self.assertEqual(data["owner"], "anonymous")
        self.assertEqual(data["type"], "dmp")

        self.assertEqual(self.data["dmp"]["mdm0:0001"]["name"], "gary")
        self.assertEqual(self.data["dmp"]["mdm0:0001"]["data"], {"color": "red"})
        self.assertEqual(self.data["dmp"]["mdm0:0001"]["id"], "mdm0:0001")
        self.assertEqual(self.data["dmp"]["mdm0:0001"]["owner"], "anonymous")

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp/mdm1/mdm0:0001'
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(data["name"], "gary")
        self.assertEqual(data["data"], {"color": "red"})
        self.assertEqual(data["id"], "mdm0:0001")
        self.assertEqual(data["owner"], "anonymous")
        self.assertEqual(data["type"], "dmp")

        self.resp = []
        req = {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': '/midas/dmp/mdm1/mdm0:0001/data',
            'wsgi.input': StringIO(json.dumps({"size": "grande"}))
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(data, {"color": "red", "size": "grande"})

        self.resp = []
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': '/midas/dmp/mdm1/mdm0:0001/name',
            'wsgi.input': StringIO(json.dumps("bob"))
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(data, "bob")

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp/mdm1/mdm0:0001'
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(data["name"], "bob")
        self.assertEqual(data["data"], {"color": "red", "size": "grande"})
        self.assertEqual(data["id"], "mdm0:0001")
        self.assertEqual(data["owner"], "anonymous")
        self.assertEqual(data["type"], "dmp")

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dmp/mdm1/mdm0:0001/meta'
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])
        self.assertEqual(data, {})

    def test_dap(self):
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dap/def'
        }
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body2dict(body)
        self.assertEqual(data, [])

        self.resp = []
        inp = {
            "name": "gary",
            "data": {
                "color": "red"
            }
        }
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/midas/dap/def',
            'wsgi.input': StringIO(json.dumps(inp))
        }
        body = self.app(req, self.start)
        self.assertIn("201 ", self.resp[0])
        data = self.body2dict(body)

        self.assertEqual(data["name"], "gary")
        self.assertEqual(data["data"], {"color": "red"})
        self.assertEqual(data["id"], "mds3:0001")
        self.assertEqual(data["owner"], "anonymous")
        self.assertEqual(data["type"], "drafts")

        self.assertEqual(self.data["drafts"]["mds3:0001"]["name"], "gary")
        self.assertEqual(self.data["drafts"]["mds3:0001"]["data"], {"color": "red"})
        self.assertEqual(self.data["drafts"]["mds3:0001"]["id"], "mds3:0001")
        self.assertEqual(self.data["drafts"]["mds3:0001"]["owner"], "anonymous")

        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/midas/dap/def/mds3:0001'
        }
        body = self.app(req, self.start)
        data = self.body2dict(body)
        self.assertIn("200 ", self.resp[0])

        self.assertEqual(data["name"], "gary")
        self.assertEqual(data["data"], {"color": "red"})
        self.assertEqual(data["id"], "mds3:0001")
        self.assertEqual(data["owner"], "anonymous")
        self.assertEqual(data["type"], "drafts")


        
    

if __name__ == '__main__':
    test.main()
        
        
