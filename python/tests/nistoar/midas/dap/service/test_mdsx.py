import os, json, pdb, logging, tempfile
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.wsgi import broker
from nistoar.midas.dap.service import mdsx
from nistoar.pdr.publish import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_broker.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_pdp.log"))
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

class TestProjectRecordBroker(test.TestCase):

    def setUp(self):
        self.cfg = {
            "clients": {
                "midas": {
                    "default_shoulder": "mdsx"
                },
                "default": {
                    "default_shoulder": "mdsx"
                }
            },
            "allowed_project_shoulders": ["mdsx", "spc1"],
            "default_shoulder": "mdsx",
            "assign_doi": "always",
            "doi_naan": "88888"
        }
        self.fact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsx": 2 }})
        self.dbcli = self.fact.create_client(base.DMP_PROJECTS, self.cfg, nistr.actor)
        self.resp = []

    def create_broker(self, request=None):
        self.resp = []
        if not request:
            request = {'REQUEST_METHOD': 'GRUB'}
        self.broker = mdsx.DAPBroker(self.dbcli, self.cfg, nistr, request, rootlog.getChild("broker"))
        return self.broker

    def test_ctor(self):
        self.create_broker()
        self.assertTrue(self.broker.dbcli)
        self.assertEqual(self.broker.cfg, self.cfg)
        self.assertEqual(self.broker.who.actor, "nstr1")
        self.assertEqual(self.broker.who.group, "midas")
        self.assertEqual(self.broker.env, {'REQUEST_METHOD': 'GRUB'})
        self.assertTrue(self.broker.log)

    def test_create_record(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("goob"))
        
        prec = self.broker.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdsx:0003")
        self.assertEqual(prec.meta, {"creatorisContact": True, "resourceType": "data"})
        self.assertEqual(prec.owner, "nstr1")
        for key in "_schema @context _extensionSchemas".split():
            self.assertIn(key, prec.data)
        self.assertEqual(prec.data['doi'], "88888/mdsx-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsx-0003")

        self.assertTrue(self.broker.dbcli.name_exists("goob"))
        prec2 = self.broker.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdsx:0003")
        self.assertEqual(prec2.data['@id'], "ark:/88434/mdsx-0003")
        self.assertEqual(prec2.data['doi'], "88888/mdsx-0003")
        self.assertEqual(prec2.meta, {"creatorisContact": True, "resourceType": "data"})
        self.assertEqual(prec2.owner, "nstr1")

        with self.assertRaises(broker.AlreadyExists):
            self.broker.create_record("goob")

    def test_create_record_withdata(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("gurn"))
        
        prec = self.broker.create_record("gurn", {"color": "red"},
                                         {"temper": "dark", "creatorisContact": "goob"})
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdsx:0003")
        self.assertEqual(prec.meta, {"creatorisContact": False, "resourceType": "data"})
        for key in "_schema @context _extensionSchemas".split():
            self.assertIn(key, prec.data)
        self.assertEqual(prec.data['color'], "red")
        self.assertEqual(prec.data['doi'], "88888/mdsx-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsx-0003")




                         
if __name__ == '__main__':
    test.main()
        
        
