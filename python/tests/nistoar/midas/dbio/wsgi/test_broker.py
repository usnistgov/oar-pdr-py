import os, json, pdb, logging, tempfile
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio.wsgi import broker
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
                    "default_shoulder": "mdm1"
                },
                "default": {
                    "default_shoulder": "mdm0"
                }
            },
            "allowed_project_shoulders": ["mdm1", "spc1"],
            "default_shoulder": "mdm0"
        }
        self.fact = inmem.InMemoryDBClientFactory(self.cfg, { "nextnum": { "mdm1": 2 }})
        self.dbcli = self.fact.create_client(base.DMP_PROJECTS, nistr.actor)
        self.resp = []

    def create_broker(self, request=None):
        self.resp = []
        if not request:
            request = {'REQUEST_METHOD': 'GRUB'}
        self.broker = broker.ProjectRecordBroker(self.dbcli, self.cfg, nistr, request,
                                                 rootlog.getChild("broker"))
        return self.broker

    def test_ctor(self):
        self.create_broker()
        self.assertTrue(self.broker.dbcli)
        self.assertEqual(self.broker.cfg, self.cfg)
        self.assertEqual(self.broker.who.actor, "nstr1")
        self.assertEqual(self.broker.who.group, "midas")
        self.assertEqual(self.broker.env, {'REQUEST_METHOD': 'GRUB'})
        self.assertTrue(self.broker.log)

    def test_get_id_shoulder(self):
        self.create_broker()
        self.assertEqual(self.broker._get_id_shoulder(nistr), "mdm1")
        
        usr = prov.PubAgent("malware", prov.PubAgent.USER, "nstr1")
        self.assertEqual(self.broker._get_id_shoulder(usr), "mdm0")

        del self.cfg['clients']['default']['default_shoulder']
        self.create_broker()
        with self.assertRaises(broker.NotAuthorized):
            self.broker._get_id_shoulder(usr)
        del self.cfg['clients']['default']
        self.create_broker()
        with self.assertRaises(broker.NotAuthorized):
            self.broker._get_id_shoulder(usr)
        
        self.assertEqual(self.broker._get_id_shoulder(nistr), "mdm1")

    def test_extract_data_part(self):
        data = {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A", "vec": [22, 11, 0], "desc": {"a": 1}}}
        self.create_broker()
        self.assertEqual(self.broker._extract_data_part(data, "color"), "red")
        self.assertEqual(self.broker._extract_data_part(data, "pos"),
                         {"x": 23, "y": 12, "grid": "A", "vec": [22, 11, 0], "desc": {"a": 1}})
        self.assertEqual(self.broker._extract_data_part(data, "pos/vec"), [22, 11, 0])
        self.assertEqual(self.broker._extract_data_part(data, "pos/y"), 12)
        self.assertEqual(self.broker._extract_data_part(data, "pos/desc/a"), 1)
        with self.assertRaises(broker.ObjectNotFound):
            self.broker._extract_data_part(data, "pos/desc/b")
        

    def test_create_record(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("goob"))
        
        prec = self.broker.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {})
        self.assertEqual(prec.meta, {})
        self.assertEqual(prec.owner, "nstr1")

        self.assertTrue(self.broker.dbcli.name_exists("goob"))
        prec2 = self.broker.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdm1:0003")
        self.assertEqual(prec2.data, {})
        self.assertEqual(prec2.meta, {})
        self.assertEqual(prec2.owner, "nstr1")

        with self.assertRaises(broker.AlreadyExists):
            self.broker.create_record("goob")

    def test_create_record_withdata(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("gurn"))
        
        prec = self.broker.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {"color": "red"})
        self.assertEqual(prec.meta, {})

    def test_get_data(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("gurn"))
        prec = self.broker.create_record("gurn", {"color": "red", "pos": {"x": 23, "y": 12, "desc": {"a": 1}}})
        self.assertTrue(self.broker.dbcli.name_exists("gurn"))

        self.assertEqual(self.broker.get_data(prec.id),
                         {"color": "red", "pos": {"x": 23, "y": 12, "desc": {"a": 1}}})
        self.assertEqual(self.broker.get_data(prec.id, "color"), "red")
        self.assertEqual(self.broker.get_data(prec.id, "pos"), {"x": 23, "y": 12, "desc": {"a": 1}})
        self.assertEqual(self.broker.get_data(prec.id, "pos/desc"), {"a": 1})
        self.assertEqual(self.broker.get_data(prec.id, "pos/desc/a"), 1)
        
        with self.assertRaises(broker.ObjectNotFound):
            self.broker.get_data(prec.id, "pos/desc/b")
        with self.assertRaises(broker.ObjectNotFound):
            self.broker.get_data("goober")
        
        

    def test_update_replace_data(self):
        self.create_broker()
        self.assertTrue(not self.broker.dbcli.name_exists("goob"))
        
        prec = self.broker.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {})
        self.assertEqual(prec.meta, {})

        data = self.broker.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        prec = self.broker.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})

        data = self.broker.update_data(prec.id, {"y": 1, "z": 10, "grid": "B"}, "pos")
        self.assertEqual(data, {"x": 23, "y": 1, "z": 10, "grid": "B"})
        prec = self.broker.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "B"}})
        
        data = self.broker.update_data(prec.id, "C", "pos/grid")
        self.assertEqual(data, "C")
        prec = self.broker.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "C"}})

        # replace
        data = self.broker.replace_data(prec.id, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        self.assertEqual(data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        prec = self.broker.get_record(prec.id)
        self.assertEqual(prec.data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})

        # update again
        data = self.broker.update_data(prec.id, "blue", "color")
        self.assertEqual(data, "blue")
        prec = self.broker.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "blue", "pos": {"vec": [15, 22, 1], "grid": "Z"}})

        with self.assertRaises(broker.PartNotAccessible):
            self.broker.update_data(prec.id, 2, "pos/vec/x")
        


                         
if __name__ == '__main__':
    test.main()
        
        

        
