import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test

from pymongo import MongoClient

from nistoar.nsd import service as serv
from nistoar.base.config import ConfigurationException

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
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

testdir = Path(__file__).parents[0]
datadir = testdir / 'data'

dburl = None
if os.environ.get('MONGO_TESTDB_URL'):
    dburl = os.environ.get('MONGO_TESTDB_URL')
dbname = re.sub(r'\?.*$', '', dburl).split('/')[-1]
assert dbname

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMongoPeopleService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dir": datadir
        }
        self.svc = serv.MongoPeopleService(dburl)

    def tearDown(self):
        self.svc._cli.drop_database(dbname)

    def test_load(self):
        self.assertEqual(self.svc._cli[dbname]['OUs'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Divisions'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Groups'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['People'].count_documents({}), 0)

        self.svc.load(self.cfg, rootlog)
        self.assertEqual(self.svc._cli[dbname]['OUs'].count_documents({}), 4)
        self.assertEqual(self.svc._cli[dbname]['Divisions'].count_documents({}), 2)
        self.assertEqual(self.svc._cli[dbname]['Groups'].count_documents({}), 2)
        self.assertEqual(self.svc._cli[dbname]['People'].count_documents({}), 4)
        
    def test_OUs(self):
        self.svc.load(self.cfg, rootlog, False)
        ous = self.svc.OUs()
        self.assertEqual(len(ous), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in ous]), set("DOL DOC DOF DOS".split()))
    
    def test_divs(self):
        self.svc.load(self.cfg, rootlog, False)
        divs = self.svc.divs()
        self.assertEqual(len(divs), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in divs]), set("LERA SAA".split()))
    
    def test_groups(self):
        self.svc.load(self.cfg, rootlog, False)
        grps = self.svc.groups()
        self.assertEqual(len(grps), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in grps]), set("BWM VTA".split()))

    def test_select_people(self):
        self.svc.load(self.cfg, rootlog, False)

        peops = self.svc.select_people({"firstName": ["Phillip"]})
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]})
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]})
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = self.svc.select_people({"groupNumber": ["10001"]})
        self.assertEqual(len(peops), 3)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Ossman Bergman".split()))

    def test_get_person(self):
        self.svc.load(self.cfg, rootlog, False)

        who = self.svc.get_person(11)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Austin")

        who = self.svc.get_person(10)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Proctor")

    def test_get_OU(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_OU(3)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOF")

        what = self.svc.get_OU(1)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOL")

    def test_get_div(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_div(5)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "LERA")

        what = self.svc.get_div(6)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "SAA")

    def test_get_group(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_group(7)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "BWM")

        what = self.svc.get_group(8)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "VTA")
        
                         
if __name__ == '__main__':
    test.main()
