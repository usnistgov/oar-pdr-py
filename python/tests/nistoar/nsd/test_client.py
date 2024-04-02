import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test

from pymongo import MongoClient

from nistoar.nsd import client
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

serviceurl = None
if os.environ.get('PEOPLE_TEST_URL'):
    serviceurl = os.environ.get('PEOPLE_TEST_URL')

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestNSDClient(test.TestCase):

    def setUp(self):
        self.cli = client.NSDClient(serviceurl)

    def test_OUs(self):
        ous = self.cli.OUs()
        self.assertEqual(len(ous), 4)
        self.assertEqual(set([u.abbrev for u in ous]), set("DOL DOC DOF DOS".split()))

    def test_divs(self):
        divs = self.cli.divs()
        self.assertEqual(len(divs), 2)
        self.assertEqual(set([u.abbrev for u in divs]), set("LERA SAA".split()))

    def test_groups(self):
        groups = self.cli.groups()
        self.assertEqual(len(groups), 2)
        self.assertEqual(set([u.abbrev for u in groups]), set("BWM VTA".split()))

    def test_get_person(self):
        who = self.cli.get_person(13)
        self.assertIsNotNone(who)
        self.assertEqual(who['lastName'], 'Bergman')

        who = self.cli.get_person(0)
        self.assertIsNone(who)
        
    def test_select_people(self):
        who = self.cli.select_people(fname="Phillip")
        self.assertEqual(len(who), 2)
        self.assertEqual(set([u['lastName'] for u in who]), set("Austin Proctor".split()))

        who = self.cli.select_people(name="Phillip")
        self.assertEqual(len(who), 2)
        self.assertEqual(set([u['lastName'] for u in who]), set("Austin Proctor".split()))

        who = self.cli.select_people(name="Phillip", lname="Bergman")
        self.assertEqual(len(who), 3)
        self.assertEqual(set([u['lastName'] for u in who]), set("Austin Proctor Bergman".split()))

        who = self.cli.select_people(fname="David", lname="Bergman")
        self.assertEqual(len(who), 2)
        self.assertEqual(set([u['lastName'] for u in who]), set("Ossman Bergman".split()))

        who = self.cli.select_people(fname=["David", "Phillip"], name=["Bergman"])
        self.assertEqual(len(who), 4)
        self.assertEqual(set([u['lastName'] for u in who]), set("Proctor Austin Ossman Bergman".split()))

        who = self.cli.select_people(fname=["David"], ou=[3])  # note: ou is ORed (arg)
        self.assertEqual(len(who), 4)
        self.assertEqual(set([u['lastName'] for u in who]), set("Proctor Austin Ossman Bergman".split()))

        who = self.cli.select_people(fname=["David"], ou=["DOF", 2])  # note: ou is ORed (arg)
        self.assertEqual(len(who), 4)
        self.assertEqual(set([u['lastName'] for u in who]), set("Proctor Austin Ossman Bergman".split()))

        

                         
if __name__ == '__main__':
    test.main()

