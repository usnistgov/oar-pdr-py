import os, json, pdb
import unittest as test

import nistoar.midas.dbio.wsgi.search_sorter as srtr
from nistoar.midas.dbio.base import ACLs, ProtectedRecord, DBClient
from nistoar.midas.dbio.inmem import InMemoryDBClient

class MockRecord(ProtectedRecord):
    def __repr__(self):
        return f"<MockRecord: {self.id}>"


dbcli = InMemoryDBClient({}, { "default_shoulder": "pdr" }, "mock", "gurn")
recs = [
    MockRecord("mock", {"id": "pdr0:0001", "owner": "gomer",
                        "acls": {"write": ["gomer", "gurn"], "read": ["gomer", "gurn"],
                                 "admin": ["gomer"], "delete": ["gomer"]}}, dbcli),
    MockRecord("mock", {"id": "pdr0:0002", "owner": "gurn",
                        "acls": {"write": ["gurn"], "read": ["gomer", "gurn"],
                                 "admin": ["gomer"], "delete": ["gomer"]}}, dbcli),
    MockRecord("mock", {"id": "pdr0:0003", "owner": "gomer",
                        "acls": {"write": ["gomer"], "read": ["gomer", "gurn"],
                                 "admin": ["gomer"], "delete": ["gomer"]}}, dbcli),
    MockRecord("mock", {"id": "pdr0:0004", "owner": "alice",
                        "acls": {"write": ["bob", "alice"], "read": ["alice", "bob"],
                                 "admin": ["alice"], "delete": ["alice"]}}, dbcli),
    MockRecord("mock", {"id": "pdr0:0005", "owner": "alice",
                        "acls": {"write": ["gurn", "alice"], "read": ["alice", "gurn"],
                                 "admin": ["alice"], "delete": ["alice"]}}, dbcli),
]

class OriginalOrderTest(test.TestCase):

    def setUp(self):
        self.recs = list(recs)
        self.sorter = srtr.OriginalOrder()

    def test_add_record(self):
        self.assertEqual(len(self.sorter), 0)

        for i in range(len(self.recs)):
            self.sorter.add_record(self.recs[i])
            self.assertEqual(len(self.sorter), i+1)

    def test_pop(self):
        self.test_add_record()
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(pop=False)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i+1}" for i in range(len(self.recs))])
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted()
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i+1}" for i in range(len(self.recs))])
        self.assertEqual(len(self.sorter), 0)

    def test_reverse(self):
        self.test_add_record()
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(True, False)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in range(len(self.recs), 0, -1)])
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(True)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in range(len(self.recs), 0, -1)])
        self.assertEqual(len(self.sorter), 0)

class SortByPermTest(test.TestCase):

    def setUp(self):
        self.recs = list(recs)
        self.sorter = srtr.SortByPerm()

    def test_add_record(self):
        self.assertEqual(len(self.sorter), 0)

        for i in range(len(self.recs)):
            self.sorter.add_record(self.recs[i])
            self.assertEqual(len(self.sorter), i+1)

    def test_pop(self):
        self.test_add_record()
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(pop=False)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in [2, 1, 5, 3, 4]])
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted()
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in [2, 1, 5, 3, 4]])
        self.assertEqual(len(self.sorter), 0)

    def test_reverse(self):
        self.test_add_record()
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(True, False)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in [4, 3, 5, 1, 2]])
        self.assertEqual(len(self.sorter), 5)

        srtd = self.sorter.sorted(True)
        self.assertEqual([r.id for r in srtd],
                         [f"pdr0:000{i}" for i in [4, 3, 5, 1, 2]])
        self.assertEqual(len(self.sorter), 0)


    




if __name__ == '__main__':
    test.main()

