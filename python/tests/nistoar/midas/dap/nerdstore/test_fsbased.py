import os, json, pdb, tempfile
from pathlib import Path
import unittest as test

import nistoar.midas.dap.nerdstore as ns
from nistoar.midas.dap.nerdstore import fsbased, inmem
from nistoar.pdr.utils import read_json, write_json

testdir = Path(__file__).parents[3] / 'pdr' / 'preserve' / 'data' / 'simplesip'
sipnerd = testdir / '_nerdm.json'

def load_simple():
    return read_json(sipnerd)

class TestFSBasedAuthorList(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.auths = fsbased.FSBasedAuthorList(inmem.InMemoryResource("pdr0:0001"), self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.auths._dir.is_dir())
        self.assertEqual(self.auths._pfx, "auth")
        self.assertEqual(self.auths._idfile, "_ids.json")
        self.assertEqual(self.auths._seqfile, "_seq.json")
        self.assertEqual(self.auths._idp.name, self.auths._idfile)
        self.assertEqual(self.auths._seqp.name, self.auths._seqfile)
        self.assertTrue(self.auths._idp.exists())
        self.assertTrue(not self.auths._seqp.exists())

    def test_new_id(self):
        self.assertTrue(not self.auths._seqp.exists())
        self.assertEqual(self.auths._new_id(), "auth_0")
        self.assertTrue(self.auths._seqp.exists())
        self.assertEqual(read_json(str(self.auths._seqp)), 1)

        self.assertEqual(self.auths._new_id(), "auth_1")
        self.assertEqual(self.auths._new_id(), "auth_2")
        self.assertEqual(self.auths._new_id(), "auth_3")
        self.assertEqual(read_json(str(self.auths._seqp)), 4)

    def test_load_authors(self):
        nerd = load_simple()
        self.auths.load_authors(nerd['authors'])
        
        self.assertEqual(self.auths._order, "auth_0 auth_1".split())
        self.assertEqual(self.auths.count, 2)
        self.assertEqual(len(self.auths), 2)
        self.assertIn("auth_0", self.auths)
        self.assertIn("auth_1", self.auths)
        self.assertNotIn("auth_2", self.auths)

        self.assertEqual(self.auths.ids, "auth_0 auth_1".split())

        mdf = self.auths._obj_file("auth_0")
        self.assertTrue(mdf.is_file())
        auth = read_json(str(mdf))
        self.assertEqual(auth['@id'], "auth_0")
        self.assertEqual(auth['familyName'], "Levine")

        mdf = self.auths._obj_file("auth_1")
        self.assertTrue(mdf.is_file())
        auth = read_json(str(mdf))
        self.assertEqual(auth['@id'], "auth_1")
        self.assertEqual(auth['familyName'], "Curry")

    def test_getsetpop(self):
        nerd = load_simple()
        self.auths.load_authors(nerd['authors'])

        # test access by id or position
        auth = self.auths.get("auth_1")
        self.assertEqual(auth['familyName'], "Curry")
        self.assertEqual(auth['givenName'], "John")
        auth = self.auths.get(1)
        self.assertEqual(auth['familyName'], "Curry")
        self.assertEqual(auth['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_0 auth_1".split())

        # an update to my copy doesn't change original
        auth['givenName'] = "Steph"
        self.assertEqual(self.auths.get(1)['givenName'], "John")
        self.assertEqual(auth['givenName'], "Steph")

        # update original via set
        self.auths.set(1, auth)
        auth = self.auths.get("auth_1")
        self.assertEqual(auth['familyName'], "Curry")
        self.assertEqual(auth['givenName'], "Steph")
        self.assertEqual(self.auths.get(1)['givenName'], "Steph")
        self.assertEqual(self.auths.ids, "auth_0 auth_1".split())

        # let's append a new author
        auth['givenName'] = "John"
        self.auths.append(auth)
        self.assertEqual(self.auths.count, 3)
        auth = self.auths.get(-1)
        self.assertEqual(auth['givenName'], "John")
        self.assertEqual(auth['@id'], "auth_2")
        self.assertEqual(self.auths.get(1)['givenName'], "Steph")
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_2".split())

        # let's prepend a new author
        auth['givenName'] = "George"
        self.auths.insert(0, auth)
        self.assertEqual(self.auths.count, 4)
        auth = self.auths.get(0)
        self.assertEqual(auth['givenName'], "George")
        self.assertEqual(auth['@id'], "auth_3")
        self.assertEqual(self.auths.get(2)['givenName'], "Steph")
        self.assertEqual(self.auths.get(1)['familyName'], "Levine")
        self.assertEqual(self.auths.get(-1)['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_3 auth_0 auth_1 auth_2".split())

        # let's insert a new author
        auth['givenName'] = "Paul"
        self.auths.insert(2, auth)
        self.assertEqual(self.auths.count, 5)
        auth = self.auths.get(2)
        self.assertEqual(auth['givenName'], "Paul")
        self.assertEqual(auth['@id'], "auth_4")
        self.assertEqual(self.auths.get(3)['givenName'], "Steph")
        self.assertEqual(self.auths.get(0)['givenName'], "George")
        self.assertEqual(self.auths.get(1)['familyName'], "Levine")
        self.assertEqual(self.auths.get(-1)['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_3 auth_0 auth_4 auth_1 auth_2".split())

        # pop from end and readd: id is retained
        auth = self.auths.pop(-1)
        self.assertEqual(self.auths.count, 4)
        self.assertEqual(auth['givenName'], "John")
        self.assertEqual(auth['@id'], "auth_2")
        self.assertNotIn(auth['@id'], self.auths)
        self.assertEqual(self.auths.ids, "auth_3 auth_0 auth_4 auth_1".split())
        self.auths.append(auth)
        self.assertEqual(self.auths.count, 5)
        self.assertIn(auth['@id'], self.auths)
        self.assertEqual(self.auths.get(-1)['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_3 auth_0 auth_4 auth_1 auth_2".split())

        # pop off from beginning
        auth = self.auths.pop(0)
        self.assertEqual(self.auths.count, 4)
        self.assertEqual(auth['givenName'], "George")
        self.assertEqual(auth['@id'], "auth_3")
        self.assertEqual(self.auths.get(1)['givenName'], "Paul")
        self.assertEqual(self.auths.get(2)['givenName'], "Steph")
        self.assertEqual(self.auths.get(0)['familyName'], "Levine")
        self.assertEqual(self.auths.get(-1)['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_0 auth_4 auth_1 auth_2".split())

        # pop out from a position
        auth = self.auths.pop(2)
        self.assertEqual(self.auths.count, 3)
        self.assertEqual(auth['givenName'], "Steph")
        self.assertEqual(auth['@id'], "auth_1")
        self.assertEqual(self.auths.get(0)['familyName'], "Levine")
        self.assertEqual(self.auths.get(1)['givenName'], "Paul")
        self.assertEqual(self.auths.get(2)['givenName'], "John")
        self.assertEqual(self.auths.get(-1)['givenName'], "John")
        self.assertEqual(self.auths.ids, "auth_0 auth_4 auth_2".split())

        # pop off from end
        auth = self.auths.pop(-1)
        self.assertEqual(self.auths.count, 2)
        self.assertEqual(auth['givenName'], "John")
        self.assertEqual(auth['@id'], "auth_2")
        self.assertEqual(self.auths.get(1)['givenName'], "Paul")
        self.assertEqual(self.auths.get(0)['familyName'], "Levine")
        self.assertEqual(self.auths.get(-1)['givenName'], "Paul")
        self.assertEqual(self.auths.ids, "auth_0 auth_4".split())

    def test_iter(self):
        nerd = load_simple()
        self.auths.load_authors(nerd['authors'])

        auth = self.auths.get("auth_1")
        auth['givenName'] = "Steph"
        self.auths.append(auth)
        self.assertEqual(self.auths.count, 3)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_2".split())

        it = iter(self.auths)
        self.assertTrue(hasattr(it, '__next__'), "not an iterator")
        names = [a['givenName'] for a in it]
        self.assertEqual(names, "Zachary John Steph".split())

    def test_set_order(self):
        nerd = load_simple()
        self.auths.load_authors(nerd['authors'])

        auth = self.auths.get("auth_1")
        auth['givenName'] = "Steph"
        self.auths.append(auth)
        auth['givenName'] = "George"
        self.auths.append(auth)
        auth['givenName'] = "Paul"
        self.auths.append(auth)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_2 auth_3 auth_4".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary John Steph George Paul".split())

        self.auths.set_order("auth_3 auth_1 auth_0 auth_4 auth_2".split())
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_3 auth_1 auth_0 auth_4 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "George John Zachary Paul Steph".split())
        
        self.auths.set_order("auth_0 auth_1 auth_4".split())
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_4 auth_3 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary John Paul George Steph".split())
        
        self.auths.set_order("auth_0 auth_1 auth_2 auth_3 auth_4 auth_0 auth_8 auth_4".split())
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_2 auth_3 auth_4".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary John Steph George Paul".split())

    def test_move(self):
        nerd = load_simple()
        self.auths.load_authors(nerd['authors'])

        auth = self.auths.get("auth_1")
        auth['givenName'] = "Steph"
        self.auths.append(auth)
        auth['givenName'] = "George"
        self.auths.append(auth)
        auth['givenName'] = "Paul"
        self.auths.append(auth)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_2 auth_3 auth_4".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary John Steph George Paul".split())

        # move to end
        self.auths.move("auth_1", None)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_2 auth_3 auth_4 auth_1".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary Steph George Paul John".split())
        
        self.auths.move("auth_2")
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_3 auth_4 auth_1 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary George Paul John Steph".split())
        
        self.auths.move("auth_2")
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_3 auth_4 auth_1 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary George Paul John Steph".split())
        
        # move to absolute position
        self.auths.move(3, 1)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_0 auth_1 auth_3 auth_4 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Zachary John George Paul Steph".split())

        self.auths.move("auth_2", -5, 0)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_2 auth_0 auth_1 auth_3 auth_4".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Steph Zachary John George Paul".split())

        self.auths.move("auth_0", 10, False)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_2 auth_1 auth_3 auth_4 auth_0".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Steph John George Paul Zachary".split())

        # push an author down
        self.auths.move(1, 2, 1)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_2 auth_3 auth_4 auth_1 auth_0".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Steph George Paul John Zachary".split())
        
        self.auths.move("auth_4", -1, True)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_2 auth_4 auth_3 auth_1 auth_0".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Steph Paul George John Zachary".split())
        
        self.auths.move(0, 10, "Goober!")
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_4 auth_3 auth_1 auth_0 auth_2".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Paul George John Zachary Steph".split())

        # pull an author up
        self.auths.move(4, 3, -1)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_4 auth_2 auth_3 auth_1 auth_0".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Paul Steph George John Zachary".split())
        
        self.auths.move("auth_3", -2, -1)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_4 auth_2 auth_1 auth_0 auth_3".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "Paul Steph John Zachary George".split())
        
        self.auths.move(-3, 20, -1)
        self.assertEqual(self.auths.count, 5)
        self.assertEqual(self.auths.ids, "auth_1 auth_4 auth_2 auth_0 auth_3".split())
        self.assertEqual([a['givenName'] for a in iter(self.auths)],
                         "John Paul Steph Zachary George".split())
        
class TestFSBasedRefList(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.refs = fsbased.FSBasedRefList(inmem.InMemoryResource("pdr0:0001"), self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.refs._dir.is_dir())
        self.assertEqual(self.refs._pfx, "ref")
        self.assertEqual(self.refs._idfile, "_ids.json")
        self.assertEqual(self.refs._seqfile, "_seq.json")
        self.assertEqual(self.refs._idp.name, self.refs._idfile)
        self.assertEqual(self.refs._seqp.name, self.refs._seqfile)
        self.assertTrue(self.refs._idp.exists())
        self.assertTrue(not self.refs._seqp.exists())
        self.assertEqual(self.refs._nxtseq, 0)

    def test_load_references(self):
        nerd = load_simple()
        for r in nerd.get('references', []):
            if '@id' in r:
                del r['@id']
        self.refs.load_references(nerd['references'])
        
        self.assertEqual(self.refs._order, "ref_0".split())
        self.assertEqual(self.refs.count, 1)
        self.assertEqual(len(self.refs), 1)
        self.assertIn("ref_0", self.refs)
        self.assertNotIn("ref_2", self.refs)

        self.assertEqual(self.refs.ids, "ref_0".split())

        mdf = self.refs._obj_file("ref_0")
        self.assertTrue(mdf.is_file())
        ref = read_json(str(mdf))
        self.assertEqual(ref['@id'], "ref_0")
        self.assertEqual(ref['refType'], "IsReferencedBy")

    def test_contains(self):
        nerd = load_simple()
        self.refs.load_references(nerd['references'])
        
        self.assertIn("pdr:ref/doi:10.1364/OE.24.014100", self.refs)
        self.assertNotIn("ref_0", self.refs)

    def test_getsetpop(self):
        nerd = load_simple()
        self.refs.load_references(nerd['references'])
        
        # test access by id or position
        ref = self.refs.get("pdr:ref/doi:10.1364/OE.24.014100")
        self.assertEqual(ref['refType'], "IsReferencedBy")

        # add a reference
        ref['refType'] = "IsSupplementTo"
        self.refs.append(ref)
        ref = self.refs.get(-1)
        self.assertEqual(ref['refType'], "IsSupplementTo")
        self.assertEqual(ref['@id'], "ref_0")
        self.assertEqual(self.refs.get(0)['refType'], "IsReferencedBy")

        # and another
        ref['refType'] = "Documents"
        self.refs.append(ref)
        ref = self.refs.get(-1)
        self.assertEqual(ref['refType'], "Documents")
        self.assertEqual(ref['@id'], "ref_1")
        self.assertEqual(self.refs.get(0)['refType'], "IsReferencedBy")
        self.assertEqual(self.refs.get(1)['refType'], "IsSupplementTo")

class TestFSBasedNonFileComps(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.cmps = fsbased.FSBasedNonFileComps(inmem.InMemoryResource("pdr0:0001"), self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.cmps._dir.is_dir())
        self.assertEqual(self.cmps._pfx, "cmp")
        self.assertEqual(self.cmps._idfile, "_ids.json")
        self.assertEqual(self.cmps._seqfile, "_seq.json")
        self.assertEqual(self.cmps._idp.name, self.cmps._idfile)
        self.assertEqual(self.cmps._seqp.name, self.cmps._seqfile)
        self.assertTrue(self.cmps._idp.exists())
        self.assertTrue(not self.cmps._seqp.exists())
        self.assertEqual(self.cmps._nxtseq, 0)

    def test_load_cmperences(self):
        nerd = load_simple()
        self.cmps.load_nonfile_components(nerd['components'])

        self.assertEqual(self.cmps._order, "cmp_0".split())
        self.assertEqual(self.cmps.count, 1)
        self.assertEqual(len(self.cmps), 1)
        self.assertIn("cmp_0", self.cmps)
        self.assertNotIn("cmp_2", self.cmps)

        self.assertEqual(self.cmps.ids, "cmp_0".split())

        mdf = self.cmps._obj_file("cmp_0")
        self.assertTrue(mdf.is_file())
        cmp = read_json(str(mdf))
        self.assertEqual(cmp['@id'], "cmp_0")
        self.assertEqual(cmp['mediaType'], "application/zip")

    def test_contains(self):
        nerd = load_simple()
        self.cmps.load_nonfile_components(nerd['components'])
        
        self.assertIn("cmp_0", self.cmps)
        self.assertNotIn("cmp_2", self.cmps)

    def test_getsetpop(self):
        nerd = load_simple()
        self.cmps.load_nonfile_components(nerd['components'])
        
        # test access by id or position
        cmp = self.cmps.get("cmp_0")
        self.assertEqual(cmp['mediaType'], "application/zip")

        # add a component
        cmp['mediaType'] = "text/plain"
        self.cmps.append(cmp)
        cmp = self.cmps.get(-1)
        self.assertEqual(cmp['mediaType'], "text/plain")
        self.assertEqual(cmp['@id'], "cmp_1")
        self.assertEqual(self.cmps.get(0)['mediaType'], "application/zip")

class TestFSBasedFileComps(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.cmps = fsbased.FSBasedFileComps(inmem.InMemoryResource("pdr0:0001"), self.outdir.name)
        
    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.cmps._dir.is_dir())
        self.assertEqual(self.cmps._pfx, "file")
        self.assertEqual(self.cmps._chldfile, "_children.json")
        self.assertEqual(self.cmps._seqfile, "_seq.json")
        self.assertEqual(self.cmps._chldp.name, self.cmps._chldfile)
        self.assertEqual(self.cmps._seqp.name, self.cmps._seqfile)
        self.assertTrue(not self.cmps._seqp.exists())
        self.assertTrue(self.cmps._chldp.exists())
        self.assertEqual(self.cmps._nxtseq, 0)
        self.assertEqual(self.cmps.count, 0)

    def test_new_id(self):
        self.assertTrue(not self.cmps._seqp.exists())
        self.assertEqual(self.cmps._new_id(), "file_0")
        self.assertTrue(self.cmps._seqp.exists())
        self.assertEqual(read_json(str(self.cmps._seqp)), 1)

        self.assertEqual(self.cmps._new_id(), "file_1")
        self.assertEqual(self.cmps._new_id(), "file_2")
        self.assertEqual(self.cmps._new_id(), "file_3")
        self.assertEqual(read_json(str(self.cmps._seqp)), 4)

        self.cmps._reserve_id("doi:goober")
        self.assertEqual(read_json(str(self.cmps._seqp)), 4)
        self.cmps._reserve_id("auth_9")
        self.assertEqual(read_json(str(self.cmps._seqp)), 10)
        self.cmps._reserve_id("file_12")
        self.assertEqual(read_json(str(self.cmps._seqp)), 13)
        self.assertEqual(self.cmps._new_id(), "file_13")

    def test_fmd_file(self):
        self.assertEqual(self.cmps._fmd_file("file_3", False), self.cmps._dir / "f:file_3.json")
        fname = self.cmps._fmd_file("goober", False)
        self.assertTrue(not self.cmps._is_coll_mdfile(fname))
        self.assertEqual(fname, self.cmps._dir / "f:goober.json")
        fname = self.cmps._fmd_file("10.88434/mds2-2341/pdr:f/goob", True)
        self.assertEqual(fname, self.cmps._dir / "c:10.88434::mds2-2341::pdr:f::goob.json")
        self.assertTrue(self.cmps._is_coll_mdfile(fname))

    def test_load_file_components(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)
        self.assertEqual(len(self.cmps._children), 3)

        fnames = [os.path.splitext(f)[0] for f in os.listdir(self.cmps._dir)
                                         if not f.startswith("_") and f.endswith(".json")]
        fnames.sort()
        self.assertEqual(fnames, "c:file_2 f:file_0 f:file_1 f:file_3".split())

        self.assertEqual(self.cmps.ids, "file_0 file_1 file_2 file_3".split())
        paths = [f['filepath'] for f in self.cmps.iter_files()]
        self.assertEqual(len(paths), 4)
        self.assertEqual(paths, "trial1.json trial2.json trial3 trial3/trial3a.json".split())

    def test_discover_toplevel_files(self):
        self.assertEqual(list(self.cmps._discover_toplevel_files()), [])
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])

        top = dict(self.cmps._discover_toplevel_files())
        for path in "trial1.json trial2.json trial3".split():
            self.assertIn(path, top)
        self.assertEqual(len(top), 3)
        self.assertEqual(top["trial1.json"], "file_0")
        self.assertEqual(top["trial2.json"], "file_1")
        self.assertEqual(top["trial3"], "file_2")

    def test_read_toplevel_files(self):
        self.assertEqual(self.cmps._read_toplevel_files(), {})
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])

        children = self.cmps._read_toplevel_files()
        for path in "trial1.json trial2.json trial3".split():
            self.assertIn(path, children)
        self.assertEqual(len(children), 3)
        self.assertEqual(children["trial1.json"], "file_0")
        self.assertEqual(children["trial2.json"], "file_1")
        self.assertEqual(children["trial3"], "file_2")

        chldf = self.cmps._dir / "_children.json"
        self.assertTrue(chldf.is_file())
        chldf.unlink()
        self.assertTrue(not chldf.exists())

        children = self.cmps._read_toplevel_files()
        for path in "trial1.json trial2.json trial3".split():
            self.assertIn(path, children)
        self.assertEqual(len(children), 3)
        self.assertEqual(children["trial1.json"], "file_0")
        self.assertEqual(children["trial2.json"], "file_1")
        self.assertEqual(children["trial3"], "file_2")

    def test_get_file(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)

        # by id
        f = self.cmps.get_file_by_id("file_1")
        self.assertEqual(f['@id'], "file_1")
        self.assertEqual(f['filepath'], "trial2.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)

        f = self.cmps.get_file_by_id("file_2")
        self.assertTrue(self.cmps.is_collection(f))
        self.assertEqual(f['@id'], "file_2")
        self.assertEqual(f['filepath'], "trial3")
        self.assertNotIn("downloadURL", f)
        self.assertNotIn("__children", f)
        self.assertTrue(isinstance(f['has_member'], list))
        self.assertTrue(len(f['has_member']), 1)
        self.assertEqual(f['has_member'][0], {"@id": "file_3", "name": "trial3a.json"})

        # by path
        f = self.cmps.get_file_by_path("trial1.json")
        self.assertEqual(f['@id'], "file_0")
        self.assertEqual(f['filepath'], "trial1.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)
        
        f = self.cmps.get_file_by_path("trial3")
        self.assertTrue(self.cmps.is_collection(f))
        self.assertEqual(f['@id'], "file_2")
        self.assertEqual(f['filepath'], "trial3")
        self.assertNotIn("downloadURL", f)
        self.assertNotIn("__children", f)
        self.assertTrue(isinstance(f['has_member'], list))
        self.assertTrue(len(f['has_member']), 1)
        self.assertEqual(f['has_member'][0], {"@id": "file_3", "name": "trial3a.json"})
        
        f = self.cmps.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(f['@id'], "file_3")
        self.assertEqual(f['filepath'], "trial3/trial3a.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)

    def test_get_ids_in_subcoll(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)

        self.assertEqual(self.cmps.get_ids_in_subcoll("trial3"), ["file_3"])
        ids = self.cmps.get_ids_in_subcoll("")
        self.assertIn("file_0", ids)
        self.assertIn("file_1", ids)
        self.assertIn("file_2", ids)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, "file_0 file_1 file_2".split())

    def test_get_subcoll_members(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)

        members = list(self.cmps.get_subcoll_members("trial3"))
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]['filepath'], "trial3/trial3a.json")

        paths = [f['filepath'] for f in self.cmps.get_subcoll_members("")]
        self.assertIn("trial1.json", paths)
        self.assertIn("trial2.json", paths)
        self.assertIn("trial3", paths)

    def test_set_order_in_subcoll(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_0 file_1 file_2".split())

        self.cmps.set_order_in_subcoll("", "file_2 file_1 file_0".split())
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_2 file_1 file_0".split())

        self.cmps.set_order_in_subcoll("", "file_1 file_0".split())
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_1 file_0 file_2".split())

        self.cmps.set_order_in_subcoll("trial3", [])
        self.assertEqual(self.cmps.get_ids_in_subcoll("trial3"), ["file_3"])

        self.cmps.set_order_in_subcoll("trial3", ["goob"])
        self.assertEqual(self.cmps.get_ids_in_subcoll("trial3"), ["file_3"])

        self.cmps.set_order_in_subcoll("trial3", ["file_3"])
        self.assertEqual(self.cmps.get_ids_in_subcoll("trial3"), ["file_3"])

        with self.assertRaises(inmem.ObjectNotFound):
            self.cmps.set_order_in_subcoll("goob", ["file_3"])

    def test_set_file_at(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_0 file_1 file_2".split())

        file = self.cmps.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertNotEqual(file.get('title'), "My Magnum Opus")
        file['title'] = "My Magnum Opus"
        self.cmps.set_file_at(file)

        file = self.cmps.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus")
        file = self.cmps.get_file_by_path("trial1.json")
        self.assertEqual(file.get('@id'), "file_0")
        self.assertEqual(file.get('title'), "My Magnum Opus")
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_0 file_1 file_2".split())

        file['title'] = "My Magnum Opus, redux"
        del file['@id']
        self.cmps.set_file_at(file, "trial1.json")

        file = self.cmps.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")

        # move the file
        self.cmps.set_file_at(file, "trial3/trial3b.json", "file_0")
        with self.assertRaises(inmem.ObjectNotFound):
            self.cmps.get_file_by_path("trial1.json")
        file = self.cmps.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial3/trial3b.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")

        # create a new file
        fmdf = self.cmps._dir / "f:file_4.json"
        self.assertTrue(not fmdf.exists())
        del file['@id']
        file['title'] = "My Magnum Opus, reloaded"
        self.cmps.set_file_at(file, "trial1.json")
        file = self.cmps.get_file_by_path("trial1.json")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, reloaded")
        self.assertEqual(file.get('@id'), "file_4")
        self.assertTrue(fmdf.is_file())

        # move a file onto an existing file
        file = self.cmps.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3a.json")
        self.assertFalse(file.get('title',"").startswith("My Magnum Opus"))
        self.assertEqual(file.get('@id'), "file_3")
        file = self.cmps.get_file_by_path("trial3/trial3b.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3b.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")
        self.assertEqual(file.get('@id'), "file_0")

        self.cmps.set_file_at(file, "trial3/trial3a.json")
        file = self.cmps.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3a.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")
        self.assertEqual(file.get('@id'), "file_0")
        with self.assertRaises(inmem.ObjectNotFound):
            self.cmps.get_file_by_path("trial3/trial3b.json")
        
        del file['@id']
        file['filepath'] = "goober/gurnson.json"
        with self.assertRaises(inmem.ObjectNotFound):
            self.cmps.set_file_at(file)
        file['filepath'] = "trial3"
        with self.assertRaises(inmem.CollectionRemovalDissallowed):
            self.cmps.set_file_at(file)

        file = self.cmps.get_file_by_path("trial2.json")
        with self.assertRaises(inmem.CollectionRemovalDissallowed):
            self.cmps.set_file_at(file, "trial3")
        
        del file['filepath']
        del file['@type']
        del file['@id']
        file['title'] = "Series 3"
        self.cmps.set_file_at(file, "trial3", as_coll=True)
        file = self.cmps.get_file_by_path("trial3")
        self.assertEqual(file['title'], "Series 3")
        self.assertEqual(self.cmps.get_ids_in_subcoll("trial3"), ["file_0"])

        # create a new directory
        fmdf = self.cmps._dir / "c:file_6.json"
        self.assertTrue(not fmdf.exists())
        self.assertTrue(not self.cmps.path_exists("trial4"))
        self.cmps.set_file_at({"title": "Series 4"}, "trial4", as_coll=True)
        file = self.cmps.get_file_by_path("trial4")
        self.assertEqual(file['filepath'], "trial4")
        self.assertEqual(file['title'], "Series 4")
        self.assertEqual(file['@id'], "file_6")
        self.assertIn(file['@id'], self.cmps.get_ids_in_subcoll(""))
        self.assertTrue(fmdf.is_file())

        # create a new file in subdirectory
        fmdf = self.cmps._dir / "f:pdr:f::4a.json"
        self.assertTrue(not fmdf.exists())
        self.assertTrue(not self.cmps.path_exists("trial4/trial4a.json"))
        self.cmps.set_file_at({"title": "Trial 4a"}, "trial4/trial4a.json", "pdr:f/4a")
        file = self.cmps.get_file_by_path("trial4/trial4a.json")
        self.assertEqual(file['filepath'], "trial4/trial4a.json")
        self.assertEqual(file['title'], "Trial 4a")
        self.assertEqual(file['@id'], "pdr:f/4a")
        self.assertNotIn(file['@id'], self.cmps.get_ids_in_subcoll(""))
        self.assertIn(file['@id'], self.cmps.get_ids_in_subcoll("trial4"))
        self.assertTrue(fmdf.is_file())
        
    def test_move(self):
        nerd = load_simple()
        self.cmps.load_file_components(nerd['components'])
        self.assertEqual(self.cmps.count, 4)
        self.assertEqual(self.cmps.get_ids_in_subcoll(""), "file_0 file_1 file_2".split())

        # rename a file
        self.assertTrue(self.cmps.path_exists("trial1.json"))
        self.assertTrue(not self.cmps.path_exists("trial1.json.hold"))
        self.assertIn("file_0", self.cmps.get_ids_in_subcoll(""))
        
        self.assertEqual(self.cmps.move("trial1.json", "trial1.json.hold"), "file_0")
        
        self.assertTrue(not self.cmps.path_exists("trial1.json"))
        self.assertTrue(self.cmps.exists("file_0"))
        self.assertIn("file_0", self.cmps.get_ids_in_subcoll(""))
        file = self.cmps.get_file_by_path("trial1.json.hold")
        self.assertTrue(file['@id'], "file_0")

        self.assertEqual(self.cmps.move("file_0", "trial1.json"), "file_0")

        self.assertTrue(self.cmps.path_exists("trial1.json"))
        self.assertTrue(not self.cmps.path_exists("trial1.json.hold"))

        # clobber another file
        self.assertTrue(self.cmps.path_exists("trial1.json"))
        self.assertTrue(self.cmps.path_exists("trial2.json"))
        self.assertIn("file_0", self.cmps.get_ids_in_subcoll(""))
        self.assertEqual(self.cmps.move("trial1.json", "trial2.json"), "file_0")
        self.assertTrue(not self.cmps.path_exists("trial1.json"))
        self.assertTrue(self.cmps.path_exists("trial2.json"))
        self.assertIn("file_0", self.cmps.get_ids_in_subcoll(""))
        file = self.cmps.get_file_by_path("trial2.json")
        self.assertTrue(file['@id'], "file_0")
        self.assertTrue(file['filepath'], "trial2.json")
        
        # fail to move a non-existent file
        self.assertTrue(not self.cmps.path_exists("goob"))
        self.assertTrue(not self.cmps.exists("goob"))
        with self.assertRaises(inmem.ObjectNotFound):
            self.cmps.move("goob", "gurn/goober")

        # move a file to a directory
        self.assertTrue(self.cmps.exists("file_0"))
        self.assertNotIn("file_0", self.cmps.get_ids_in_subcoll("trial3"))
        self.assertEqual(self.cmps.move("file_0", "trial3"), "file_0")
        self.assertTrue(self.cmps.exists("file_0"))
        self.assertIn("file_0", self.cmps.get_ids_in_subcoll("trial3"))
        file = self.cmps.get_file_by_id("file_0")
        self.assertEqual(file['filepath'], "trial3/trial2.json")

        # move a file from one directory to another
        self.assertTrue(not self.cmps.path_exists("trial4"))
        self.cmps.set_file_at({"title": "Series 4"}, "trial4", as_coll=True)
        self.assertTrue(self.cmps.path_is_collection("trial4"))

        self.assertTrue(not self.cmps.path_exists("trial4/trial4a.json"))
        self.assertEqual(self.cmps.move("trial3/trial3a.json", "trial4/trial4a.json"), "file_3")
        file = self.cmps.get_file_by_path("trial4/trial4a.json")
        self.assertEqual(file['@id'], "file_3")
        self.assertEqual(file['filepath'], "trial4/trial4a.json")


class TestFSBasedResource(test.TestCase):
    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.res = fsbased.FSBasedResource("pdr0:0001", self.outdir.name)
        
    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.res._dir.name, "pdr0:0001")
        self.assertEqual(self.res._resmdfile, self.res._dir / "res.json")
        self.assertTrue(self.res._dir.is_dir())
        self.assertTrue(self.res._resmdfile.is_file())
        self.assertEqual(read_json(str(self.res._resmdfile)), {"@id": "pdr0:0001"})
        
        self.assertFalse(self.res.deleted)
        self.assertIsNotNone(self.res.get_res_data())
        
        itms = self.res.authors
        self.assertTrue(isinstance(itms, ns.NERDAuthorList))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, self.res)
        itms = self.res.references
        self.assertTrue(isinstance(itms, ns.NERDRefList))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, self.res)
        itms = self.res.files
        self.assertTrue(isinstance(itms, ns.NERDFileComps))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, self.res)
        itms = self.res.nonfiles
        self.assertTrue(isinstance(itms, ns.NERDNonFileComps))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, self.res)

        self.assertEqual(self.res.get_data(), {'@id': "pdr0:0001"})

        self.res.delete()
        self.assertTrue(self.res.deleted)
        self.assertIsNone(self.res.get_data())
        self.assertIsNone(self.res.get_res_data())
        with self.assertRaises(fsbased.RecordDeleted):
            self.res.references

        self.res = fsbased.FSBasedResource("pdr0:0002", self.outdir.name, False)
        self.assertEqual(self.res._dir.name, "pdr0:0002")
        self.assertEqual(self.res._resmdfile, self.res._dir / "res.json")
        self.assertTrue(not self.res._dir.exists())
        with self.assertRaises(fsbased.RecordDeleted):
            self.res.authors

class TestFSBasedResourceStorage(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.fact = fsbased.FSBasedResourceStorage(self.outdir.name)
        
    def tearDown(self):
        self.outdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.fact._pfx, "nrd")
        self.assertEqual(self.fact._seqfile, "_seq.json")
        self.assertEqual(str(self.fact._dir.name), os.path.basename(self.outdir.name))
#        self.assertEqual(str(self.fact._seqp), os.path.join(self.outdir.name, "_seq.json"))
        self.assertIsNotNone(self.fact._log)

    def test_new_id(self):
        self.assertTrue(not self.fact._seqp.exists())
        self.assertEqual(self.fact._new_id(), "nrd:0001")
        self.assertTrue(self.fact._seqp.exists())
        self.assertEqual(read_json(str(self.fact._seqp)), 2)

        self.assertEqual(self.fact._new_id(), "nrd:0002")
        self.assertEqual(self.fact._new_id(), "nrd:0003")
        self.assertEqual(read_json(str(self.fact._seqp)), 4)

        self.fact._reserve_id("nrd:12344")
        self.assertEqual(self.fact._new_id(), "nrd:12345")
        self.assertEqual(read_json(str(self.fact._seqp)), 12346)

        self.fact._reserve_id("nrd:20000")
        self.assertEqual(self.fact._new_id(), "nrd:20001")
        self.assertEqual(read_json(str(self.fact._seqp)), 20002)
        
        self.fact._reserve_id("nrd:10000")
        self.assertEqual(self.fact._new_id(), "nrd:20002")
        self.assertEqual(read_json(str(self.fact._seqp)), 20003)
        
        self.fact._reserve_id("goober")
        self.assertEqual(self.fact._new_id(), "nrd:20003")
        self.assertEqual(read_json(str(self.fact._seqp)), 20004)

    def test_load_from(self):
        self.assertTrue(not self.fact.exists("pdr02p1s"))
        nerd = load_simple()
        self.fact.load_from(nerd)
        self.assertTrue(self.fact.exists("pdr02p1s"))

        res = self.fact.open("pdr02p1s")
        rec = res.get_data()
        self.assertEqual(rec['@id'], "ark:/88434/pdr02p1s")
        self.assertEqual(rec['doi'], "doi:10.18434/T4SW26")
        self.assertEqual(res.authors.count, 2)
        self.assertEqual(res.references.count, 1)
        self.assertEqual(res.nonfiles.count, 1)
        self.assertEqual(res.files.count, 4)
        
    def test_delete(self):
        self.assertTrue(not self.fact.exists("pdr02p1s"))
        nerd = load_simple()
        self.fact.load_from(nerd)
        self.assertTrue(self.fact.exists("pdr02p1s"))

        self.assertFalse(self.fact.delete("nobody"))
        self.assertTrue(self.fact.exists("pdr02p1s"))
        self.assertTrue(self.fact.delete("pdr02p1s"))
        self.assertTrue(not self.fact.exists("pdr02p1s"))

        


if __name__ == '__main__':
    test.main()
        
