import os, json, pdb, tempfile, logging, re
from copy import deepcopy
from pathlib import Path
import unittest as test
from unittest.mock import Mock, patch

import nistoar.midas.dap.nerdstore as ns
from nistoar.midas.dap.nerdstore import fmfs, inmem
from nistoar.pdr.utils import read_json, write_json
from nistoar.midas.dap.fm import FileManager

testdir = Path(__file__).parents[3] / 'pdr' / 'preserve' / 'data' / 'simplesip'
sipnerd = testdir / '_nerdm.json'
daptestdir = Path(__file__).parents[1] / 'data' 

def load_simple():
    return read_json(sipnerd)

def read_scan(id=None):
    return read_json(daptestdir/"scan-report.json")

def read_scan_reply(id=None):
    return read_json(daptestdir/"scan-req-ack.json")

tmpdir = None
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    global tmpdir
    tmpdir = tempfile.TemporaryDirectory(prefix="_test_fmfs.")
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_fmfs.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    global loghdlr
    global tmpdir
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestFMFSFileComps(test.TestCase):

    def setUp(self):
        ack = read_scan_reply()
        self.scanid = ack['scan_id']
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=tmpdir.name)
        self.fm = None
        with patch('nistoar.midas.dap.fm.FileManager') as mock:
            self.fm = mock.return_value
            self.fm.post_scan_files.return_value = ack
            self.fm.get_scan_files.return_value = read_scan()
        self.cmps = fmfs.FMFSFileComps(inmem.InMemoryResource("pdr0:0001"), self.outdir.name)
        
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

        self.assertEqual(self.cmps.fm_summary['file_count'], -1)
        self.assertEqual(self.cmps.fm_summary['folder_count'], -1)
        self.assertEqual(self.cmps.fm_summary['usage'], -1)
        self.assertFalse(self.cmps.fm_summary['syncing'])

    def test_get_file_scan(self):
        self.cmps._fmcli = self.fm
        self.assertIsNone(self.cmps.last_scan_id)
        self.cmps.last_scan_id = self.scanid
        scan = self.cmps._get_file_scan()
        self.fm.scan_files.assert_not_called()
        self.assertEqual(scan['scan_id'], self.scanid)
        self.assertIn('contents', scan)
        self.assertEqual(len(scan['contents']), 8)

        self.assertTrue(self.cmps._fmsumf.is_file())
        summ = read_json(self.cmps._fmsumf)
        self.assertIsNotNone(summ['last_scan_id'])

    def test_scan_files(self):
        self.cmps._fmcli = self.fm
        self.assertIsNone(self.cmps.last_scan_id)
        scan = self.cmps._scan_files()
        self.assertEqual(self.cmps.last_scan_id, self.scanid)
        self.fm.delete_scan_files.assert_not_called()
        self.fm.post_scan_files.assert_called()
        self.assertEqual(scan['scan_id'], self.scanid)
        self.assertIn('contents', scan)
        self.assertEqual(len(scan['contents']), 8)

        self.assertTrue(self.cmps._fmsumf.is_file())
        summ = read_json(self.cmps._fmsumf)
        self.assertEqual(summ.get('last_scan_id'), self.scanid)

    def test_update_files_from_scan(self):
        self.cmps._fmcli = self.fm
        self.assertEqual(self.cmps.count, 0)
        scan = read_scan()
        stat = self.cmps._update_files_from_scan(scan)
        self.assertEqual(stat['file_count'], 7)
        self.assertEqual(stat['folder_count'], 2)
        self.assertEqual(stat['usage'], 4997166)
        self.assertFalse(stat['syncing'])
        self.assertEqual(self.cmps.count, 9)
        self.assertTrue(self.cmps.path_exists("analysis"))
        self.assertTrue(self.cmps.path_exists("ngc7793-cont.fits"))
        self.assertTrue(self.cmps.path_exists("previews"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-cont.gif"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-HIm1.gif"))

        self.assertTrue(self.cmps._fmsumf.is_file())

        scan = read_scan()
        stat = self.cmps._update_files_from_scan(scan)
        self.assertEqual(stat['file_count'], 7)
        self.assertEqual(stat['folder_count'], 2)
        self.assertEqual(stat['usage'], 4997166)
        self.assertFalse(stat['syncing'])
        self.assertEqual(self.cmps.count, 9)
        self.assertTrue(self.cmps.path_exists("analysis"))
        self.assertTrue(self.cmps.path_exists("ngc7793-cont.fits"))
        self.assertTrue(self.cmps.path_exists("previews"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-cont.gif"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-HIm1.gif"))

        # move a file within the hierarchy
        fmd = self.cmps.get_file_by_path("previews/ngc7793-HIm1.gif")
        fmd['title'] = "The End"
        self.cmps.set_file_at(fmd)
        scan = read_scan()
        scan['contents'][-1]['path'] = re.sub(r'/previews/', '/', scan['contents'][-1]['path'])
        id = scan['contents'][-1]['fileid']
        stat = self.cmps._update_files_from_scan(scan)
        self.assertEqual(stat['file_count'], 7)
        self.assertEqual(stat['folder_count'], 2)
        self.assertEqual(stat['usage'], 4997166)
        self.assertFalse(stat['syncing'])
        self.assertEqual(self.cmps.count, 9)
        self.assertTrue(self.cmps.path_exists("analysis"))
        self.assertTrue(self.cmps.path_exists("ngc7793-cont.fits"))
        self.assertTrue(self.cmps.path_exists("previews"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-cont.gif"))
        self.assertTrue(self.cmps.path_exists("ngc7793-HIm1.gif"))
        self.assertTrue(not self.cmps.path_exists("previews/ngc7793-HIm1.gif"))
        fmd = self.cmps.get_file_by_path("ngc7793-HIm1.gif")
        self.assertEqual(fmd['@id'], id)
        self.assertEqual(fmd['title'], "The End")

        # remove a file
        scan = read_scan()
        m1 = scan['contents'].pop(-1)
        stat = self.cmps._update_files_from_scan(scan)
        self.assertEqual(stat['file_count'], 6)
        self.assertEqual(stat['folder_count'], 2)
        self.assertEqual(stat['usage'], 4992391)
        self.assertFalse(stat['syncing'])
        self.assertEqual(self.cmps.count, 8)
        self.assertTrue(self.cmps.path_exists("analysis"))
        self.assertTrue(self.cmps.path_exists("ngc7793-cont.fits"))
        self.assertTrue(self.cmps.path_exists("previews"))
        self.assertTrue(self.cmps.path_exists("previews/ngc7793-cont.gif"))
        self.assertTrue(not self.cmps.path_exists("ngc7793-HIm1.gif"))
        self.assertTrue(not self.cmps.path_exists("previews/ngc7793-HIm1.gif"))
        
        scan = read_scan()
        scan['contents'][-2]['path'] = "/goob"+scan['contents'][-2]['path']
        del scan['contents'][-1]['path']
        with self.assertRaises(fmfs.RemoteStorageException):
            self.cmps._update_files_from_scan(scan)
        

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

    def test_update_metadataa_not_on_ctor(self):
        self.cmps = fmfs.FMFSFileComps(inmem.InMemoryResource("pdr0:0001"), self.outdir.name, self.fm)
        self.assertTrue(self.cmps._fmsumf.is_file())
        summ = read_json(self.cmps._fmsumf)
        self.assertEqual(summ['file_count'], -1)
        self.assertIsNone(summ['last_scan_id'])
        self.assertEqual(self.cmps.count, 0)

        self.cmps.update_metadata()
        self.assertEqual(self.cmps.count, 9)
        self.assertEqual(self.cmps.fm_summary['file_count'], 7)
        self.assertEqual(self.cmps.fm_summary['folder_count'], 2)
        self.assertEqual(self.cmps.fm_summary['usage'], 4997166)
        self.assertFalse(self.cmps.fm_summary['syncing'])

        # simulate constructing while (slow) scanning is still in progress
        summ = deepcopy(fmfs._NO_FM_SUMMARY)
        ack = read_scan_reply()
        summ['last_scan_id'] = ack['scan_id']
        summ['last_scan_is_complete'] = False
        self.cmps.empty()
        self.cmps._fmsumf.unlink()
        write_json(summ, self.cmps._fmsumf)
        self.assertEqual(self.cmps.count, 0)
        self.fm.get_scan_files.return_value = read_scan()

        self.cmps = fmfs.FMFSFileComps(inmem.InMemoryResource("pdr0:0001"), self.outdir.name, self.fm)
        self.assertEqual(self.cmps.count, 0)

        self.cmps.update_metadata()
        self.assertEqual(self.cmps.count, 9)
        self.assertEqual(self.cmps.fm_summary['file_count'], 7)
        self.assertEqual(self.cmps.fm_summary['folder_count'], 2)
        self.assertEqual(self.cmps.fm_summary['usage'], 4997166)
        self.assertFalse(self.cmps.fm_summary['syncing'])


class TestFMFSResource(test.TestCase):
    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.res = fmfs.FMFSResource("pdr0:0001", self.outdir.name)
        
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
        with self.assertRaises(fmfs.RecordDeleted):
            self.res.references

        self.res = fmfs.FMFSResource("pdr0:0002", self.outdir.name, create=False)
        self.assertEqual(self.res._dir.name, "pdr0:0002")
        self.assertEqual(self.res._resmdfile, self.res._dir / "res.json")
        self.assertTrue(not self.res._dir.exists())
        with self.assertRaises(fmfs.RecordDeleted):
            self.res.authors

    def test_replace_res_data(self):
        nerd = load_simple()
        self.res.replace_res_data(nerd)
        resmd = self.res.get_res_data()
        self.assertEqual(resmd['title'], nerd.get('title'))
        self.assertEqual(resmd['description'], nerd.get('description'))
        self.assertEqual(resmd['contactPoint'], nerd.get('contactPoint'))
        self.assertNotIn('authors', resmd)
        self.assertNotIn('references', resmd)
        self.assertNotIn('components', resmd)
        

class TestFMFSResourceStorage(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_nerdstore.", dir=".")
        self.fact = fmfs.FMFSResourceStorage(self.outdir.name)
        
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
        
