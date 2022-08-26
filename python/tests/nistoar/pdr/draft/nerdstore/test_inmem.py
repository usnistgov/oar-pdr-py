import os, json, pdb, logging
from pathlib import Path
import unittest as test

import nistoar.pdr.draft.nerdstore as ns
from nistoar.pdr.draft.nerdstore import inmem
from nistoar.pdr.utils import read_json

testdir = Path(__file__).parents[2] / 'preserve' / 'data' / 'simplesip'
sipnerd = testdir / '_nerdm.json'

def load_simple():
    return read_json(sipnerd)

class TestInMemoryResource(test.TestCase):

    def setUp(self):
        pass

    def test_createempty(self):
        res = inmem.InMemoryResource("pdr0:0001")
        data = res.data()
        self.assertIsNotNone(res.data())
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertIsNone(data.get('title'))

        self.assertFalse(res.deleted)
        self.assertIsNotNone(res.res_data())

        itms = res.authors
        self.assertTrue(isinstance(itms, ns.NERDAuthorList))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, res)
        itms = res.references
        self.assertTrue(isinstance(itms, ns.NERDRefList))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, res)
        itms = res.files
        self.assertTrue(isinstance(itms, ns.NERDFileComps))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, res)
        itms = res.nonfiles
        self.assertTrue(isinstance(itms, ns.NERDNonFileComps))
        self.assertEqual(itms.count, 0)
        self.assertIs(itms._res, res)

        self.assertEqual(data, {'@id': "pdr0:0001"})

        res.delete()
        self.assertTrue(res.deleted)
        self.assertIsNone(res.data())
        self.assertIsNone(res.res_data())

    def test_load_data(self):
        nerd = load_simple()
        res = inmem.InMemoryResource("pdr0:0001", nerd)
        self.assertEqual(res.id, "pdr0:0001")
        data = res.res_data()
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertEqual(data.get('title'), nerd['title'])
        self.assertEqual(data.get('contactPoint'), nerd['contactPoint'])
        
        itms = res.authors
        self.assertEqual(itms.count, 2)
        self.assertEqual(itms[0].get('familyName'), "Levine")

        itms = res.references
        self.assertEqual(itms.count, 1)
        self.assertEqual(itms[0].get('refType'), "IsReferencedBy")

        itms = res.nonfiles
        self.assertEqual(itms.count, 1)
        self.assertEqual(itms[0].get('accessURL'), nerd['components'][2]['accessURL'])

        itms = res.files
        self.assertEqual(itms.count, 4)
        self.assertEqual(itms['file_0'].get('filepath'), nerd['components'][0]['filepath'])

    def test_replace_res_data(self):
        res = inmem.InMemoryResource("pdr0:0001")
        data = res.data()
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertIsNone(data.get('title'))
        self.assertEqual(res.authors.count, 0)
        self.assertEqual(res.references.count, 0)
        self.assertEqual(res.nonfiles.count, 0)
        self.assertEqual(res.files.count, 0)
        self.assertIsNone(data.get('color'))
        self.assertIsNone(data.get('authors'))
        self.assertIsNone(data.get('components'))

        md = {
            '@id':   "Whahoo!",
            'title': "The Replacements",
            'color': "green",
            'contactPoint': [ { "comment": "this is not real contact info" } ],
            'authors': [ { "fn": "Gurn Cranston" } ]
        }
        res.replace_res_data(md)
        data = res.data()
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertEqual(data.get('title'), "The Replacements")
        self.assertEqual(data.get('contactPoint'), [{"comment": "this is not real contact info"}])
        self.assertEqual(data.get('color'), "green")
        self.assertIsNone(data.get('authors'))
        self.assertIsNone(data.get('references'))
        self.assertIsNone(data.get('components'))
        
        nerd = load_simple()
        res.replace_res_data(nerd)
        data = res.data()
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertTrue(data.get('title').startswith('OptSortSph: '))
        self.assertEqual(data.get('contactPoint').get("fn"), "Zachary Levine")
        self.assertEqual(data.get('doi'), "doi:10.18434/T4SW26")
        self.assertIsNone(data.get('contactPoint').get("comment"))
        self.assertIsNone(data.get('color'))
        self.assertIsNone(data.get('authors'))
        self.assertIsNone(data.get('references'))
        self.assertIsNone(data.get('components'))
        
        res.replace_res_data(md)
        data = res.data()
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertEqual(data.get('title'), "The Replacements")
        self.assertEqual(data.get('contactPoint'), [{"comment": "this is not real contact info"}])
        self.assertEqual(data.get('color'), "green")
        self.assertIsNone(data.get('doi'))
        self.assertIsNone(data.get('authors'))
        self.assertIsNone(data.get('references'))
        self.assertIsNone(data.get('components'))
        
        res = inmem.InMemoryResource("pdr0:0001", nerd)
        res.replace_res_data(md)
#        data = res.data()
        self.assertEqual(res.id, "pdr0:0001")
        self.assertEqual(data.get('@id'), "pdr0:0001")
        self.assertEqual(data.get('title'), "The Replacements")
        self.assertEqual(data.get('contactPoint'), [{"comment": "this is not real contact info"}])
        self.assertEqual(data.get('color'), "green")
        self.assertIsNone(data.get('doi'))
        self.assertEqual(res.authors.count, 2)
        self.assertEqual(res.references.count, 1)
        self.assertEqual(res.nonfiles.count, 1)
        self.assertEqual(res.files.count, 4)

#    def test_replace_all_data(self):
        
class TestInMemoryFileComps(test.TestCase):

    def test_load_from(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"))
        self.assertEqual(files.count, 0)
        files._load_from(nerd['components'])

        self.assertEqual(files.count, 4)
        self.assertEqual(len(files._files), 4)
        self.assertEqual(len(files._children), 3)
        self.assertIn("trial3", files._children)
        self.assertNotIn("trial3/trial3a.json", files._children)

        coll = files._files[list(files._children.values())[-1]]
        self.assertTrue(coll.get('@id').startswith("coll_"))
        self.assertIn('_children', coll)
        self.assertEqual(list(coll['_children'].keys()), ["trial3a.json"])
        self.assertEqual(files._ididx, 4)

        files.empty()
        self.assertEqual(files.count, 0)
        
    def test_iter_files(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)

        paths = [f['filepath'] for f in files.iter_files()]
        self.assertEqual(len(paths), 4)

        fcmps = files.data()
        self.assertTrue(isinstance(fcmps, list))
        self.assertEqual([f['filepath'] for f in fcmps], paths)
        
    def test_ids(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)

        ids = files.ids
        self.assertIn("file_0", ids)
        self.assertIn("file_1", ids)
        self.assertIn("coll_2", ids)
        self.assertIn("file_3", ids)
        self.assertEqual(len(ids), 4)

    def test_get_file(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)

        # by id
        f = files.get_file_by_id("file_1")
        self.assertEqual(f['@id'], "file_1")
        self.assertEqual(f['filepath'], "trial2.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)

        f = files.get_file_by_id("coll_2")
        self.assertTrue(files.is_collection(f))
        self.assertEqual(f['@id'], "coll_2")
        self.assertEqual(f['filepath'], "trial3")
        self.assertNotIn("downloadURL", f)
        self.assertNotIn("_children", f)
        self.assertTrue(isinstance(f['has_member'], list))
        self.assertTrue(len(f['has_member']), 1)
        self.assertEqual(f['has_member'][0], {"@id": "file_3", "name": "trial3a.json"})

        # by path
        f = files.get_file_by_path("trial1.json")
        self.assertEqual(f['@id'], "file_0")
        self.assertEqual(f['filepath'], "trial1.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)
        
        f = files.get_file_by_path("trial3")
        self.assertTrue(files.is_collection(f))
        self.assertEqual(f['@id'], "coll_2")
        self.assertEqual(f['filepath'], "trial3")
        self.assertNotIn("downloadURL", f)
        self.assertNotIn("_children", f)
        self.assertTrue(isinstance(f['has_member'], list))
        self.assertTrue(len(f['has_member']), 1)
        self.assertEqual(f['has_member'][0], {"@id": "file_3", "name": "trial3a.json"})
        
        f = files.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(f['@id'], "file_3")
        self.assertEqual(f['filepath'], "trial3/trial3a.json")
        self.assertIn(f['filepath'], f['downloadURL'])
        self.assertNotIn("has_member", f)

    def test_get_ids_in_subcoll(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)

        self.assertEqual(files.get_ids_in_subcoll("trial3"), ["file_3"])
        ids = files.get_ids_in_subcoll("")
        self.assertIn("file_0", ids)
        self.assertIn("file_1", ids)
        self.assertIn("coll_2", ids)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, "file_0 file_1 coll_2".split())

    def test_get_subcoll_members(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)

        members = list(files.get_subcoll_members("trial3"))
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]['filepath'], "trial3/trial3a.json")

        paths = [f['filepath'] for f in files.get_subcoll_members("")]
        self.assertIn("trial1.json", paths)
        self.assertIn("trial2.json", paths)
        self.assertIn("trial3", paths)

    def test_set_order_in_subcoll(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)
        self.assertEqual(files.get_ids_in_subcoll(""), "file_0 file_1 coll_2".split())

        files.set_order_in_subcoll("", "coll_2 file_1 file_0".split())
        self.assertEqual(files.get_ids_in_subcoll(""), "coll_2 file_1 file_0".split())

        files.set_order_in_subcoll("", "file_1 file_0".split())
        self.assertEqual(files.get_ids_in_subcoll(""), "file_1 file_0 coll_2".split())

        files.set_order_in_subcoll("trial3", [])
        self.assertEqual(files.get_ids_in_subcoll("trial3"), ["file_3"])

        files.set_order_in_subcoll("trial3", ["goob"])
        self.assertEqual(files.get_ids_in_subcoll("trial3"), ["file_3"])

        files.set_order_in_subcoll("trial3", ["file_3"])
        self.assertEqual(files.get_ids_in_subcoll("trial3"), ["file_3"])

        with self.assertRaises(inmem.ObjectNotFound):
            files.set_order_in_subcoll("goob", ["file_3"])

    def test_set_file_at(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)
        self.assertEqual(files.get_ids_in_subcoll(""), "file_0 file_1 coll_2".split())

        file = files.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertNotEqual(file.get('title'), "My Magnum Opus")
        file['title'] = "My Magnum Opus"
        files.set_file_at(file)

        file = files.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus")
        file = files.get_file_by_path("trial1.json")
        self.assertEqual(file.get('@id'), "file_0")
        self.assertEqual(file.get('title'), "My Magnum Opus")
        self.assertEqual(files.get_ids_in_subcoll(""), "file_0 file_1 coll_2".split())

        file['title'] = "My Magnum Opus, redux"
        del file['@id']
        files.set_file_at(file, "trial1.json")

        file = files.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")

        # move the file
        files.set_file_at(file, "trial3/trial3b.json", "file_0")
        with self.assertRaises(inmem.ObjectNotFound):
            files.get_file_by_path("trial1.json")
        file = files.get_file_by_id("file_0")
        self.assertEqual(file.get('filepath'), "trial3/trial3b.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")

        # create a new file
        del file['@id']
        file['title'] = "My Magnum Opus, reloaded"
        files.set_file_at(file, "trial1.json")
        file = files.get_file_by_path("trial1.json")
        self.assertEqual(file.get('filepath'), "trial1.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, reloaded")
        self.assertEqual(file.get('@id'), "file_4")

        # move a file onto an existing file
        file = files.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3a.json")
        self.assertFalse(file.get('title',"").startswith("My Magnum Opus"))
        self.assertEqual(file.get('@id'), "file_3")
        file = files.get_file_by_path("trial3/trial3b.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3b.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")
        self.assertEqual(file.get('@id'), "file_0")

        files.set_file_at(file, "trial3/trial3a.json")
        file = files.get_file_by_path("trial3/trial3a.json")
        self.assertEqual(file.get('filepath'), "trial3/trial3a.json")
        self.assertEqual(file.get('title'), "My Magnum Opus, redux")
        self.assertEqual(file.get('@id'), "file_0")
        with self.assertRaises(inmem.ObjectNotFound):
            files.get_file_by_path("trial3/trial3b.json")
        
        del file['@id']
        file['filepath'] = "goober/gurnson.json"
        with self.assertRaises(inmem.ObjectNotFound):
            files.set_file_at(file)
        file['filepath'] = "trial3"
        with self.assertRaises(inmem.CollectionRemovalDissallowed):
            files.set_file_at(file)

        file = files.get_file_by_path("trial2.json")
        with self.assertRaises(inmem.CollectionRemovalDissallowed):
            files.set_file_at(file, "trial3")
        
        del file['filepath']
        del file['@type']
        del file['@id']
        file['title'] = "Series 3"
        files.set_file_at(file, "trial3", as_coll=True)
        file = files.get_file_by_path("trial3")
        self.assertEqual(file['title'], "Series 3")
        self.assertEqual(files.get_ids_in_subcoll("trial3"), ["file_0"])

        # create a new directory
        self.assertTrue(not files.path_exists("trial4"))
        files.set_file_at({"title": "Series 4"}, "trial4", as_coll=True)
        file = files.get_file_by_path("trial4")
        self.assertEqual(file['filepath'], "trial4")
        self.assertEqual(file['title'], "Series 4")
        self.assertEqual(file['@id'], "coll_6")
        self.assertIn(file['@id'], files.get_ids_in_subcoll(""))

        # create a new file in subdirectory
        self.assertTrue(not files.path_exists("trial4/trial4a.json"))
        files.set_file_at({"title": "Trial 4a"}, "trial4/trial4a.json", "pdr:f/4a")
        file = files.get_file_by_path("trial4/trial4a.json")
        self.assertEqual(file['filepath'], "trial4/trial4a.json")
        self.assertEqual(file['title'], "Trial 4a")
        self.assertEqual(file['@id'], "pdr:f/4a")
        self.assertNotIn(file['@id'], files.get_ids_in_subcoll(""))
        self.assertIn(file['@id'], files.get_ids_in_subcoll("trial4"))
        
        

    def test_move(self):
        nerd = load_simple()
        files = inmem.InMemoryFileComps(inmem.InMemoryResource("goob"), nerd['components'])
        self.assertEqual(files.count, 4)
        self.assertEqual(files.get_ids_in_subcoll(""), "file_0 file_1 coll_2".split())

        # rename a file
        self.assertTrue(files.path_exists("trial1.json"))
        self.assertTrue(not files.path_exists("trial1.json.hold"))
        self.assertIn("file_0", files.get_ids_in_subcoll(""))
        
        self.assertEqual(files.move("trial1.json", "trial1.json.hold"), "file_0")
        
        self.assertTrue(not files.path_exists("trial1.json"))
        self.assertTrue(files.exists("file_0"))
        self.assertIn("file_0", files.get_ids_in_subcoll(""))
        file = files.get_file_by_path("trial1.json.hold")
        self.assertTrue(file['@id'], "file_0")

        self.assertEqual(files.move("file_0", "trial1.json"), "file_0")

        self.assertTrue(files.path_exists("trial1.json"))
        self.assertTrue(not files.path_exists("trial1.json.hold"))

        # clobber another file
        self.assertTrue(files.path_exists("trial1.json"))
        self.assertTrue(files.path_exists("trial2.json"))
        self.assertIn("file_0", files.get_ids_in_subcoll(""))
        self.assertEqual(files.move("trial1.json", "trial2.json"), "file_0")
        self.assertTrue(not files.path_exists("trial1.json"))
        self.assertTrue(files.path_exists("trial2.json"))
        self.assertIn("file_0", files.get_ids_in_subcoll(""))
        file = files.get_file_by_path("trial2.json")
        self.assertTrue(file['@id'], "file_0")
        self.assertTrue(file['filepath'], "trial2.json")
        
        # fail to move a non-existent file
        self.assertTrue(not files.path_exists("goob"))
        self.assertTrue(not files.exists("goob"))
        with self.assertRaises(inmem.ObjectNotFound):
            files.move("goob", "gurn/goober")

        # move a file to a directory
        self.assertTrue(files.exists("file_0"))
        self.assertNotIn("file_0", files.get_ids_in_subcoll("trial3"))
        self.assertEqual(files.move("file_0", "trial3"), "file_0")
        self.assertTrue(files.exists("file_0"))
        self.assertIn("file_0", files.get_ids_in_subcoll("trial3"))
        file = files.get_file_by_id("file_0")
        self.assertEqual(file['filepath'], "trial3/trial2.json")

        # move a file from one directory to another
        self.assertTrue(not files.path_exists("trial4"))
        files.set_file_at({"title": "Series 4"}, "trial4", as_coll=True)
        self.assertTrue(files.path_is_collection("trial4"))

        self.assertTrue(not files.path_exists("trial4/trial4a.json"))
        self.assertEqual(files.move("trial3/trial3a.json", "trial4/trial4a.json"), "file_3")
        file = files.get_file_by_path("trial4/trial4a.json")
        self.assertEqual(file['@id'], "file_3")
        self.assertEqual(file['filepath'], "trial4/trial4a.json")



        
        
                         
if __name__ == '__main__':
    test.main()
        

        

        
