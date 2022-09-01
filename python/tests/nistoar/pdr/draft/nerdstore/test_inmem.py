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
        self.assertEqual(itms.get(0).get('familyName'), "Levine")

        itms = res.references
        self.assertEqual(itms.count, 1)
        self.assertEqual(itms.get(0).get('refType'), "IsReferencedBy")

        itms = res.nonfiles
        self.assertEqual(itms.count, 1)
        self.assertEqual(itms.get(0).get('accessURL'), nerd['components'][2]['accessURL'])

        itms = res.files
        self.assertEqual(itms.count, 4)
        self.assertEqual(itms.get('file_0').get('filepath'), nerd['components'][0]['filepath'])

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

class TestInMemoryAuthorList(test.TestCase):

    def setUp(self):
        nerd = load_simple()
        self.auths = inmem.InMemoryAuthorList(nerd, nerd['authors'])

    def test_ctor(self):
        self.assertEqual(self.auths._pfx, "auth")
        self.assertEqual(self.auths._order, "auth_0 auth_1".split())
        self.assertEqual(self.auths._data['auth_0']['familyName'], "Levine")
        self.assertEqual(self.auths._data['auth_1']['familyName'], "Curry")
        self.assertEqual(self.auths._ididx, 2)

        self.assertEqual(self.auths.ids, "auth_0 auth_1".split())
        self.assertEqual(self.auths.count, 2)

    def test_contains(self):
        self.assertIn("auth_1", self.auths)
        self.assertIn("auth_0", self.auths)
        self.assertNotIn("auth_2", self.auths)

    def test_getsetpop(self):
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
        

class TestInMemoryRefList(test.TestCase):

    def setUp(self):
        nerd = load_simple()
        self.refs = inmem.InMemoryRefList(nerd, nerd['references'])

    def test_ctor(self):
        self.assertEqual(self.refs._pfx, "ref")
        self.assertEqual(self.refs._order, "ref_0".split())
        self.assertEqual(self.refs._data['ref_0']['refType'], "IsReferencedBy")
        self.assertEqual(self.refs._ididx, 1)

        self.assertEqual(self.refs.ids, "ref_0".split())
        self.assertEqual(self.refs.count, 1)

    def test_contains(self):
        self.assertIn("ref_0", self.refs)
        self.assertNotIn("ref_2", self.refs)


    def test_getsetpop(self):
        # test access by id or position
        ref = self.refs.get("ref_0")
        self.assertEqual(ref['refType'], "IsReferencedBy")

        # add a reference
        ref['refType'] = "IsSupplementTo"
        self.refs.append(ref)
        ref = self.refs.get(-1)
        self.assertEqual(ref['refType'], "IsSupplementTo")
        self.assertEqual(ref['@id'], "ref_1")
        self.assertEqual(self.refs.get(0)['refType'], "IsReferencedBy")


class TestInMemoryNonFileList(test.TestCase):

    def setUp(self):
        nerd = load_simple()
        self.cmps = inmem.InMemoryNonFileComps(inmem.InMemoryResource("goob"))
        self.cmps._load_data(nerd['components'])

    def test_ctor(self):
        self.assertEqual(self.cmps._pfx, "cmp")
        self.assertEqual(self.cmps._order, "cmp_0".split())
        self.assertEqual(self.cmps._data['cmp_0']['mediaType'], "application/zip")
        self.assertIn("accessURL", self.cmps._data['cmp_0'])
        self.assertEqual(self.cmps._ididx, 1)

        self.assertEqual(self.cmps.ids, "cmp_0".split())
        self.assertEqual(self.cmps.count, 1)

    def test_contains(self):
        self.assertIn("cmp_0", self.cmps)
        self.assertNotIn("cmp_2", self.cmps)


    def test_getsetpop(self):
        # test access by id or position
        cmp = self.cmps.get("cmp_0")
        self.assertEqual(cmp['mediaType'], "application/zip")

        # add a cmperence
        cmp['mediaType'] = "text/plain"
        self.cmps.append(cmp)
        cmp = self.cmps.get(-1)
        self.assertEqual(cmp['mediaType'], "text/plain")
        self.assertEqual(cmp['@id'], "cmp_1")
        self.assertEqual(self.cmps.get(0)['mediaType'], "application/zip")



if __name__ == '__main__':
    test.main()
        

        

        
