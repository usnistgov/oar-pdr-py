# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging, random, math
import tempfile, shutil
import unittest as test

import multibag.testing.mkdata as mkdata

class TestFunctions(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_create_file(self):
        dest = os.path.join(self.tempdir, "datafile")
        self.assertTrue(not os.path.exists(dest))

        sz = mkdata.create_file(dest, 350)
        self.assertTrue(os.path.exists(dest))
        self.assertEqual(sz, os.stat(dest).st_size)
        self.assertEqual(sz, 350)
        i = 0
        with open(dest) as fd:
            for line in fd:
                if i < 3:
                    self.assertEqual(len(line), 100)
                    self.assertEqual(int(line.strip().split()[0]), i)
                else:
                    self.assertEqual(len(line), 50)
                i += 1
        self.assertEqual(i, 4)

        sz = mkdata.create_file(dest, 202)
        self.assertTrue(os.path.exists(dest))
        self.assertEqual(sz, os.stat(dest).st_size)
        self.assertEqual(sz, 202)
        with open(dest) as fd:
            self.assertEqual(len(fd.readlines()), 3)

        sz = mkdata.create_file(dest, 64)
        self.assertTrue(os.path.exists(dest))
        self.assertEqual(sz, os.stat(dest).st_size)
        self.assertEqual(sz, 64)
        with open(dest) as fd:
            self.assertEqual(len(fd.readlines()), 1)


class TestInventorySizeIterator(test.TestCase):

    def setUp(self):
        self.sizes = { 25: 2, 100: 1, 4: 4 }
        self.iter = mkdata.InventorySizeIterator(self.sizes)

    def test_type(self):
        self.assertEqual(self.iter.type, 'unkwn')
        self.iter = mkdata.InventorySizeIterator(self.sizes, type='goober')
        self.assertEqual(self.iter.type, 'goober')

    def test_target_props(self):
        self.assertIsNone(self.iter.target_totalsize)
        self.iter.target_totalsize = 3982
        self.assertEqual(self.iter.target_totalsize, 3982)
        with self.assertRaises(ValueError):
            self.iter.target_totalsize = 'goober'

        self.assertIsNone(self.iter.target_totalfiles)
        self.iter.target_totalfiles = 99
        self.assertEqual(self.iter.target_totalfiles, 99)
        self.assertEqual(self.iter.target_totalsize, 3982)
        with self.assertRaises(ValueError):
            self.iter.target_totalfiles = 4.5

        self.iter.target_totalfiles = None
        self.assertIsNone(self.iter.target_totalfiles)
        self.assertEqual(self.iter.target_totalsize, 3982)
        self.iter.target_totalsize = None
        self.assertIsNone(self.iter.target_totalsize)

    def test_iterate(self):
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 100, 25, 25, 4, 4, 4, 4 ])

    def test_totalsize(self):
        self.assertEqual(self.iter.totalsize, 166)

    def test_totalfiles(self):
        self.assertEqual(self.iter.totalfiles, 7)


class TestUniformSizeIterator(test.TestCase):

    def setUp(self):
        self.cfg = { 'totalsize': 361, 'totalfiles': 6 }
        self.iter = mkdata.UniformSizeIterator(**self.cfg)

    def test_target_props(self):
        self.assertEqual(self.iter.target_totalsize, 361)
        self.iter.target_totalsize = 3982
        self.assertEqual(self.iter.target_totalsize, 3982)
        with self.assertRaises(ValueError):
            self.iter.target_totalsize = 'goober'

        self.assertEqual(self.iter.target_totalfiles, 6)
        self.iter.target_totalfiles = 99
        self.assertEqual(self.iter.target_totalfiles, 99)
        self.assertEqual(self.iter.target_totalsize, 3982)
        with self.assertRaises(ValueError):
            self.iter.target_totalfiles = 4.5

        self.iter.target_totalfiles = None
        self.assertIsNone(self.iter.target_totalfiles)
        self.assertEqual(self.iter.target_totalsize, 3982)
        self.iter.target_totalsize = None
        self.assertIsNone(self.iter.target_totalsize)

    def test_not_ready(self):
        self.iter = mkdata.UniformSizeIterator()
        with self.assertRaises(RuntimeError):
            next(self.iter.iterate())
        with self.assertRaises(RuntimeError):
            self.iter.totalsize
        with self.assertRaises(RuntimeError):
            self.iter.totalfiles

        self.iter.target_totalsize = 250
        self.iter.target_totalfiles = 50
        next(self.iter.iterate())

    def test_iterate(self):
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 60, 60, 60, 60, 60, 61 ])

    def test_iterate_many(self):
        self.iter = mkdata.UniformSizeIterator(20, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 4, 4, 4, 4, 4 ])

        self.iter = mkdata.UniformSizeIterator(21, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 4, 4, 4, 4, 5 ])

        self.iter = mkdata.UniformSizeIterator(22, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 4, 4, 4, 5, 5 ])

        self.iter = mkdata.UniformSizeIterator(19, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 4, 4, 4, 4, 3 ])

        self.iter = mkdata.UniformSizeIterator(18, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 4, 4, 4, 3, 3 ])

        self.iter = mkdata.UniformSizeIterator(17, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 3, 3, 3, 4, 4 ])

        self.iter = mkdata.UniformSizeIterator(5, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 1, 1, 1, 1, 1 ])

        self.iter = mkdata.UniformSizeIterator(0, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 0, 0, 0, 0, 0 ])

        self.iter = mkdata.UniformSizeIterator(3, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 1, 1, 1, 0, 0 ])

        self.iter = mkdata.UniformSizeIterator(2, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 0, 0, 0, 1, 1 ])

        self.iter = mkdata.UniformSizeIterator(1, 5)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 0, 0, 0, 0, 1 ])

        self.iter = mkdata.UniformSizeIterator(5, 1)
        sizes = list(self.iter.iterate())
        self.assertEqual(sizes, [ 5 ])

        for i in range(5):
            sz = random.randrange(100)
            nf = random.randrange(1, 1000)
            tz = sz*nf + random.randint(-int(nf/2), int(nf/2))
            szl = list(mkdata.UniformSizeIterator(tz, nf).iterate())
            self.assertEqual(len(szl), nf)
            self.assertEqual(sum(szl), tz)
            self.assertTrue(all([math.fabs(e-sz)<=1 for e in szl]))

    def test_totalsize(self):
        self.assertEqual(self.iter.totalsize, 361)

    def test_totalfiles(self):
        self.assertEqual(self.iter.totalfiles, 6)


            

class TestDatasetMaker(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.dsdir = os.path.join(self.tempdir, "dataset")

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_mkfid(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        nms = [mkr._mkfid(100) for i in range(10)]
        for i in range(len(nms)):
            self.assertNotEqual(nms[0], nms[1])
            n = nms.pop(0)
            nms.append(n)

    def test_ensure_root(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))
        mkr.ensure_root()
        self.assertTrue(os.path.exists(self.dsdir))
        mkr.ensure_root()
        self.assertTrue(os.path.exists(self.dsdir))

    def test_mkfilename(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        mkr.ensure_root()
        files = []
        for i in range(100):
            p = mkr._mkfilename(lambda fn: "_{0}_".format(fn), '')
            with open(os.path.join(mkr.root, p), 'w') as fd:
                fd.write('\n')
                files.append(p)

        for f in files:
            self.assertTrue(os.path.exists(os.path.join(self.dsdir, f)),
                            "failed to create "+f)

    def test_create_file(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        f = mkr._create_file(82)
        self.assertTrue(os.path.exists(self.dsdir))
        fp = os.path.join(self.dsdir,f)
        self.assertTrue(os.path.exists(fp))
        self.assertEqual(os.stat(fp).st_size, 82)

        f = mkr._create_file(8200841, under='goob')
        self.assertTrue(f.startswith("goob"+os.sep),
                        "file not created under goob/")
        fp = os.path.join(self.dsdir,f)
        self.assertTrue(os.path.exists(fp))
        self.assertEqual(os.stat(fp).st_size, 8200841)

        f = mkr._create_file(411, under='goob')
        self.assertTrue(f.startswith("goob"+os.sep),
                        "file not created under goob/")
        fp = os.path.join(self.dsdir,f)
        self.assertTrue(os.path.exists(fp))
        self.assertEqual(os.stat(fp).st_size, 411)

    def test_create_dir(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        f = mkr._create_dir()
        self.assertTrue(os.path.exists(self.dsdir))
        fp = os.path.join(self.dsdir,f)
        self.assertTrue(os.path.exists(fp))
        self.assertTrue(os.path.isdir(fp))

        f = mkr._create_dir("furry/goob")
        self.assertTrue(os.path.exists(self.dsdir))
        self.assertTrue(f.startswith("furry"+os.sep+"goob"+os.sep),
                        "directory not created under furry/goob/")
        fp = os.path.join(self.dsdir,f)
        self.assertTrue(os.path.exists(fp))
        self.assertTrue(os.path.isdir(fp))

    def test_fill_with_files(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))
        
        n = mkr._fill_with_files("goob")
        self.assertEqual(n, (0, 0))
        self.assertTrue(not os.path.exists(self.dsdir))

        n = mkr._fill_with_files("goob", 103, 2)
        self.assertEqual(n, (0, 0))
        self.assertTrue(not os.path.exists(self.dsdir))

        n = mkr._fill_with_files("gurn", 103, 4, [75, 80])
        self.assertEqual(n, (75, 1))
        self.assertTrue(os.path.exists(self.dsdir))
        self.assertTrue(os.path.isdir(os.path.join(self.dsdir,'gurn')))
        files = os.listdir(os.path.join(self.dsdir,'gurn'))
        self.assertEqual(len(files), 1)
        self.assertEqual(os.stat(os.path.join(self.dsdir,'gurn',files[0])).st_size, 75)

        n = mkr._fill_with_files("furry"+os.sep+"goob", 1000, 3, [75, 80, 200])
        self.assertEqual(n, (355, 3))
        self.assertTrue(os.path.exists(self.dsdir))
        self.assertTrue(os.path.isdir(os.path.join(self.dsdir,'furry','goob')))
        files = os.listdir(os.path.join(self.dsdir,'furry','goob'))
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,'furry','goob',f)).st_size
             for f in files])
        self.assertEqual(sizes, [75, 80, 200])

    def test_fill_with_files_w_iter(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        iter = mkdata.UniformSizeIterator(totalsize=500, totalfiles=5).iterate()
        n = mkr._fill_with_files("goob", 3000, 4, iter)
        self.assertEqual(n, (400, 4))
        parent = os.path.join(self.dsdir,'goob')
        files = os.listdir(os.path.join(self.dsdir,'goob'))
        sizes = [os.stat(os.path.join(self.dsdir,'goob',f)).st_size
                 for f in files]
        self.assertTrue(all([s == 100 for s in sizes]),
                        "Wrong file sizes: "+str(sizes))
        self.assertEqual(len(files), 4)
            
    def test_distribute(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})

        files = [{'totalsize': 100, 'totalfiles': 4},
                 {'totalsize': 50, 'reps': 4, 'totalfiles': 1}, {}]
        dirs = [{'totalsize': 500}, {'totalfiles': 5}]

        mkr._distribute(2000, 19, files, dirs)
        for fs in files:
            self.assertIn('iter', fs)
            del fs['iter']
            del fs['type']
        self.assertEquals(files,
                          [
                              {'totalsize': 100, 'totalfiles': 4},
                              {'totalsize': 50, 'reps': 4, 'totalfiles': 1},
                              {'totalsize': 600, 'totalfiles': 3}
                          ])
        self.assertEquals(dirs,
                          [
                              {'totalsize': 500, 'totalfiles': 3},
                              {'totalsize': 600, 'totalfiles': 5}
                          ])

        # test for adding extra
        mkr._distribute(2200, 22, files, dirs)
        for fs in files:
            self.assertIn('iter', fs)
            del fs['iter']
            del fs['type']
        self.assertEquals(files,
                          [
                              {'totalsize': 100, 'totalfiles': 4},
                              {'totalsize': 50, 'reps': 4, 'totalfiles': 1},
                              {'totalsize': 600, 'totalfiles': 3},
                              {'totalsize': 200, 'totalfiles': 3 }
                          ])
        self.assertEquals(dirs,
                          [
                              {'totalsize': 500, 'totalfiles': 3},
                              {'totalsize': 600, 'totalfiles': 5}
                          ])


    def test_fill_dir_1(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        files = [{'totalsize': 100, 'totalfiles': 4},
                 {'totalsize': 50, 'reps': 4, 'totalfiles': 1}, {}]
        # dirs = [{'totalsize': 500}, {'totalfiles': 5}]

        mkr._fill_dir('', 1500, 14, files)
        self.assertTrue(os.path.exists(self.dsdir))

        fns = os.listdir(self.dsdir)
        self.assertEqual(len(fns), 14,
                         "Wrong number of files: expected 14; got "+str(fns))
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns])
        self.assertEqual(sizes, [ 25,  25,  25,  25, 50,  50,  50,  50,
                                  200, 200, 200, 200, 200, 200 ])
        self.assertEqual(sum(sizes), 1500)

    def test_fill_dir_2(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        files = [{'totalsize': 100, 'totalfiles': 4},
                 {'totalsize': 50, 'reps': 4, 'totalfiles': 1}, {}]
        dirs = [{'totalsize': 500}, {'totalfiles': 5}]

        mkr._fill_dir('', 2000, 19, files, dirs)
        self.assertTrue(os.path.exists(self.dsdir))

        fns = os.listdir(self.dsdir)
        self.assertEqual(len(fns), 13,
                         "Wrong number of file/dirs: expected 13; got "+str(fns))
        dirs = [f for f in fns if f.endswith('_d')]
        self.assertEqual(len(dirs), 2)
        self.assertTrue(all([os.path.isdir(os.path.join(self.dsdir,d))
                             for d in dirs]),
                        "Not all directories are really directories: "+str(dirs))
        
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns
             if not f.endswith('_d')])
        self.assertEqual(sizes, [ 25,  25,  25,  25, 50,  50,  50,  50,
                                  200, 200, 200 ])
        self.assertEqual(sum(sizes), 900)

        fns = [ [os.path.join(dirs[0], f)
                 for f in os.listdir(os.path.join(self.dsdir, dirs[0]))],
                [os.path.join(dirs[1], f)
                 for f in os.listdir(os.path.join(self.dsdir, dirs[1]))] ]
        if fns[0][0].endswith('_120'):
            fns = [fns[1], fns[0]]

        self.assertEqual(len(fns[0]), 3)
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns[0]])
        self.assertEqual(sizes, [166, 167, 167])
        
        self.assertEqual(len(fns[1]), 5)
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns[1]])
        self.assertEqual(sizes, [120, 120, 120, 120, 120])
        
    def test_fill_dir_3(self):
        mkr = mkdata.DatasetMaker(self.dsdir, {'totalfiles': 10, 'totalsize': 0})
        self.assertTrue(not os.path.exists(self.dsdir))

        files = [{'totalsize': 100, 'totalfiles': 4},
                 {'type': 'inventory', 'sizes': { 15: 2, 10: 3}},
                 {'totalsize': 50, 'reps': 4, 'totalfiles': 1},
                 {'type': 'inventory', 'sizes': { 22: 1, 18: 1}}]
        dirs = [{'totalsize': 500}, {'totalfiles': 5}]

        # pdb.set_trace()
        mkr._fill_dir('', 2000, 22, files, dirs)
        self.assertTrue(os.path.exists(self.dsdir))

        fns = os.listdir(self.dsdir)
        self.assertEqual(len(fns), 17,
                         "Wrong number of file/dirs: expected 17; got "+str(fns))
        dirs = [f for f in fns if f.endswith('_d')]
        self.assertEqual(len(dirs), 2)
        self.assertTrue(all([os.path.isdir(os.path.join(self.dsdir,d))
                             for d in dirs]),
                        "Not all directories are really directories: "+str(dirs))
        
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns
             if not f.endswith('_d')])
        self.assertEqual(sizes, [ 10, 10, 10, 15, 15, 18, 22, 25,  25,  25,  25, 
                                  50, 50, 50, 50 ])
        self.assertEqual(sum(sizes), 400)

        fns = [ [os.path.join(dirs[0], f)
                 for f in os.listdir(os.path.join(self.dsdir, dirs[0]))],
                [os.path.join(dirs[1], f)
                 for f in os.listdir(os.path.join(self.dsdir, dirs[1]))] ]
        if fns[0][0].endswith('_220'):
            fns = [fns[1], fns[0]]

        self.assertEqual(len(fns[0]), 2)
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns[0]])
        self.assertEqual(sizes, [250, 250])
        
        self.assertEqual(len(fns[1]), 5)
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns[1]])
        self.assertEqual(sizes, [220, 220, 220, 220, 220])
        
    def test_fill(self):
        mkr = mkdata.DatasetMaker(self.dsdir,
                                  { 'totalsize': 15, 'totalfiles': 3,
                                    'files': [{
                                        'totalsize': 10, 'totalfiles': 2
                                    }], 'dirs': [{
                                        'totalsize': 5, 'totalfiles': 1
                                    }]
                                  })
        self.assertTrue(not os.path.exists(self.dsdir))

        mkr.fill()
        self.assertTrue(os.path.exists(self.dsdir))

        fns = os.listdir(self.dsdir)
        self.assertEqual(len(fns), 3,
                         "Wrong number of file/dirs: expected 3; got "+str(fns))
        dirs = [f for f in fns if f.endswith('_d')]
        self.assertEqual(len(dirs), 1)
        self.assertTrue(os.path.isdir(os.path.join(self.dsdir,dirs[0])),
                        "Not a directoru: "+str(dirs))

        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns
             if not f.endswith('_d')])
        self.assertEqual(sizes, [ 5, 5 ])

        fns = os.listdir(os.path.join(self.dsdir, dirs[0]))
        self.assertEqual(len(fns), 1)
        self.assertEqual(os.stat(os.path.join(self.dsdir, dirs[0],fns[0])).st_size,
                         5)

    def test_mkdataset(self):
        self.assertTrue(not os.path.exists(self.dsdir))

        mkdata.mkdataset(self.dsdir, 130)
        
        fns = os.listdir(self.dsdir)
        self.assertEqual(len(fns), 10,
                         "Wrong number of files: expected 10; got "+str(fns))
        dirs = [f for f in fns if f.endswith('_d')]
        self.assertEqual(len(dirs), 0)
        
        sizes = sorted(
            [os.stat(os.path.join(self.dsdir,f)).st_size for f in fns
             if not f.endswith('_d')])
        self.assertEqual(sum(sizes), 130)
        self.assertTrue(all([sz == 13 for sz in sizes]))
        
        

        
if __name__ == '__main__':
    test.main()
