import os, sys, pdb, json, subprocess, threading, time, logging
import unittest as test
from pathlib import Path
from multiprocessing import Process, TimeoutError

from nistoar.testing import *
import nistoar.pdr.utils.io as utils
from nistoar.pdr.exceptions import StateException

testdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
testdatadir = os.path.join(testdir, 'data')
testdatadir3 = os.path.join(testdir, 'preserve', 'data')
testdatadir2 = os.path.join(testdatadir3, 'simplesip')

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_utils.log"))
    loghdlr.setLevel(logging.INFO)
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.flush()
        loghdlr.close()
        loghdlr = None
    rmtmpdir()

class TestLockedFile(test.TestCase):

    class OtherThread(threading.Thread):
        def __init__(self, func, pause=0.05):
            threading.Thread.__init__(self)
            self.f = func
            self.pause = pause
        def run(self):
            if self.f:
                time.sleep(self.pause)
                self.f('o')

    def lockedop(self, who, mode='r', sleep=0.5):
        lf = utils.LockedFile(self.lfile, mode)
        self.assertIsNone(lf.fo)
        with lf as lockdfile:
            self.assertIsNotNone(lf.fo)
            self.rfd.write(who+'a')
            time.sleep(sleep)
            self.rfd.write(who+'r')
        self.assertIsNone(lf.fo)
    
    def setUp(self):
        self.tf = Tempfiles()
        self.lfile = self.tf("test.txt")
        self.rfile = self.tf("result.txt")
        self.rfd = None

    def tearDown(self):
        self.tf.clean()

    def test_shared_reads(self):
        def f(who):
            self.lockedop(who, 'r')
        t = self.OtherThread(f)
        with open(self.rfile,'w') as self.rfd:
            t.start()
            f('t')
            t.join()
        with open(self.rfile) as self.rfd:
            data = self.rfd.read()

        self.assertEqual(data, "taoatror")
            
    def test_exclusive_writes1(self):
        def f(who):
            self.lockedop(who, 'w')
        t = self.OtherThread(f)
        with open(self.rfile,'w') as self.rfd:
            t.start()
            f('t')
            t.join()
        with open(self.rfile) as self.rfd:
            data = self.rfd.read()

        self.assertEqual(data, "tatroaor")
            
    def test_exclusive_writes2(self):
        def f(who):
            self.lockedop(who, 'w')
        t = self.OtherThread(f)
        with open(self.rfile,'w') as self.rfd:
            t.start()
            self.lockedop('t', 'r')
            t.join()
        with open(self.rfile) as self.rfd:
            data = self.rfd.read()

        self.assertEqual(data, "tatroaor")
            
    def test_exclusive_writes3(self):
        def f(who):
            self.lockedop(who, 'r')
        t = self.OtherThread(f)
        with open(self.rfile,'w') as self.rfd:
            t.start()
            self.lockedop('t', 'w')
            t.join()
        with open(self.rfile) as self.rfd:
            data = self.rfd.read()

        self.assertEqual(data, "tatroaor")

class TestAtomicAccessFile(test.TestCase):

    testdata = os.path.join(testdatadir3,
                            "3A1EE2F169DD3B8CE0531A570681DB5D1491.json")

    class TstDataFile(utils.AtomicAccessFile):
        def _parse_data(self, fd):
            return json.load(fd)
        def _format_data(self, data, fd):
            json.dump(data, fd)

    def setUp(self):
        self.TstDataFile._clean_lock_dir()
        self.tf = Tempfiles()
        self.lfile = self.tf.track("test.json")
        self.rfile = self.tf.track("result.json")
        self.rfd = None

    def tearDown(self):
        self.tf.clean()

    def test_aquirerelease(self):
        sf = self.TstDataFile(self.rfile)
        sf.acquire(self.TstDataFile.LOCK_READ)
        self.assertEqual(sf.lock_type, self.TstDataFile.LOCK_READ)
        sf.acquire(self.TstDataFile.LOCK_READ)
        self.assertEqual(sf.lock_type, self.TstDataFile.LOCK_READ)
        with self.assertRaises(RuntimeError):
            sf.acquire(self.TstDataFile.LOCK_WRITE)
        sf.release()
        self.assertIsNone(sf.lock_type)

        sf.acquire(self.TstDataFile.LOCK_WRITE)
        self.assertEqual(sf.lock_type, self.TstDataFile.LOCK_WRITE)
        sf.acquire(self.TstDataFile.LOCK_WRITE)
        self.assertEqual(sf.lock_type, self.TstDataFile.LOCK_WRITE)
        sf.release()
        self.assertIsNone(sf.lock_type)

        with self.TstDataFile(self.rfile, self.TstDataFile.LOCK_READ) as sf:
            self.assertEqual(sf.lock_type, self.TstDataFile.LOCK_READ)
        self.assertIsNone(sf.lock_type)

    def test_simple_readwrite(self):
        self.assertFalse(self.TstDataFile.lock_dir.exists())
        self.assertFalse(os.path.exists(self.lfile))

        wrap = self.TstDataFile(self.lfile)
        self.assertTrue(self.TstDataFile.lock_dir.is_dir())
        self.assertTrue(wrap._lockfile.is_file())
        self.assertEqual(wrap._lockfile.parent, self.TstDataFile.lock_dir)
        self.assertFalse(os.path.exists(self.lfile))

        data = utils.read_json(self.testdata)
        wrap.write_data(data)
        self.assertTrue(os.path.isfile(self.lfile))
        written = utils.read_json(self.lfile)
        self.assertEqual(written, data)

        written = None
        written = wrap.read_data()
        self.assertEqual(written, data)
        self.assertTrue(wrap._lockfile.is_file())
        
    def test_atomic_readwrite(self):
        self.assertFalse(self.TstDataFile.lock_dir.exists())
        self.assertFalse(os.path.exists(self.lfile))

        data = utils.read_json(self.testdata)
        self.TstDataFile.write(self.lfile, data)
        self.assertTrue(os.path.isfile(self.lfile))
        written = utils.read_json(self.lfile)
        self.assertEqual(written, data)

        written = None
        written = self.TstDataFile.read(self.lfile)
        self.assertEqual(written, data)

    @classmethod
    def other_proc_read(cls, dfile, ofile):
        data = cls.TstDataFile.read(dfile)
        with open(ofile, 'w') as fd:
            json.dump(data, fd)

    def test_other_proc_read(self):
        with open(self.testdata) as fd:
            data = json.load(fd)

        self.assertFalse(os.path.exists(self.rfile))
        self.other_proc_read(self.testdata, self.rfile)
        self.assertTrue(os.path.isfile(self.rfile))
        
        with open(self.rfile) as fd:
            loaded = json.load(fd)
        self.assertEqual(loaded, data)

    def test_shared_reads(self):
        """
        Try some simultaneous reads
        """
        p = Process(target=self.other_proc_read, args=(self.testdata, self.rfile,))
        self.assertFalse(p.is_alive())
        self.assertFalse(os.path.exists(self.rfile))

        with open(self.testdata) as fd:
            data = json.load(fd)
        
        wrap = self.TstDataFile(self.testdata, self.TstDataFile.LOCK_READ)
        try:
            p.start()
            p.join(1.0)
            self.assertFalse(p.is_alive() and p.exitcode is not None,
                             "Other proc read apparently block")
            self.assertEqual(p.exitcode, 0)
            self.assertTrue(os.path.isfile(self.rfile))
            with open(self.rfile) as fd:
                loaded = json.load(fd)
            self.assertEqual(loaded, data)
        finally:
            wrap.release()

    def test_blocked_reads(self):
        """
        Try to read while write-locked
        """
        p = Process(target=self.other_proc_read, args=(self.testdata, self.rfile,))
        self.assertFalse(p.is_alive())
        self.assertFalse(os.path.exists(self.rfile))

        with open(self.testdata) as fd:
            data = json.load(fd)
        
        wrap = self.TstDataFile(self.testdata, self.TstDataFile.LOCK_WRITE)
        try:
            p.start()
            p.join(0.5)
            self.assertTrue(p.is_alive(), "Other proc apparently failed to block")
            self.assertFalse(os.path.exists(self.rfile))
        finally:
            wrap.release()
            p.join(1.0)
            self.assertFalse(p.is_alive(), "Other proc apparently couldn't unblock")

        self.assertEqual(p.exitcode, 0)
        self.assertTrue(os.path.isfile(self.rfile))
        with open(self.rfile) as fd:
            loaded = json.load(fd)
        self.assertEqual(loaded, data)

    @classmethod
    def other_proc_write(cls, dfile, ofile):
        with open(dfile) as fd:
            data = json.load(fd)
        cls.TstDataFile.write(ofile, data)

    def test_other_proc_write(self):
        with open(self.testdata) as fd:
            data = json.load(fd)

        self.assertFalse(os.path.exists(self.rfile))
        self.other_proc_write(self.testdata, self.rfile)
        self.assertTrue(os.path.isfile(self.rfile))
        
        with open(self.rfile) as fd:
            loaded = json.load(fd)
        self.assertEqual(loaded, data)

    def test_block_write(self):
        """
        Try to write while read-locked
        """
        p = Process(target=self.other_proc_write, args=(self.testdata, self.rfile,))
        self.assertFalse(p.is_alive())
        self.assertFalse(os.path.exists(self.rfile))

        with open(self.testdata) as fd:
            data = json.load(fd)
        
        wrap = self.TstDataFile(self.rfile, self.TstDataFile.LOCK_READ)
        try:
            p.start()
            p.join(0.5)
            self.assertFalse(os.path.exists(self.rfile),
                             "Other proc apparently failed to block write")
            self.assertTrue(p.is_alive(), "Other proc apparently failed to block")
        finally:
            wrap.release()
            p.join(1.0)
            self.assertFalse(p.is_alive(), "Other proc apparently couldn't unblock")

        self.assertEqual(p.exitcode, 0)
        self.assertTrue(os.path.isfile(self.rfile))
        with open(self.rfile) as fd:
            loaded = json.load(fd)
        self.assertEqual(loaded, data)

    def test_block_multi_writes(self):
        """
        Try to write while read-locked
        """
        p = Process(target=self.other_proc_write, args=(self.testdata, self.rfile,))
        self.assertFalse(p.is_alive())
        self.assertFalse(os.path.exists(self.rfile))

        with open(self.testdata) as fd:
            data = json.load(fd)
        
        wrap = self.TstDataFile(self.rfile, self.TstDataFile.LOCK_WRITE)
        try:
            p.start()
            p.join(0.5)
            self.assertFalse(os.path.exists(self.rfile),
                             "Other proc apparently failed to block write")
            self.assertTrue(p.is_alive(), "Other proc apparently failed to block")
        finally:
            wrap.release()
            p.join(1.0)
            self.assertFalse(p.is_alive(), "Other proc apparently couldn't unblock")

        self.assertEqual(p.exitcode, 0)
        self.assertTrue(os.path.isfile(self.rfile))
        with open(self.rfile) as fd:
            loaded = json.load(fd)
        self.assertEqual(loaded, data)

        
        
                
        

    

class TestJsonIO(test.TestCase):
    # this class focuses on testing the locking of JSON file IO
    
    testdata = os.path.join(testdatadir3,
                            "3A1EE2F169DD3B8CE0531A570681DB5D1491.json")

    def setUp(self):
        self.tf = Tempfiles()
        self.jfile = self.tf("data.json")

    def tearDown(self):
        self.tf.clean()

    class OtherThread(threading.Thread):
        def __init__(self, func, pause=0.05):
            threading.Thread.__init__(self)
            self.f = func
            self.pause = pause
        def run(self):
            if self.f:
                time.sleep(self.pause)
                self.f()

    def write_test_data(self):
        with open(self.testdata) as fd:
            data = json.load(fd)

    def test_writes(self):
        # this is not a definitive test that the use of LockedFile is working
        data = utils.read_json(self.testdata)
        data['foo'] = 'bar'
        def f():
            utils.write_json(data, self.jfile)
        t = self.OtherThread(f)

        data2 = dict(data)
        data2['foo'] = 'BAR'
        
        t.start()
        utils.write_json(data2, self.jfile)
        t.join()

        # success in these two lines indicate that the file was not corrupted
        data = utils.read_json(self.jfile)
        self.assertIn('@id', data)

        # success in this test indicates that writing happened in the expected
        # order; failure means that the test function is not test what we
        # exected.
        self.assertEqual(data['foo'], 'bar')

    def test_readwrite(self):
        # this is not a definitive test that the use of LockedFile is working
        data = utils.read_json(self.testdata)
        with open(self.jfile,'w') as fd:
            json.dump(data, fd)
        data['foo'] = 'bar'
        def f():
            utils.write_json(data, self.jfile)
        t = self.OtherThread(f)
        
        t.start()
        td = utils.read_json(self.jfile)
        t.join()

        self.assertIn('@id', td)
        self.assertNotIn('foo', td)
        td = utils.read_json(self.jfile)
        self.assertIn('@id', td)
        self.assertEqual(td['foo'], 'bar')

    def test_writeread(self):
        # this is not a definitive test that the use of LockedFile is working
        data = utils.read_json(self.testdata)
        with open(self.jfile,'w') as fd:
            json.dump(data, fd)
        data['foo'] = 'bar'
        self.td = None
        def f():
            self.td = utils.read_json(self.jfile)
        t = self.OtherThread(f)
        
        t.start()
        utils.write_json(data, self.jfile)
        t.join()

        self.assertIn('@id', self.td)
        self.assertEqual(self.td['foo'], 'bar')

    def test_path_encoder(self):
        p = Path("junk")
        enc = json.JSONEncoder()
        with self.assertRaises(TypeError):
            enc.default(p)
        with self.assertRaises(TypeError):
            enc.default(5j)
        enc = utils._PathTolerantJSONEncoder()
        self.assertTrue(isinstance(enc.default(p), str))
        with self.assertRaises(TypeError):
            enc.default(5j)

    def test_write_with_path(self):
        data = utils.read_json(self.testdata)
        data['workdir'] = Path("/tmp/work")
        utils.write_json(data, self.jfile)
        data = utils.read_json(self.jfile)
        self.assertTrue(isinstance(data['workdir'], str))

        data['complx'] = 5j
        with self.assertRaises(StateException):
            utils.write_json(data, self.jfile)
        


if __name__ == '__main__':
    test.main()
