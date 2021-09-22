import os, sys, pdb, json, subprocess, threading, time, logging
import unittest as test

from nistoar.testing import *
import nistoar.pdr.utils.io as utils

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

    


if __name__ == '__main__':
    test.main()
