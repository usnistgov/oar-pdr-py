import os, pdb, sys, json
import unittest as test
from copy import deepcopy

from nistoar.testing import *
from nistoar.pdr.publish.service import status

def setUpModule():
    ensure_tmpdir()
def tearDownModule():
    rmtmpdir()

class TestSIPStatusFile(test.TestCase):

    cachefile = os.path.join(tmpdir(), "statusfile.json")
    data = {
        'id':  'aaa',
        'goob': 'gurn',
        'age': 5
    }

    def setUp(self):
        with open(self.cachefile, 'w') as fd:
            json.dump(self.data, fd)

    def tearDown(self):
        if os.path.exists(self.cachefile):
            os.remove(self.cachefile)

    def test_ctor(self):
        sf = status.SIPStatusFile(self.cachefile)
        self.assertEqual(sf._file, self.cachefile)
        self.assertIsNone(sf._fd)
        self.assertIsNone(sf._type)
        del sf

        sf = status.SIPStatusFile(self.cachefile, status.LOCK_READ)
        self.assertEqual(sf._file, self.cachefile)
        self.assertIsNotNone(sf._fd)
        self.assertEqual(sf._type, status.LOCK_READ)
        self.assertEqual(sf.lock_type, status.LOCK_READ)
        del sf

        sf = status.SIPStatusFile(self.cachefile, status.LOCK_WRITE)
        self.assertEqual(sf._file, self.cachefile)
        self.assertIsNotNone(sf._fd)
        self.assertEqual(sf._type, status.LOCK_WRITE)
        self.assertEqual(sf.lock_type, status.LOCK_WRITE)

    def test_aquirerelease(self):
        sf = status.SIPStatusFile(self.cachefile)
        sf.acquire(status.LOCK_READ)
        self.assertEqual(sf.lock_type, status.LOCK_READ)
        sf.acquire(status.LOCK_READ)
        self.assertEqual(sf.lock_type, status.LOCK_READ)
        with self.assertRaises(RuntimeError):
            sf.acquire(status.LOCK_WRITE)
        sf.release()
        self.assertIsNone(sf.lock_type)

        sf.acquire(status.LOCK_WRITE)
        self.assertEqual(sf.lock_type, status.LOCK_WRITE)
        sf.acquire(status.LOCK_WRITE)
        self.assertEqual(sf.lock_type, status.LOCK_WRITE)
        sf.release()
        self.assertIsNone(sf.lock_type)

        with status.SIPStatusFile(self.cachefile, status.LOCK_READ) as sf:
            self.assertEqual(sf.lock_type, status.LOCK_READ)
        self.assertIsNone(sf.lock_type)

    def test_read(self):
        sf = status.SIPStatusFile(self.cachefile)
        self.assertIsNone(sf.lock_type)
        data = sf.read_data()
        self.assertIsNone(sf.lock_type)
        self.assertEqual(data, self.data)
            
        sf = status.SIPStatusFile(self.cachefile, status.LOCK_READ)
        self.assertEqual(sf.lock_type, status.LOCK_READ)
        data = sf.read_data()
        self.assertEqual(sf.lock_type, status.LOCK_READ)
        self.assertEqual(data, self.data)
            
    def test_write(self):
        data = deepcopy(self.data)
        data['goob'] = 'gurn'
        
        sf = status.SIPStatusFile(self.cachefile)
        self.assertIsNone(sf.lock_type)
        sf.write_data(data)
        self.assertIsNone(sf.lock_type)

        with open(self.cachefile) as fd:
            d = json.load(fd)
        self.assertEqual(d, data)
        self.assertIn('goob', d)
            
        sf = status.SIPStatusFile(self.cachefile, status.LOCK_WRITE)
        self.assertEqual(sf.lock_type, status.LOCK_WRITE)
        sf.write_data(data)
        self.assertEqual(sf.lock_type, status.LOCK_WRITE)
        sf.release()

        with open(self.cachefile) as fd:
            d = json.load(fd)
        self.assertEqual(d, data)
        self.assertIn('goob', d)
            
class TestReadWrite(test.TestCase):

    cachefile = os.path.join(tmpdir(), "status.json")
    data = {
        'id':  'aaa',
        'goob': 'gurn',
        'age': 5
    }

    def tearDown(self):
        if os.path.exists(self.cachefile):
            os.remove(self.cachefile)

    def testWrite(self):
        status.SIPStatusFile.write(self.cachefile, self.data)

        with open(self.cachefile) as fd:
            got = json.load(fd)

        self.assertIn('goob', got)
        self.assertEqual(self.data, got)

    def testRead(self):
        with open(self.cachefile, 'w') as fd:
            json.dump(self.data, fd)

        got = status.SIPStatusFile.read(self.cachefile)
        self.assertIn('goob', got)
        self.assertEqual(self.data, got)


class TestReadWriteOld(test.TestCase):

    cachefile = os.path.join(tmpdir(), "status.json")
    data = {
        'id':  'aaa',
        'goob': 'gurn',
        'age': 5
    }

    def tearDown(self):
        if os.path.exists(self.cachefile):
            os.remove(self.cachefile)

    def testWrite(self):
        status._write_status(self.cachefile, self.data)

        with open(self.cachefile) as fd:
            got = json.load(fd)

        self.assertIn('goob', got)
        self.assertEqual(self.data, got)

    def testRead(self):
        with open(self.cachefile, 'w') as fd:
            json.dump(self.data, fd)

        got = status._read_status(self.cachefile)
        self.assertIn('goob', got)
        self.assertEqual(self.data, got)

class TestSIPStatus(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.cachedir = self.tf.mkdir("status")
        self.cfg = { 'cachedir': self.cachedir }
        self.status = status.SIPStatus("ffff", self.cfg)

    def tearDown(self):
        self.tf.clean()

    def read_data(self, filepath):
        with open(filepath) as fd:
            return json.load(fd)
    
    def test_ctor(self):
        data = {
            'user': {
                'id': 'ffff',
                'state': "not found",
                'message': status.user_message[status.NOT_FOUND],
                'authorized': [],
                'siptype': ''
            },
            'sys': {},
            'history': []
        }
        self.assertEqual(self.status.id, data['user']['id'])
        self.assertEqual(self.status.data, data)
        self.assertEqual(self.status._cachefile,
                         os.path.join(self.cachedir,"ffff.json"))

        with self.assertRaises(ValueError):
            status.SIPStatus('')

        data['goob'] = 'gurn'
        self.status = status.SIPStatus('ffff', _data=data)
        self.assertEqual(self.status.data['goob'], 'gurn')
        self.assertEqual(self.status.data, data)
        self.assertEqual(self.status._cachefile, "/tmp/sipstatus/ffff.json")

    def test_agent(self):
        self.assertEqual(self.status.agent_groups, [])
        self.status.agent_groups.append('goober')
        self.assertEqual(self.status.agent_groups, [])
        self.assertFalse(self.status.any_authorized('goober'))
        self.assertFalse(self.status.any_authorized(''))
        
        self.status.add_agent_group('goober')
        self.assertEqual(self.status.agent_groups, ['goober'])
        self.status.add_agent_group('gurn')
        self.assertEqual(self.status.agent_groups, ['goober', 'gurn'])

        self.assertTrue(self.status.any_authorized('goober'))
        self.assertTrue(self.status.any_authorized('gurn'))
        self.assertTrue(self.status.any_authorized(['bob', 'goober']))
        self.assertTrue(self.status.any_authorized(['gurn', 'goober', 'bob']))
        self.assertFalse(self.status.any_authorized(''))
        

    def test_arkid(self):
        self.status = status.SIPStatus("ark:/88888/pdr0-1518")
        self.assertEqual(self.status._cachefile, "/tmp/sipstatus/pdr0-1518.json")

    def test_str(self):
        self.status.start("goob1")
        self.status.update(status.PENDING, "starting soon")
        self.assertEqual(str(self.status), "ffff goob1 status: pending: starting soon")

    def test_requests(self):
        self.assertTrue(not os.path.exists(self.status._cachefile))
        self.assertEqual(status.SIPStatus.requests(self.cfg), [])
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'hank'), [])
        
        self.status.cache()
        self.assertTrue(os.path.exists(self.status._cachefile))
        self.assertEqual(status.SIPStatus.requests(self.cfg), ['ffff'])
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'hank'), [])
        self.assertEqual(status.SIPStatus.requests(self.cfg, ''), [])

        self.status.add_agent_group("hank", True)
        self.assertEqual(status.SIPStatus.requests(self.cfg), ['ffff'])
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'hank'), ['ffff'])
        self.assertEqual(status.SIPStatus.requests(self.cfg, ''), [])

        stat = status.SIPStatus("goob", self.cfg)
        stat.add_agent_group("gurn")
        sips = status.SIPStatus.requests(self.cfg)
        self.assertIn('ffff', sips)
        self.assertIn('goob', sips)
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'hank'), ['ffff'])
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'gurn'), ['goob'])
        stat.add_agent_group("hank")
        sips = status.SIPStatus.requests(self.cfg)
        self.assertIn('ffff', sips)
        self.assertIn('goob', sips)
        sips = status.SIPStatus.requests(self.cfg, 'hank')
        self.assertIn('ffff', sips)
        self.assertIn('goob', sips)
        self.assertEqual(status.SIPStatus.requests(self.cfg, 'gurn'), ['goob'])

    def test_cache(self):
        self.assertTrue(not os.path.exists(self.status._cachefile))
        self.status.data['gurn'] = 'goob'
        self.status.cache()
        self.assertTrue(os.path.exists(self.status._cachefile))

        self.assertIn('update_time', self.status.data['user'])
        self.assertTrue(isinstance(self.status.data['user']['update_time'], float))
        self.assertIn('updated', self.status.data['user'])

        data = self.read_data(self.status._cachefile)
        self.assertIn('update_time', data['user'])
        self.assertTrue(isinstance(data['user']['update_time'], float))
        self.assertIn('updated', data['user'])
        self.assertIn('gurn', data)
        self.assertEqual(data['user']['id'], 'ffff')
        self.assertEqual(data['gurn'], 'goob')
        self.assertEqual(data['user']['state'], 'not found')
        self.assertEqual(data['user']['message'], 
                         status.user_message[status.NOT_FOUND])

        self.status = status.SIPStatus("ffff", self.cfg)
        self.assertIn('gurn', self.status.data)
        self.assertEqual(self.status.data['gurn'], 'goob')
        self.assertEqual(self.status.data['user']['id'], 'ffff')

    def test_refresh(self):
        self.status.data['foo'] = 'bar'
        self.status.cache()
        self.status.data['gurn'] = 'goob'
        self.status.refresh()
        self.assertIn('foo', self.status.data)
        self.assertEqual(self.status.data['foo'], 'bar')
        self.assertNotIn('gurn', self.status.data)

    def test_update(self):
        self.assertTrue(not os.path.exists(self.status._cachefile))
        self.status.data['gurn'] = 'goob'

        with self.assertRaises(ValueError):
            self.status.update("hanky")

        self.status.update(status.PUBLISHED)
        self.assertTrue(os.path.exists(self.status._cachefile))
        self.assertEqual(self.status.data['user']['state'], 'published')
        self.assertEqual(self.status.data['user']['message'], 
                         status.user_message[status.PUBLISHED])
        self.assertNotIn('start_time', self.status.data['user'])
        self.assertNotIn('started', self.status.data['user'])
        
        data = self.read_data(self.status._cachefile)
        self.assertEqual(data['user']['state'], 'published')
        self.assertEqual(data['user']['message'], 
                         status.user_message[status.PUBLISHED])
        self.assertEqual(data['gurn'], 'goob')

        self.status.update(status.FAILED, "SIP is too big")
        self.assertEqual(self.status.data['user']['state'], 'failed')
        self.assertEqual(self.status.data['user']['message'], "SIP is too big")

    def test_start(self):
        self.assertTrue(not os.path.exists(self.status._cachefile))
        self.status.data['gurn'] = 'goob'

        self.status.start("goob1")
        self.assertEqual(self.status.data['user']['state'], 'processing')
        self.assertEqual(self.status.data['user']['message'], 
                         status.user_message[status.PROCESSING])
        self.assertEqual(self.status.data['gurn'], 'goob')
        self.assertEqual(self.status.agent_groups, [])

        self.status.update(status.FAILED)
        self.assertNotEqual(self.status.data['user']['state'], 'processing')

        self.status.start("goob1", "gurn", "chugging...")
        self.assertEqual(self.status.data['user']['state'], 'processing')
        self.assertEqual(self.status.data['user']['message'], "chugging...")
        self.assertEqual(self.status.agent_groups, ['gurn'])

    def test_user_export(self):
        self.status.start('goob')
        self.status.update(status.FAILED)
        self.status.start('gurn')

        data = self.status.user_export()
        self.assertIn('id',  data)
        self.assertIn('state',  data)
        self.assertIn('siptype', data)
        self.assertIn('history', data)
        self.assertEqual(data['id'],  'ffff')
        self.assertEqual(data['state'],  status.PROCESSING)
        self.assertEqual(data['siptype'],  'gurn')
        self.assertEqual(data['history'][0]['state'],  status.FAILED)
        self.assertEqual(data['history'][0]['siptype'],  'goob')

    def test_revert(self):
        self.test_user_export()
        self.status.revert()
        self.assertEqual(self.status.id, 'ffff')
        self.assertEqual(self.status.state, status.FAILED)
        self.assertEqual(self.status.siptype, 'goob')
        self.assertEqual(self.status.data['history'], [])
        
    def test_record_progress(self):
        self.assertEqual(self.status.data['user']['state'], status.NOT_FOUND)

        self.status.record_progress("almost there")
        data = self.read_data(self.status._cachefile)
        self.assertEqual(data['user']['state'], status.NOT_FOUND)
        self.assertEqual(data['user']['message'], "almost there")

        self.status.start("goob1")
        self.status.record_progress("started")
        data = self.read_data(self.status._cachefile)
        self.assertEqual(data['user']['state'], status.PROCESSING)
        self.assertEqual(data['user']['message'], "started")



        

if __name__ == '__main__':
    test.main()
