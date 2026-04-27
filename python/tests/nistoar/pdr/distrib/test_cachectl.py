import os, json, pdb, logging, tempfile, re, shutil
from pathlib import Path
import unittest as test
from unittest import mock
from io import StringIO
import yaml

from nistoar.pdr.distrib.cachectl import CacheCtlClient

datadir = Path(__file__).parents[0] / "data"
assert os.path.isdir(datadir)

with open(datadir/"cache_responses.json") as fd:
    respdata = json.load(fd)

# This method will be used by the mock to replace requests.request
# Thanks to Johannes Fahrenkrug for the code this function is based on
# (https://stackoverflow.com/questions/15753390/how-can-i-mock-requests-and-the-response)
class MockResponse:
    def __init__(self, status_code, reason="OK", json_data=None, method=None):
        self.json_data = json_data
        self.status_code = status_code
        self.reason = reason
        self.method = method

    def json(self):
        if self.json_data is None:
            raise requests.exceptions.JSONDecoderError("No content")
        return self.json_data

    def close(self):
        pass

    @property
    def text(self):
        if self.json_data is None:
            return ""
        elif isinstance(self.json_data, str):
            return self.json_data   # interpret as already text, not JSON
        return json.dumps(self.json_data)

class TestCacheCtlClient(test.TestCase):

    def setUp(self):
        self.base = "http://example.com/cache"
        self.cli = CacheCtlClient(self.base, "secret")

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_is_up(self, mockreq):
        mockreq.return_value = MockResponse(200, method='HEAD')

        self.assertTrue(self.cli.is_up())
        self.assertEqual(mockreq.call_count, 1)
        self.assertEqual(mockreq.call_args[0][0], 'HEAD')

        mockreq.return_value = MockResponse(500, method='HEAD')

        self.assertTrue(not self.cli.is_up())
        self.assertEqual(mockreq.call_count, 2)        
        self.assertEqual(mockreq.call_args[0][0], 'HEAD')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_volumes(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['volumes'], method='GET')

        vols = self.cli.volumes()
        self.assertEqual(vols, respdata['volumes'])
        self.assertEqual(mockreq.call_count, 1)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/volumes/')

        names = self.cli.volume_names()
        self.assertEqual(names, ["gen0", "gen1"])
        self.assertEqual(mockreq.call_count, 2)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/volumes/')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_describe_volume(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['volumes'][0], method='GET')

        vol = self.cli.describe_volume("gen0")
        self.assertEqual(vol, respdata['volumes'][0])
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/volumes/gen0')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_objects_for(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['objects'], method='GET')

        objs = self.cli.objects_for("mds2-3834")
        self.assertEqual(objs, respdata['objects'])
        self.assertEqual(mockreq.call_count, 1)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/objects/mds2-3834/:files')

        objs = self.cli.datasets()
        self.assertEqual(objs, respdata['objects'])
        self.assertEqual(mockreq.call_count, 2)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/objects/')

        objs = self.cli.cached_objects_for("mds2-3834")
        self.assertEqual(objs, respdata['objects'])
        self.assertEqual(mockreq.call_count, 3)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/objects/mds2-3834/:cached')

        objs = self.cli.checked_objects_for("mds2-3834")
        self.assertEqual(objs, respdata['objects'])
        self.assertEqual(mockreq.call_count, 4)
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/objects/mds2-3834/:checked')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_describe_object(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['objects'][0], method='GET')

        obj = self.cli.describe_object("mds2-3834", "EVMG Survey - data for publication.csv")
        self.assertEqual(obj, respdata['objects'][0])
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1],
                         self.base+'/objects/mds2-3834/EVMG%20Survey%20-%20data%20for%20publication.csv')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_cached(self, mockreq):
        mockreq.return_value = MockResponse(202, "OK", "queued")

        self.cli.ensure_cached("mds2-3834", "EVMG Survey - data for publication.csv")
        self.assertEqual(mockreq.call_args[0][0], 'PUT')
        self.assertEqual(mockreq.call_args[0][1],
                         self.base+'/objects/mds2-3834/EVMG%20Survey%20-%20data%20for%20publication.csv/%3Acached')

        self.assertTrue(self.cli.uncache("mds2-3834", "EVMG Survey - data for publication.csv"))
        self.assertEqual(mockreq.call_args[0][0], 'DELETE')
        self.assertEqual(mockreq.call_args[0][1],
                         self.base+'/objects/mds2-3834/EVMG%20Survey%20-%20data%20for%20publication.csv/%3Acached')

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_queue_status(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['queue_status'], method='GET')

        self.assertEqual(self.cli.queue_status(), respdata['queue_status'])
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/queue/')
        self.assertEqual(mockreq.call_count, 1)

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_monitor_status(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", respdata['monitor_status'], method='GET')

        self.assertEqual(self.cli.monitor_status(), respdata['monitor_status'])
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/monitor/')
        self.assertEqual(mockreq.call_count, 1)

    @mock.patch('nistoar.pdr.distrib.client.requests.request')
    def test_monitor_status(self, mockreq):
        mockreq.return_value = MockResponse(200, "OK", 'True\n', method='GET')

        self.assertTrue(self.cli.monitor_is_running())
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/monitor/running')
        self.assertEqual(mockreq.call_count, 1)

        mockreq.return_value = MockResponse(200, "OK", 'False\n', method='GET')

        self.assertTrue(not self.cli.monitor_is_running())
        self.assertEqual(mockreq.call_args[0][0], 'GET')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/monitor/running')
        self.assertEqual(mockreq.call_count, 2)

        mockreq.return_value = MockResponse(200, "OK", 'False\n', method='DELETE')

        self.assertTrue(self.cli.stop_monitoring())
        self.assertEqual(mockreq.call_args[0][0], 'DELETE')
        self.assertEqual(mockreq.call_args[0][1], self.base+'/monitor/running')
        self.assertEqual(mockreq.call_count, 3)

        
        
        
    




    
        
    


if __name__ == '__main__':
    test.main()
