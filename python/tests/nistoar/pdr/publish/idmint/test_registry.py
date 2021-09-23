import os, json, pdb, logging, time
import unittest as test

from nistoar.pdr.publish.idmint import registry as reg
from nistoar.pdr.exceptions import StateException, ConfigurationException

from nistoar.testing import *
from nistoar.pdr.describe import rmm

descdir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                       "describe")
datadir = os.path.join(descdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(descdir)))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), srvport,
                     os.path.join(basedir, wpy), pidfile)
    os.system(cmd)
    time.sleep(0.5)

def stopService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_merge.log"))
    loghdlr.setLevel(logging.INFO)
    rootlog.addHandler(loghdlr)

    startService()

def tearDownModule():
    stopService()
    
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    rmtmpdir()

class Test_RMMLoader(test.TestCase):

    def setUp(self):
        self.cfg = {
            'metadata_service': {
                'service_endpoint': baseurl
            }
        }
        self.proj = lambda r: {"ediid": r['ediid']}

    def test_ctor(self):
        self.ldr = reg.RMMLoader(self.cfg, None, self.proj)
        self.assertTrue(self.ldr.cli)
        self.assertIsNone(self.ldr.prefix)
        self.assertEqual(self.ldr.project({"foo": "bar", "ediid": "crazyfox"}), {"ediid": "crazyfox"})

        self.ldr = reg.RMMLoader(self.cfg)
        self.assertTrue(self.ldr.cli)
        self.assertIsNone(self.ldr.prefix)
        self.assertEqual(self.ldr.project({"foo": "bar", "ediid": "crazyfox"}), {})

        with self.assertRaises(ConfigurationException):
            self.ldr = reg.RMMLoader({})

    def test_iter_all(self):
        self.ldr = reg.RMMLoader(self.cfg, None, self.proj)
        data = dict(self.ldr.iter())
        self.assertIn("ark:/88434/pdr02d4t", data)
        self.assertEqual(data["ark:/88434/pdr02d4t"], {"ediid": "ABCDEFG"})
        self.assertIn("ark:/88434/edi00hw91c", data)
        self.assertEqual(data["ark:/88434/edi00hw91c"], {"ediid": "ark:/88434/pdr2210"})
        self.assertEqual(len(data), 2)
        
    def test_iter_filter(self):
        self.ldr = reg.RMMLoader(self.cfg, "ark:/88434/edi0", self.proj)
        data = dict(self.ldr.iter())
        self.assertIn("ark:/88434/edi00hw91c", data)
        self.assertEqual(data["ark:/88434/edi00hw91c"], {"ediid": "ark:/88434/pdr2210"})
        self.assertNotIn("ark:/88434/pdr02d4t", data)
        # self.assertEqual(data["ark:/88434/pdr02d4t"], {"ediid": "ABCDEFG"})
        self.assertEqual(len(data), 1)

        self.ldr = reg.RMMLoader(self.cfg, "edi0", self.proj)
        data = dict(self.ldr.iter())
        self.assertEqual(len(data), 0)

        self.ldr = reg.RMMLoader(self.cfg, "ark:/88434/", self.proj)
        data = dict(self.ldr.iter())
        self.assertEqual(len(data), 2)

    def test_loader_in_registry(self):
        self.ldr = reg.RMMLoader(self.cfg, "ark:/88434/pdr", self.proj)
        areg = reg.CachingIDRegistry(None, {}, self.ldr)
        self.assertIn("ark:/88434/pdr02d4t", areg.uncached)
        self.assertEqual(len(areg.uncached), 1)
        self.assertTrue(areg.registered("ark:/88434/pdr02d4t"))
        self.assertEqual(areg.get_data("ark:/88434/pdr02d4t"), {"ediid": "ABCDEFG"})


class TestCachingIDRegistry(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.cfg = {
            'cache_on_register': False,
            'id_store_file': "goober.tsv"
        }
        self.cachedir = self.tf.mkdir("idreg")
        self.cachefile = os.path.join(self.cachedir, self.cfg.get('id_store_file', 'f00.tsv'))
        self.reg = reg.CachingIDRegistry(self.cachedir, self.cfg, name="f00")

    def tearDown(self):
        self.tf.clean()

    def test_ctor(self):
        self.assertEqual(len(self.reg.cached), 0)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(self.reg.store, self.cachefile)
        self.assertTrue(not os.path.exists(self.cachefile))

    def test_cache_reload(self):
        self.test_ctor()

        self.reg.registerID("ark:/88888/mds2-4444", {"a": 1})
        self.assertIn("ark:/88888/mds2-4444", self.reg.uncached)
        self.assertEqual(len(self.reg.uncached), 1)
        self.assertEqual(len(self.reg.cached), 0)
        self.assertTrue(not os.path.exists(self.cachefile))

        self.assertTrue(self.reg.registered("ark:/88888/mds2-4444"))
        self.assertEqual(self.reg.get_data("ark:/88888/mds2-4444"), {"a": 1})

        self.reg.registerID("foo:bar", {"foo": "bar"})
        self.assertIn("foo:bar", self.reg.uncached)
        self.assertEqual(len(self.reg.uncached), 2)
        self.assertEqual(len(self.reg.cached), 0)
        self.assertTrue(not os.path.exists(self.cachefile))

        self.assertTrue(self.reg.registered("foo:bar"))
        self.assertEqual(self.reg.get_data("foo:bar"), {"foo": "bar"})

        self.reg.cache_data()
        self.assertIn("ark:/88888/mds2-4444", self.reg.cached)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(len(self.reg.cached), 2)
        self.assertTrue(os.path.exists(self.cachefile))
        
        self.assertTrue(self.reg.registered("ark:/88888/mds2-4444"))
        self.assertTrue(self.reg.registered("foo:bar"))

        with open(self.cachefile, 'a') as fd:
            fd.write("%s\t%s\n" % ("goob", 'null'))
        self.reg = reg.CachingIDRegistry(self.cachedir, self.cfg, name="f00")
        self.assertIn("goob", self.reg.cached)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(len(self.reg.cached), 3)
        self.assertTrue(os.path.exists(self.cachefile))
        
        self.assertTrue(self.reg.registered("ark:/88888/mds2-4444"))
        self.assertTrue(self.reg.registered("foo:bar"))
        self.assertTrue(self.reg.registered("goob"))

        self.reg.cache_immediately = True
        self.reg.registerID("urn:X", None)
        self.reg.registered("urn:X")
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(len(self.reg.cached), 4)
        with open(self.cachefile) as fd:
            self.assertEqual(len([line[0] for line in fd]), 4)

    def test_as_pdrregistry(self):
        self.reg = reg.PDRIDRegistry(self.cfg, self.cachedir, "edi0")
        self.assertIsNone(self.reg.initloader)
        self.assertEqual(len(self.reg.cached), 0)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(self.reg.store, self.cachefile)
        self.assertTrue(not os.path.exists(self.cachefile))

        self.cfg['metadata_service'] = {
            'service_endpoint': baseurl
        }
        del self.cfg['cache_on_register']
            
        self.reg = reg.PDRIDRegistry(self.cfg, self.cachedir, "edi0", False)
        self.assertTrue(self.reg.initloader)
        self.assertEqual(len(self.reg.cached), 0)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertEqual(self.reg.store, self.cachefile)
        self.assertTrue(not os.path.exists(self.cachefile))

        self.assertEqual(self.reg.init_cache(), 1)
        self.assertEqual(len(self.reg.cached), 1)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertTrue(os.path.exists(self.cachefile))

        os.remove(self.cachefile)
        self.assertTrue(not os.path.exists(self.cachefile))
        self.reg = reg.PDRIDRegistry(self.cfg, self.cachedir, "edi0", True)
        self.assertTrue(self.reg.initloader)
        self.assertEqual(len(self.reg.uncached), 0)
        self.assertTrue(os.path.exists(self.cachefile))
        self.assertEqual(len(self.reg.cached), 1)

                    
                         
if __name__ == '__main__':
    test.main()
