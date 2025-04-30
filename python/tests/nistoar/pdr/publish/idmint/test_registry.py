import os, json, pdb, logging, time, shutil, re
import unittest as test

from nistoar.pdr.publish.idmint import registry as reg
from nistoar.pdr.exceptions import StateException, ConfigurationException
from nistoar.nerdm.convert.rmm import NERDmForRMM
from nistoar.pdr.utils import read_nerd

from nistoar.testing import *
from nistoar.pdr.describe import rmm

descdir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                       "describe")
datadir = os.path.join(descdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(descdir)))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    archdir = os.path.join(tdir, "mdarchive")
    shutil.copytree(datadir, archdir)

    rmmdir = os.path.join(archdir, "rmm-test-archive")
    loadmd(os.path.join(archdir, "pdr2210.json"), rmmdir)
    loadmd(os.path.join(archdir, "pdr02d4t.json"), rmmdir)

    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --set-ph archive_dir={4} --pidfile {5}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), rmmdir, pidfile)
    os.system(cmd)
    time.sleep(0.5)

_edi0pfx = 'ark:/88434/edi0'
def loadmd(nerdfile, rmmdir):
    rmmrec = read_nerd(nerdfile)
    if rmmrec.get('@id','').startswith(_edi0pfx):
        rmmrec['@id'] = _edi0pfx + '-' + rmmrec['@id'][len(_edi0pfx):]
    rmmrec = NERDmForRMM().to_rmm(rmmrec)
    basen = re.sub(r'^ark:/\d+/', '', rmmrec['record'].get('ediid', rmmrec['record']['@id']))
    for part in "record version releaseSet".split():
        odir = os.path.join(rmmdir, part + "s")
        if not os.path.exists(odir):
            os.mkdir(odir)

        ofile = basen
        if part == "version" and rmmrec['version'].get('version'):
            ver = rmmrec['version']['version'].replace('.','_')
            if not ofile.endswith(ver) and not ofile.endswith(rmmrec['version']['version']):
                ofile += "-v" + ver
        ofile = os.path.join(odir, ofile+".json")

        with open(ofile, 'w') as fd:
            json.dump(rmmrec[part], fd, indent=4, separators=(',', ': '))

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
        self.assertIn("ark:/88434/edi0-0hw91c", data)
        self.assertEqual(data["ark:/88434/edi0-0hw91c"], {"ediid": "ark:/88434/pdr2210"})
        self.assertEqual(len(data), 9)
        
    def test_iter_filter(self):
        self.ldr = reg.RMMLoader(self.cfg, "ark:/88434/edi0", self.proj)
        data = dict(self.ldr.iter())
        self.assertIn("ark:/88434/edi0-0hw91c", data)
        self.assertEqual(data["ark:/88434/edi0-0hw91c"], {"ediid": "ark:/88434/pdr2210"})
        self.assertNotIn("ark:/88434/pdr02d4t", data)
        # self.assertEqual(data["ark:/88434/pdr02d4t"], {"ediid": "ABCDEFG"})
        self.assertEqual(len(data), 1)

        self.ldr = reg.RMMLoader(self.cfg, "edi0", self.proj)
        data = dict(self.ldr.iter())
        self.assertEqual(len(data), 0)

        self.ldr = reg.RMMLoader(self.cfg, "ark:/88434/", self.proj)
        data = dict(self.ldr.iter())
        self.assertEqual(len(data), 9)

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
