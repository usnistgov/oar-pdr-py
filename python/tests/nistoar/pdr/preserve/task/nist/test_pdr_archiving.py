import os, json, pdb, logging, tempfile, shutil, time
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.preserve.task import framework as fw
from nistoar.pdr.preserve import PreservationStateError
from nistoar.base import config
from nistoar.pdr.utils import checksum_of

pdrdir = Path(__file__).resolve().parents[3] 
datadir = pdrdir / "distrib" / "data"
zipfiles = [datadir/f for f in os.listdir(datadir) if f.startswith("pdr2210")]
cksfiles = []
basedir = pdrdir.parents[3]

tmpdir = tempfile.TemporaryDirectory(prefix="_test_archiving.")
port = 9091
baseurl = "http://localhost:{0}/".format(port)

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    tdir = tmpdir.name
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
    status = os.system(cmd) == 0
    time.sleep(0.5)
    return status

def stopService(authmeth=None):
    tdir = tmpdir.name
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
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_archiving.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

    ckfdir = Path(tmpdir.name) / "checksums"
    ckfdir.mkdir()
    for zf in zipfiles:
        csum = checksum_of(zf)
        cf = ckfdir / (zf.name+".sha256")
        with open(cf, 'w') as fd:
            fd.write(csum)
            fd.write("\n")
        cksfiles.append(cf)
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
    tmpdir.cleanup()

class TestPDR1AIPArchiving(test.TestCase):

    def setUp(self):
        self.workdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.storedir = Path(self.workdir.name) / "store"
        self.storedir.mkdir()
        self.archfiles = zipfiles + cksfiles
        self.archfiles.sort()
        self.bucket = "s3:nist_midas"

        self.stcfg = {
            "working_dir": self.workdir.name,
            "persist_in": self.workdir.name
        }
        self.cfg = {
            "store_dir": self.storedir,
            "public_bucket": self.bucket,
            "repo_access": {
                "distrib_service": {
                    "service_endpoint": "http://localhost:9091"
                }
            },
            "polling": {
                "cycle_time": 5,
                "wait_for_completion": False
            }
        }

        self.arch = pdr.PDR1AIPArchiving(self.cfg)
        self.mgr = st.JSONPreservationStateManager.for_aip(self.stcfg, "pdr2210", "bags/pdr2210")
        self.mgr.set_serialized_files([str(f) for f in self.archfiles])

    def tearDown(self):
        self.workdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.arch.cfg)
        self.assertEqual(self.arch.storedir, self.storedir)
#        self.assertEqual(self.arch.finalbucket, self.bucket)

    def test_safe_copy(self):
        self.mgr.record_progress("Goob!")
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))
        self.arch._safe_copy(self.archfiles, self.storedir, self.mgr)
        copied = [f for f in os.listdir(self.storedir) if not f.startswith('.')]
        copied.sort()
        self.assertEqual(len(copied), len(self.archfiles))
        self.assertEqual(copied, sorted([f.name for f in self.archfiles]))
        self.assertIn(str(self.archfiles[-1].name), self.mgr.message)

    def test_safe_copy_checksums(self):
        self.mgr.record_progress("Goob!")
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))
        self.arch._safe_copy(self.archfiles, self.storedir, self.mgr, True)
        copied = [f for f in os.listdir(self.storedir) if not f.startswith('.')]
        copied.sort()
        self.assertEqual(len(copied), len(self.archfiles))
        self.assertEqual(copied, sorted([f.name for f in self.archfiles]))
        self.assertIn("checksum files", self.mgr.message)

    def test_ensure_dir(self):
        dirp = self.storedir / "goof"
        self.assertTrue(not dirp.is_dir())
        self.assertEqual(self.arch._ensure_dir(dirp), dirp)
        self.assertTrue(dirp.is_dir())
        self.assertEqual(self.arch._ensure_dir(dirp), dirp)
        self.assertTrue(dirp.is_dir())
        
    def test_ensure_dir_fail(self):
        dirp = self.storedir / "goof"
        with open(dirp, 'w'):
            pass
        self.assertTrue(dirp.is_file())
        with self.assertRaises(PreservationStateError):
            self.arch._ensure_dir(dirp)

    def test_launch_migration(self):
        self.mgr.record_progress("Goob!")
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))
        log = logging.getLogger("archiving")
        self.arch.launch_migration(self.mgr, log)

        copied = [f for f in os.listdir(self.storedir) if not f.startswith('.')]
        copied.sort()
        self.assertEqual(len(copied), len(self.archfiles)+2)  # tmp dirs still exist
        
        self.assertIn("finishing", self.mgr.message)

    def test_launch_migration_fail(self):
        self.mgr.record_progress("Goob!")
        self.mgr.set_serialized_files(self.mgr.get_serialized_files() +
                                      [str(datadir/"goob"), str(datadir/"goob.sha256")])
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))
        log = logging.getLogger("archiving")

        with self.assertRaises(fw.AIPArchivingException):
            self.arch.launch_migration(self.mgr, log)
        self.assertIn("failure detected", self.mgr.message)

        # temp dirs still exist in target directory
        tmpdirs = [f for f in os.listdir(self.storedir) if f.startswith('_')]
        self.assertEqual(len(tmpdirs), 2)

        self.arch.clean_up(self.mgr)
        tmpdirs = [f for f in os.listdir(self.storedir) if f.startswith('_')]
        self.assertEqual(len(tmpdirs), 0)

    def test_clean_tmp_dest_dirs(self):
        cksdir = self.arch._ensure_dir(self.storedir / "goob")
        serdir = self.arch._ensure_dir(self.storedir / "gurn")
        self.mgr.set_state_property("archiving:serialized_temp_store", str(serdir))
        self.mgr.set_state_property("archiving:checksum_temp_store", str(cksdir))

        self.assertTrue(cksdir.is_dir())
        self.assertTrue(serdir.is_dir())
        
        self.arch._clean_tmp_dest_dirs(self.mgr)
        self.assertTrue(not cksdir.exists())
        self.assertTrue(not serdir.exists())

    def test_monitor_destination_disabled(self):
        log = logging.getLogger("archiving")
        self.mgr.record_progress("Goob!")
        self.assertFalse(self.arch.cfg.get("polling", {}).get("wait_for_completion", True))
        self.arch.monitor_destination(self.mgr, log)
        self.assertEqual(self.mgr.message, "Goob!")

    def test_apply(self):
        self.mgr.record_progress("Goob!")
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))

        self.arch.apply(self.mgr)
        
        copied = [f for f in os.listdir(self.storedir) if not f.startswith('.')]
        copied.sort()
        self.assertEqual(len(copied), len(self.archfiles)+2)  # tmp dirs still exist
        self.assertEqual(self.mgr.steps_completed, self.mgr.ARCHIVED | self.mgr.SUBMITTED)
        self.assertIn("submitted", self.mgr.message)
        
    def test_run(self):
        self.mgr.record_progress("Goob!")
        self.assertFalse(any(f for f in os.listdir(self.storedir) if not f.startswith('.')))

        self.arch.run(self.mgr, False)
        
        copied = [f for f in os.listdir(self.storedir) if not f.startswith('.')]
        copied.sort()
        self.assertEqual(len(copied), len(self.archfiles))
        self.assertEqual(copied, sorted([f.name for f in self.archfiles]))

    def test_findaipsfor(self):
        repo = pdr.RepositoryAccess(self.cfg.get('repo_access'))
        archfiles = self.arch._findaipsfor(repo, "pdr2210", "1.0")
        self.assertEqual(len(archfiles), 2)
        self.assertTrue(all(isinstance(f, str) and f.endswith('.zip') for f in archfiles))

    def test_aip_available(self):
        repo = pdr.RepositoryAccess(self.cfg.get('repo_access'))
        self.assertTrue(self.arch._aip_available(repo, "pdr2210.1_0.mbag0_3-1.zip"))

    def test_publicaips(self):
        repo = pdr.RepositoryAccess(self.cfg.get('repo_access'))
        aipfiles = ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip"]
        found = self.arch._publicaips(repo, aipfiles)
        self.assertTrue(isinstance(found, set))
        self.assertEqual(len(aipfiles), 2)
        for aip in aipfiles:
            self.assertIn(aip, found)



if __name__ == '__main__':
    test.main()
