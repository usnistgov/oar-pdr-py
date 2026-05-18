import os, sys
from wsgiref.headers import Headers
from urllib.parse import parse_qs
from pathlib import Path

try:
    import uwsgi
except ImportError:
    print("Warning: running ingest-uwsgi in simulate mode", file=sys.stderr)
    class uwsgi_mod(object):
        def __init__(self):
            self.opt={}
    uwsgi=uwsgi_mod()
    tdir = tempfile.TemporaryDirectory(prefix="test_fmflask.")
    uwsgi.opt['workdir'] = tdir.name

from nistoar.midas.dap.fm import service as fm, sim, flask as fmflask
from nistoar.midas.dap.fm.scan import base as scan, simjobexec
from nistoar.base.config import configure_log

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'
def_baseurl = "http://localhost/"

wrkdir = uwsgi.opt.get('workdir', '/tmp')
if isinstance(wrkdir, bytes):
    wrkdir = wrkdir.decode('utf-8')
rootdir = Path(wrkdir) / "fmdata"
jobdir = Path(wrkdir) / "jobqueue"
if not rootdir.exists():
    rootdir.mkdir()
if not jobdir.exists():
    jobdir.mkdir()

config = {
    "flask": {
        "SECRET_KEY": "supersecret"
    },
    "service": {
        "admin_user": "admin",
        'nextcloud_base_url': 'http://mocknextcloud/nc',
        'webdav': {
            'service_endpoint': 'http://mockservice/api',
        },
        'generic_api': {
            'service_endpoint': 'http://mockservice/api',
        },
        'authentication': {
            'client_cert_path': str(certpath),
            'client_key_path':  str(keypath)
        },
        'local_storage_root_dir': str(rootdir),
        'admin_user': 'admin',
        'authentication': {
            'user': 'admin',
            'pass': 'pw'
        },
        'scan_queue': {
            'jobdir': str(jobdir)
        }
    },
    "admin_user": "admin",
    "logdir": wrkdir,
    "logfile": "test_sim_fmflask.log"
}

configure_log(config=config)
svccfg = config.get('service')
nccli = sim.SimNextcloudApi(rootdir, svccfg.get('generic_api',{}))
wdcli = sim.SimFMWebDAVClient(rootdir, svccfg.get('webdav',{}))
svc = fm.MIDASFileManagerService(svccfg, nccli=nccli, wdcli=wdcli)
scan.set_slow_scan_queue(str(jobdir), resume=False)
scan.slow_scan_queue.mod = simjobexec

application = fmflask.create_app(config, svc)
