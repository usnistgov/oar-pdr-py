"""
This is a stand-alone feedback server specifically for responding to unit test requests
"""
import tempfile, sys, os, logging, signal
import traceback as tb
from wsgiref.simple_server import make_server, WSGIServer
from socketserver import ThreadingMixIn

from nistoar.midas.dap.extrev import wsgi
from nistoar.midas.dap.service import mds3
from nistoar.midas.dbio import AlreadyExists, PUBLIC_GROUP
from nistoar.pdr.utils.prov import Agent

try:
    import uwsgi
except ImportError:
    print("Warning: running testfb_uwsgi in simulate mode", file=sys.stderr)
    class uwsgi_mod(object):
        def __init__(self):
            self.opt={}
    uwsgi=uwsgi_mod()

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s: %(message)s"
tmpdir = None

def make_config(workdir):
    datadir = os.path.join(workdir, "dbfiles")
    if not os.path.exists(datadir):
        os.mkdir(datadir)
    return {
        'dbio': {
            "factory": "fsbased",
            "db_root_dir": datadir,
            "project_id_minting": {
                "default_shoulder": {
                    "public": "mds3"
                },
                "localid_providers": {
                    "public": [ "mds3" ]
                }
            }
        },
        'dap_service': {
            "doi_naan": "10.88888",
            "nerdstorage": {
#                "type": "fsbased",
#                "store_dir": os.path.join(tmpdir.name, "nrdstore")
                "type": "inmem",
            },
            "default_responsible_org": {
                "@type": "org:Organization",
                "@id": mds3.NIST_ROR,
                "title": "NIST"
            },
            "reviewer_ids": [ "npsop" ]
        },
        "authentication": {
            "authorized": [
                {
                    "auth_key": "secret",
                    "user": "npsop",
                    "client": "nps",
                }
            ],
            "raise_on_anonymous": True
        }
    }

class FatalError(Exception):
    def __init__(self, message, excode=1):
        if not isinstance(excode, int):
            excode = -1
        super(FatalError, self).__init__(message)
        self.exitcode = excode

def clean_up(*args):
    if tmpdir:
        tmpdir.cleanup()
        print("Working directory cleaned up", file=sys.stderr)

def create_app(workdir):
    return wsgi.app(make_config(workdir))

def init_data(fbapp):
    who = Agent("test", Agent.AUTO, "tester")
    svc = fbapp.svcfact.create_service_for(who)
    try:
        prec = svc.create_record("testrec", {"title": "Test Record"}, dbid="mds3:0001")
    except AlreadyExists as ex:
        pass
    else:
        svc.apply_external_review(prec.id, "nps1", "requested", _prec=prec)
        svc._set_review_permissions(prec, PUBLIC_GROUP)
        prec.save()

class TestWSGIServer(WSGIServer, ThreadingMixIn):
    pass

def setup(prog, *args):
    workdir = None
    if len(args) > 0 and args[0]:
        workdir = os.path.abspath(args[0])
        if not os.path.isdir(workdir):
            if not os.path.isdir(os.path.dirname(workdir)):
                raise FatalError(f"Work directory parent does not exist as a directory ({str(ex)})", 3)
            try:
                os.mkdir(workdir)
            except OSError as ex:
                raise FatalError("Failed to create work directory: " + str(ex)) from ex
    else:
        global tmpdir
        tmpdir = tempfile.TemporaryDirectory(prefix=f"{prog}_")
        workdir = tmpdir.name

    logpath = os.path.join(workdir, "feedback_server.log")
    logging.basicConfig(format=LOG_FORMAT, filename=logpath, level=logging.DEBUG)
        
    app = create_app(workdir)
    init_data(app)
    return app

def _dec(obj):
    # decode an object if it is not None
    return obj.decode() if isinstance(obj, (bytes, bytearray)) else obj

prog = os.path.splitext(os.path.basename(__file__))[0]
workdir = _dec(uwsgi.opt.get("workdir"))
application = setup(prog, workdir)

