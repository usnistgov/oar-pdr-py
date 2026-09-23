"""
Simulated repo services

This module provides a web service that simulates the APIs of the public PDR.  It is intended for
for testing and development purposes; in particular, it can operate as a stand-in for the public PDR 
supporting the PDR publishing services.  

It combines into a single web service three APIs:  the describe (RMM) service, the distrib service, 
and the ingest service.  
"""
import sys, os, re
from pathlib import Path

from nistoar.pdr.public.sim.rmm import SimRMM, records, versions, releasesets
from nistoar.pdr.public.sim.distrib import SimDistrib

class SimRepo():

    def __init__(self, basedir, baseurl, bagdir=None, readonly=False):
        self.basedir = Path(basedir)
        if not self.basedir.is_dir():
            raise RuntimeError(f"basedir {str(self.basedir)}: does not exist as a directory")
        self.rmm = SimRMM(basedir, readonly)

        if not bagdir:
            bagdir = self.basedir/"bags"
            if not bagdir.exists():
                bagdir.mkdir()
        else:
            bagdir = Path(bagdir)    
        if not bagdir.is_dir():
            raise RuntimeError(f"distrib dir {str(archdir)}: does not exist as a directory")
        self.distrib = SimDistrib(bagdir, baseurl.rstrip('/')+"/ds/")

    def handle_request(self, env, start_resp):
        path = env.get('PATH_INFO', '/')
        parts = path.lstrip('/').split('/', 1)
        if parts[0] == "ds":
            env['PATH_INFO'] = '/'+parts[1]
            return self.distrib.handle_request(env, start_resp)
        else:
            return self.rmm.handle_request(env, start_resp)

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

# setup for when this is used as uWSGI script
application = None
if __name__.startswith("uwsgi_file_"):
  try:
    import uwsgi
    # running as a uwsgi script

    archdir = uwsgi.opt.get("archive_dir")
    if not archdir:
        raise RuntimeError("required archive_dir option not set")
    try:
        archdir = archdir.decode()
    except (UnicodeDecodeError, AttributeError):
        pass

    bagdir = uwsgi.opt.get("bagdir", os.path.join(archdir, "bags"))
    if not bagdir:
        raise RuntimeError("required archive_dir option not set")
    try:
        bagdir = bagdir.decode()
    except (UnicodeDecodeError, AttributeError):
        pass
        
    baseurl = uwsgi.opt.get("baseurl", "http://localhost/")
    try:
        baseurl = baseurl.decode()
    except (UnicodeDecodeError, AttributeError):
        pass

    readonly = uwsgi.opt.get("readonly", True)
    try:
        if isinstance(readonly, bytes):
            readonly = readonly.decode()
        if not isinstance(readonly, bool):
            if re.match(r'\-?\d+', readonly):
                readonly = bool(int(readonly))
            else:
                readonly = readonly.lower() == "true"
    except (UnicodeDecodeError, AttributeError, ValueError):
        print("*** WARNING: Trouble parsing readonly parameter; setting to True", file=sys.stderr)
        readonly = True
    else:
        if readonly:
            print(">>> Running SimRepo in readonly mode", file=sys.stderr)

    application = SimRepo(archdir, baseurl, bagdir, readonly)

  except ImportError:
    # not running 
    print("*** WARNING: failed to load uwsgi environment; is uwsgi really executing this scripts?",
          file=sys.stderr)
