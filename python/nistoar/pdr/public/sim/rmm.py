"""
Simulated describe service
"""
import re, sys, json
from urllib.parse import parse_qs
from wsgiref.headers import Headers

from nistoar.pdr.public.sim.db import SimDb, records, versions, releasesets

class SimRMM(object):
    def __init__(self, recdir, readonly=True):
        self.archive = SimDb(recdir, readonly)

    def handle_request(self, env, start_resp):
        handler = SimRMMHandler(self.archive, env, start_resp)
        return handler.handle()

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

class SimRMMHandler(object):

    _collections = [records, versions, releasesets]

    def __init__(self, archive, wsgienv, start_resp, chatty=True):
        self.arch = archive
        self._env = wsgienv
        self._start = start_resp
        self._meth = wsgienv.get('REQUEST_METHOD', 'GET')
        self._hdr = Headers([])
        self._code = 0
        self._msg = "unknown status"
        self._chatty = chatty

    def send_error(self, code, message):
        status = "{0} {1}".format(str(code), message)
        excinfo = sys.exc_info()
        if excinfo == (None, None, None):
            excinfo = None
        self._start(status, [], excinfo)
        return []

    def add_header(self, name, value):
        self._hdr.add_header(name, value)

    def set_response(self, code, message):
        self._code = code
        self._msg = message

    def end_headers(self):
        status = "{0} {1}".format(str(self._code), self._msg)
        self._start(status, list(self._hdr.items()))

    def handle(self):
        meth_handler = 'do_'+self._meth

        path = self._env.get('PATH_INFO', '/')[1:]
        params = parse_qs(self._env.get('QUERY_STRING', ''))

        if hasattr(self, meth_handler):
            return getattr(self, meth_handler)(path, params)
        else:
            return self.send_error(403, self._meth +
                                   " not supported on this resource")

    def do_POST(self, path, params=None):
        if path:
            path = path.rstrip('/')

        if self._chatty:
            print("path="+str(path)+"; params="+str(params))
        if not path:
            return self.send_error(405, "Method not allowed")

        parts = path.split('/', 1)
        if len(parts) > 1:
            return self.send_error(403, "Forbidden")
        if parts[0] == "invalid" or params.get('strictness'):
            # this endpoint is used to force an error response
            return self.post_invalid_nerdm_record()
        if parts[0] == "ingest":
            if len(parts) > 1:
                return self.send_error(403, "Forbidden")
            path = parts[0]
        else:
            coll = parts[0];
            if coll not in self._collections:
                return self.send_error(404, "Collection Not Found")
            path = (len(parts) > 1 and parts[1]) or ''

        if self.arch.ro:
            return self.send_error(405, "DB is readonly")
            
        if not params.get('auth') and \
           (not self._env.get('HTTP_AUTHORIZATION') or
            not self._env['HTTP_AUTHORIZATION'].startswith("Bearer ")):
            return self.send_error(401, "Not Authorized")
        
        try:
            bodyin = self._env['wsgi.input'].read().decode('utf-8')
            nerd = json.loads(bodyin)
            
            if path == "ingest":
                self.arch.add_rec(nerd)
            else:
                self.arch.add_to_coll(coll, nerd)

        except ValueError as ex:
            return self.send_error(400, "Unparseable JSON input")
        except (TypeError, IOError) as ex:
            return self.send_error(500, "Write error")
        except Exception as ex:
            return self.send_error(500, "Server error")

        return self.send_error(201, "Accepted")

    def post_invalid_nerdm_record(self):
        self.set_response(400, "Input record is not valid")
        self.add_header('Content-Type', 'application/json')
        self.end_headers()
        out = json.dumps([
            "You have three misspelled words.",
            "The description is too flowery.",
            "And no one's taking responsibility for this embarrassment.",
            "In other words, I didn't bother to read it."
        ]) + '\n'
                 
        return [ out.encode() ]
    
    def do_GET(self, path, params=None):
        if path:
            path = path.rstrip('/')

        if self._chatty:
            print("path="+str(path)+"; params="+str(params))
        if not path:
            return self.send_error(200, "Ready")

        parts = path.split('/', 1)
        coll = parts[0];
        if coll == "ingest":
            if len(parts) > 1:
                return self.send_error(404, "No resouces below ingest")
            return self.send_error(200, "Ready")
        if coll not in self._collections:
            return self.send_error(404, "Collection Not Found")
        path = (len(parts) > 1 and parts[1]) or ''
            
        if not path and "@id" in params:
            path = params["@id"]
            path = (len(path) > 0 and path[0]) or ''
        if path:
            out = self.arch.get_rec_from(coll, path)
            if not out:
                return self.send_error(404, path + " does not exist in " + coll)
            recit = iter([out])
        else:
            recit = self.arch.iter(coll)

        nonsearch="include exclude".split()
        out = { "ResultCount": 0, "PageSize": 0, "ResultData": [] }
        try:
            for rec in recit:
                if any([k != '@id' for k in params.keys() if k not in nonsearch]):
                    # search criteria present; do a simple test
                    keep = True
                    for prop in params:
                        if prop in nonsearch:
                            continue
                        if prop not in rec:
                            keep = False
                            break
                        keep = False
                        for val in params[prop]:
                            if rec[prop] == val:
                                keep = True
                                break
                        if not keep:
                            break
                    if not keep:
                        continue

                if '_id' not in params.get('exclude',[]):
                    rec["_id"] ={"timestamp":1521220572,"machineIdentifier":3325465}
                out["ResultData"].append(rec)
                out["ResultCount"] += 1
                out["PageSize"] += 1

        except Exception as ex:
            print(str(ex))
            if len(aipids) == 1:
                return self.send_error(500, "Internal error")

        if path and (not params or not params['@id']):
            if len(out["ResultData"]) == 0:
                # (should not happen)
                return self.send_error(404, path+" does not exist in "+coll)
            out = out["ResultData"][0]

        self.set_response(200, "Identifier exists")
        self.add_header('Content-Type', 'application/json')
        self.end_headers()
        out = json.dumps(out, indent=2) + "\n"
        return [ out.encode() ]

# setup for when this is used as uWSGI script
application = None
if __name__.startswith("uwsgi_file_"):
  # running as a uwsgi script
  try:
    import uwsgi

    archdir = uwsgi.opt.get("archive_dir")
    if not archdir:
        raise RuntimeError("required archive_dir option not set")
    try:
        archdir = archdir.decode()
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
            print(">>> Running SimRMM in readonly mode", file=sys.stderr)

    application = SimRMM(archdir, readonly)

  except ImportError:
    # not running 
    print("*** WARNING: failed to load uwsgi environment; is uwsgi really executing this scripts?",
          file=sys.stderr)
