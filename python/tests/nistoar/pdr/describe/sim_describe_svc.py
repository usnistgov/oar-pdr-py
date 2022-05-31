import json, os, sys, re, hashlib, traceback as tb
from urllib.parse import parse_qs
from collections import OrderedDict
from io import TextIOWrapper
from wsgiref.headers import Headers

testdir = os.path.dirname(os.path.abspath(__file__))
def_archdir = os.path.join(testdir, 'data', 'rmm-test-archive')

try:
    import uwsgi
except ImportError:
    print("Warning: running describe-uwsgi in simulate mode", file=sys.stderr)
    class uwsgi_mod(object):
        def __init__(self):
            self.opt={}
    uwsgi=uwsgi_mod()

class SimArchive(object):
    pfxre = re.compile("^ark:/\d+/")
    
    def __init__(self, archdir):
        self.dir = archdir
        self.loadall()

    def loadlu(self, colldir):
        lu = {}
        for rec in [f for f in os.listdir(os.path.join(self.dir, colldir)) if f.endswith(".json")]:
            try:
                with open(os.path.join(self.dir,colldir,rec)) as fd:
                    data = json.load(fd, object_pairs_hook=OrderedDict)
                if "@id" in data:
                    pdrid = data["@id"]
                    lu[pdrid] = rec[:-1*len(".json")]

            except:
                pass

        return lu

    def loadall(self):
        self.records = self.loadlu("records")
        self.versions = self.loadlu("versions")
        self.releaseSets = self.loadlu("releaseSets")

    def add_rec(self, coll, data):
        if coll not in "records versions releaseSets".split():
            raise ValueError("Unsupported collection: "+coll)

        aipid = self.pfxre.sub('', data.get('ediid', data.get('@id', ''))).rstrip('/')
        if aipid.endswith("/pdr:v"):
            aipid = aipid[:-1*len("/pdr:v")]
        aipid = re.sub('/pdr:v/', '-v', aipid)
            
        if not aipid or '/' in aipid:
            raise ValueError("Missing or bad identifier data in NERDm record: "+str(aipid))
        with open(os.path.join(self.dir, coll, aipid+".json"), 'w') as fd:
            json.dump(data, fd, indent=2)
        getattr(self, coll)[data.get('@id', data.get('ediid',''))] = aipid

    def get_rec(self, coll, id):
        dataf = None
        aipid = id
        if id.startswith('ark:'):
            aipid = getattr(self, coll).get(id)
        if aipid:
            dataf = os.path.join(self.dir, coll, aipid+'.json')
        if not dataf or not os.path.isfile(dataf):
            return None
        with open(dataf) as fd:
            return json.load(fd, object_pairs_hook=OrderedDict)
        
    def pdrid2aipid(self, coll, ediid):
        if coll not in "records versions releaseSets".split():
            raise ValueError("Bad collection name: " + coll)
        return getattr(self, coll).get(ediid)

    def aipids(self, coll="records"):
        if coll not in "records versions releaseSets".split():
            raise ValueError("Bad collection name: " + coll)
        return [f[:-5] for f in os.listdir(os.path.join(self.dir, coll)) if f.endswith(".json")]

class SimRMM(object):
    def __init__(self, recdir):
        self.archive = SimArchive(recdir)

    def handle_request(self, env, start_resp):
        handler = SimRMMHandler(self.archive, env, start_resp)
        return handler.handle()

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

class SimRMMHandler(object):

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
            return self.send_error(200, "Ready")

        parts = path.split('/', 1)
        coll = parts[0];
        if coll not in self._collections:
            return self.send_error(404, "Collection Not Found")
        path = (len(parts) > 1 and parts[1]) or ''
            
        id = None
        if len(path.strip()) > 0:
            return self.send_error(405, "POST not allowed on this resource")

        try:
            bodyin = self._env['wsgi.input'].read().decode('utf-8')
            nerd = json.loads(bodyin, object_pairs_hook=OrderedDict)
            self.arch.add_rec(coll, nerd)

        except ValueError as ex:
            return self.send_error(400, "Unparseable JSON input")
        except TypeError as ex:
            return self.send_error(500, "Write error")

        return self.send_error(201, "Accepted")

    _collections = "records versions releaseSets".split()
    def do_GET(self, path, params=None):
        if path:
            path = path.rstrip('/')

        if self._chatty:
            print("path="+str(path)+"; params="+str(params))
        if not path:
            return self.send_error(200, "Ready")

        parts = path.split('/', 1)
        coll = parts[0];
        if coll not in self._collections:
            return self.send_error(404, "Collection Not Found")
        path = (len(parts) > 1 and parts[1]) or ''
            
        aipids = []
        if not path and "@id" in params:
            path = params["@id"]
            path = (len(path) > 0 and path[0]) or ''
        if path:
            if path.startswith("ark:/"):
                aipids = [self.arch.pdrid2aipid(coll, path)]
                if not aipids[0]:
                    return self.send_error(404, path + " does not exist in " + coll)
            else:
                aipids = [path]
        else:
            aipids = self.arch.aipids(coll)

        nonsearch="include exclude".split()
        out = { "ResultCount": 0, "PageSize": 0, "ResultData": [] }
        for id in aipids:
            try:
                data = self.arch.get_rec(coll, id)
                if not data:
                    if len(aipids) == 1:
                        return self.send_error(404, id + " does not exist in " + coll)
                    continue

                if any([k != '@id' for k in params.keys() if k not in nonsearch]):
                    # search criteria present; do a simple test
                    keep = True
                    for prop in params:
                        if prop in nonsearch:
                            continue
                        if prop not in data:
                            keep = False
                            break
                        keep = False
                        for val in params[prop]:
                            if data[prop] == val:
                                keep = True
                                break
                        if not keep:
                            break
                    if not keep:
                        continue

                if '_id' not in params.get('exclude',[]):
                    data["_id"] ={"timestamp":1521220572,"machineIdentifier":3325465}
                out["ResultData"].append(data)
                out["ResultCount"] += 1
                out["PageSize"] += 1

            except Exception as ex:
                print(str(ex))
                if len(aipids) == 1:
                    return self.send_error(500, "Internal error")

        if len(aipids) == 1:
            if len(out["ResultData"]) == 0:
                return self.send_error(404, aipids[0] + " does not exist in "+coll)
            elif path and (not params or not params['@id']):
                out = out["ResultData"][0]
            
        self.set_response(200, "Identifier exists")
        self.add_header('Content-Type', 'application/json')
        self.end_headers()
        out = json.dumps(out, indent=2) + "\n"
        return [ out.encode() ]


archdir = uwsgi.opt.get("archive_dir", def_archdir)
try:
    archdir = archdir.decode()
except (UnicodeDecodeError, AttributeError):
    pass
application = SimRMM(archdir)
