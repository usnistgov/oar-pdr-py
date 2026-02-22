#! /usr/bin/env python3
#
import unittest as test
import sys, os, tempfile, json
from pathlib import Path
from io import StringIO
from collections import namedtuple, OrderedDict

from nistoar.testing import uwsgi
uwsgi = uwsgi.load()

scrptdir = Path(__file__).resolve().parents[1]
basedir = scrptdir.parents[0]
defconfigfile = basedir / 'docker'/'midasserver'/'midas-dmpdap_conf.yml'

if not uwsgi.opt.get('oar_config_file'):
    uwsgi.opt['oar_config_file'] = str(defconfigfile)
tmpdir = None
if not uwsgi.opt.get('oar_working_dir'):
    tmpdir = tempfile.TemporaryDirectory(prefix="_test_midas_uwsgi.")
    uwsgi.opt['oar_working_dir'] = tmpdir.name
uwsgi.opt['oar_midas_db_type'] = 'inmem'

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules[name] = out
    spec.loader.exec_module(out)
    return out

wsgiappsrc = scrptdir / 'midas-uwsgi.py'
midas = import_file(wsgiappsrc, 'midas')
mdsapp = midas.application

HttpResponse = namedtuple('HttpResponse', "code message headers".split())

class TestMDS3DAPApp(test.TestCase):

    def start(self, status, headers=None, extup=None):
        stat = status.split(' ', 1)
        self.resp = HttpResponse(stat[0], stat[1], headers)

    def body2dict(self, body):
        return json.loads("\n".join(self.tostr(body)), object_pairs_hook=OrderedDict)

    def tostr(self, resplist):
        return [e.decode() for e in resplist]

    def setUp(self):
        self.rootpath = "/midas/dap/mds3/"
        self.resp = []
        mdsapp.subapps[''].reset()

    def test_config(self):
        self.assertEqual(mdsapp.cfg.get('services',{}).get('dap',{}).get('conventions',{}).
                         get('mds3', {}).get('nerdstorage',{}).get('type'),
                         'inmem')

    def test_put_title(self):
        path = ""
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "Gurn's Opus" }))

        body = mdsapp(req, self.start)
        self.assertEqual(self.resp.code, "201")
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "Gurn's Opus")
        self.assertEqual(resp['id'], "mds3:0001")
        self.assertEqual(resp['data']['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['data']['doi'], 'doi:10.18434/mds3-0001')
        self.assertEqual(resp['data']['@type'], [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertEqual(resp['data'].get('title',''), '')
        id = resp['id']

        path = id + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        body = mdsapp(req, self.start)
        self.assertEqual(self.resp.code, "200")
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.18434/mds3-0001')
        self.assertEqual(resp['@type'], [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertEqual(resp.get('title',''), '')

        path = id + '/data/title'
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO('"My way"')
        self.assertEqual(self.resp.code, "200")
        body = mdsapp(req, self.start)
        resp = self.body2dict(body)
        self.assertEqual(resp, "My way")

        path = id + '/data'
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        body = mdsapp(req, self.start)
        self.assertEqual(self.resp.code, "200")
        resp = self.body2dict(body)
        self.assertEqual(resp['@id'], 'ark:/88434/mds3-0001')
        self.assertEqual(resp['doi'], 'doi:10.18434/mds3-0001')
        self.assertEqual(resp['@type'], [ "nrdp:PublicDataResource", "dcat:Resource" ])
        self.assertEqual(resp.get('title',''), 'My way')

        

        


                         
if __name__ == '__main__':
    test.main()
        
