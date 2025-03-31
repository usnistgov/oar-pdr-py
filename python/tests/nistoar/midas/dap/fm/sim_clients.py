"""
Simulated file manager clients for testing the MIDAS application layer service
"""
import json, os, shutil
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from unittest.mock import Mock

from nistoar.midas.dap.fm.clients import NextcloudApi, FMWebDAVClient
from nistoar.midas.dap.fm.exceptions import *
from nistoar.midas.dap.fm import service as svc

class SimNextcloudApi(NextcloudApi):
    PERMFILE = "_perms.json"
    USERFILE = "_users.json"

    def __init__(self, datadir, config={}):
        if not isinstance(datadir, Path):
            datadir = Path(datadir)
        self.rootdir = datadir
        self.cfg = config

        adminuser = self.cfg.get('authentication',{}).get('user', 'oar_api')
        self.basepath = f"/fm/nc/remote.php/dav/files/{adminuser}/"

    def test(self):
        out = Mock()
        if self.rootdir.is_dir():
            out.status_code = 200
            out.reason = "OK"
            out.text = "Test endpoint reached successfully."
        else:
            out.status_code = 500
            out.reason = "Server Error"
            out.text = "Sim root dir does not exist as a directory"
        return out

    def headers(self):
        return { "Host": "mockserver" }

    def get_user_permissions(self, dir_name):
        out = {"ocs": { "meta": {'status': 'ok', 'statuscode': 200, 'message': 'OK'}, 'data': []}}
        dir = self.rootdir / dir_name
        if not dir.exists():
            raise FileManagerResourceNotFound(dir_name, "folder does not exist")

        if dir.is_file():
            dir = dir.parents[0]

        permfile = None
        while str(dir).startswith(str(self.rootdir)):
            permfile = dir / self.PERMFILE
            if permfile.is_file():
                break
            permfile = None
            dir = dir.parents[0]

        if permfile:
            try:
                with open(permfile) as fd:
                    out['ocs']['data'] = json.load(fd)
            except Exception as ex:
                raise UnexpectedFileManagerResponse("Unexpected response: (500): "+str(ex))

        return out

    def set_user_permissions(self, user_name, perm_type, dir_name):
        defperm = {'share_with': user_name, 'uid_owner': self.cfg.get('admin_user', 'oar_api'),
                   'item_type': 'folder', 'permissions': svc.PERM_NONE }
        data = self.get_user_permissions(dir_name)
        data = data.get('ocs', {}).get('data', [])
        perm = [p for p in data if p.get('share_with') == user_name]
        if len(perm) == 0:
            perm = defperm
            data.append(perm)
        else:
            perm = perm[0]

        dir = self.rootdir / dir_name
        if not dir.exists():
            raise FileManagerResourceNotFound(dir_name, "Folder does not exist")

        if dir.is_file():
            raise FileManagerServerError(405, f'files/userpermissions/{user_name}/{dir_name}',
                                         "Method not allowed", "Can only delete permissions on folders")

        perm['permissions'] = perm_type
        permfile = dir / self.PERMFILE
        try:
            with open(permfile, 'w') as fd:
                json.dump(data, fd, indent=2)
        except Exception as ex:
            raise UnexpectedFileManagerResponse("Unexpected response (500): "+str(ex))

        return perm

    def delete_user_permissions(self, user_name, dir_name):
        dir = self.rootdir / dir_name
        if not dir.exists(dir):
            raise FileManagerResourceNotFound(dir_name, "folder does not exist")

        if dir.is_file():
            raise FileManagerServerError(405, f'files/userpermissions/{user_name}/{dir_name}',
                                         "Method not allowed", "Can only delete permissions on folders")

        data = self.get_user_permissions(dir_name)
        data.set_default('ocs', { 'data': [] })
        if any(p.get('share_with') == user_name for p in data['ocs']['data']):
            newperms = [p for p in data['ocs']['data'] if p['share_with'] != user_name]
            with open(dir/self.PERMFILE, 'w') as fd:
                json.dump(newperms, fd, indent=2)
            data['ocs']['data'] = newperms

        return data

    def get_users(self):
        userfile = self.rootdir / self.USERFILE
        if not userfile.is_file():
            return {}

        try:
            with open(userfile) as fd:
                return json.load(fd)
        except Exception as ex:
            raise UnexpectedFileManagerResponse("Unexpected response (500): "+str(ex))

    def get_user(self, user_name):
        return self.get_users().get(user_name, {})

    def create_user(self, user_name):
        lu = self.get_users()
        if user_name in lu:
            return lu[user_name]

        lu[user_name] = { 'user_id': user_name, 'user_directory': str(self.rootdir/user_name), 'enabled': True }
        try:
            with open(self.rootdir/self.USERFILE, 'w') as fd:
                json.dump(lu, fd, indent=2)
        except Exception as ex:
            raise UnexpectedFileManagerResponse("Unexpected response (500): "+str(ex))

    def disable_user(self, user_name):
        lu = self.get_users()
        if user_name not in lu:
            raise FileManagerResourceNotFound(f'users/{user_name}/disable', "user does not exist")
        if lu[user_name].get('enabled'):
            lu[user_name]['enabled'] = False
            try:
                with open(self.rootdir/self.USERFILE, 'w') as fd:
                    json.dump(lu, fd, indent=2)
            except Exception as ex:
                raise UnexpectedFileManagerResponse("Unexpected response (500): "+str(ex))

    def enable_user(self, user_name):
        lu = self.get_users()
        if user_name not in lu:
            raise FileManagerResourceNotFound(f'users/{user_name}/enable', "user does not exist")
        if not lu[user_name].get('enabled'):
            lu[user_name]['enabled'] = True
            try:
                with open(self.rootdir/self.USERFILE, 'w') as fd:
                    json.dump(lu, fd, indent=2)
            except Exception as ex:
                raise UnexpectedFileManagerResponse("Unexpected response (500): "+str(ex))

    def is_user(self, user):
        return bool(self.get_user(user))

    def is_user_enabled(self, user):
        return self.get_user(user).get('enabled', False)

    xfiletmpl = "<d:response>\n  <d:href>%s</d:href>\n  <d:propstat>\n   <d:prop>\n    <d:resourcetype>%s</d:resourcetype>\n   </d:prop>\n  <d:status>HTTP/1.1 200 OK</d:status>\n  </d:propstat>\n </d:response>"
    xenvtmpl = '<?xml version="1.0"?>\n<d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">\n %s\n</d:multistatus>'
    def scan_directory_files(self, dir_path):
        fromdir = self.rootdir / dir_path
        out = []
        if fromdir.is_dir():
            out = [self.xfiletmpl % (self.basepath+dir_path, "collection")]
            for root, dirs, files in os.walk(fromdir):
                for f in dirs:
                    f = os.path.join(root[len(str(self.rootdir))+1:], f)
                    out.append(self.xfiletmpl % (self.basepath+f, "<d:collection/>"))
                for f in files:
                    f = os.path.join(root[len(str(self.rootdir))+1:], f)
                    out.append(self.xfiletmpl % (self.basepath+f, ""))
                    
        return self.xenvtmpl % "\n ".join(out)

    def scan_all_files(self):
        return "+------+\n|  Junk  |\n+------+\n"
    
    def scan_user_files(self, user_name):
        return "+------+\n|  Junk  |\n+------+\n"
    
            
class SimFMWebDAVClient(FMWebDAVClient):
    def __init__(self, datadir, config={}):
        if not isinstance(datadir, Path):
            datadir = Path(datadir)
        self.rootdir = datadir
        self.cfg = config

        adminuser = self.cfg.get('authentication',{}).get('user', 'oar_api')
        self.basepath = f"/fm/nc/remote.php/dav/files/{adminuser}/"

    def authenticate(self):
        return "XXXX"

    def is_directory(self, path):
        target = self.rootdir/path
        return target.is_dir()

    def is_file(self, path):
        target = self.rootdir/path
        return target.is_file()

    def exists(self, path):
        target = self.rootdir/path
        return target.exists()

    def ensure_directory(self, path):
        target = self.rootdir/path
        if target.is_file():
            raise FileManagerClientError("Unable to create directory: already exists as a file")

        if not target.is_dir():
            try:
                os.makedirs(target)
            except Exception as ex:
                raise FileManagerServerError("Failed to create directory: "+str(ex)) from ex

    def get_resource_info(self, path):
        target = self.rootdir/path
        if not target.exists():
            raise FileManagerResourceNotFound(path, "Unable to get resource info: Remote resource does not exist")
        stat = target.stat()
        out = { "name": '/'+str(path), "fileid": "100", "size": stat.st_size, "permissions": "RGDNVCK",
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat() }
        if target.is_dir():
            out['type'] = "folder"
        else:
            out['type'] = "file"

        return out

    def delete_resource(self, path):
        target = self.rootdir/path
        if not target.exists():
            raise FileManagerResourceNotFound(path, "Unable to get resource info: Remote resource does not exist")
        if target.is_dir():
            shutil.rmtree(target)
        else:
            os.remove(target)


    
