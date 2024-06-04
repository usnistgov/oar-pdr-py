"""
This module provides a client class, WebDAVClient, designed to perform WebDAV operations that interact with a Nextcloud instance to manage directories and files
"""
import logging
import os
import tempfile
from datetime import datetime

from webdav3.client import Client as WebDavClient
from webdav3.exceptions import WebDavException

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class WebDAVApi:
    def __init__(self, config):
        self.is_prod = config.PROD
        self.api_user = config.API_USER
        if self.is_prod:
            self.base_url = config.WEBDAV_PROD_URL
        else:
            self.base_url = config.WEBDAV_DEV_URL
        self.client = WebDavClient({
            'webdav_hostname': self.base_url,
            'webdav_login': self.api_user,
            'webdav_password': config.API_PWD
        })
        self.headers = {'Accept': 'application/json'}

    def handle_request(self, method, target, content=None):
        """ Generic request handler. """
        try:
            path = f"/remote.php/dav/files/{self.api_user}/{target.lstrip('/')}"
            if method == 'MKCOL':
                self.client.mkdir(path)
                logging.info(f"Directory created: {path}")
                return {'status': 200, 'message': 'Directory created'}
            elif method == 'PROPFIND':
                info = self.client.list(path, get_info=True)
                return {'status': 200, 'info': info}
            elif method == 'DELETE':
                self.client.clean(path)
                logging.info(f"File/Folder deleted: {path}")
                return {'status': 200, 'message': 'File/Folder deleted'}
            elif method == 'CHECK':
                return self.client.check(path)
            elif method == 'PUT':
                if content is not None:
                    with tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.json') as temp_file:
                        temp_file.write(content)
                        temp_file.flush()
                        temp_file_path = temp_file.name
                    self.client.upload_sync(remote_path=path, local_path=temp_file_path)
                    logging.info(f"File uploaded/modified: {path}")
                    return {'status': 200, 'message': 'File uploaded/modified'}
                else:
                    raise ValueError("Content must be provided to upload or modify a file!")
            elif method == 'GET':
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                self.client.download_sync(remote_path=path, local_path=temp_file.name)
                with open(temp_file.name, 'r', encoding='utf-8') as file:
                    content = file.read()
                    logging.info(f"File content retrieved: {content}")
                    temp_file.close()
                    os.remove(temp_file.name)
                return {'status': 200, 'content': content}
            else:
                raise ValueError(f"Unsupported method: {method}")
        except WebDavException as e:
            logging.error(f"WebDav error occurred while handling request: {e}")
            return {'status': 500, 'message': str(e)}
        except Exception as e:
            logging.error(f"Uncaught error occurred while handling request: {e}")
            return {'status': 500, 'message': str(e)}

    def create_directory(self, path):
        """ Create a directory given a path. """
        return self.handle_request('MKCOL', path)

    def get_directory_info(self, dir_path):
        """ Get information on a directory given a path. """
        contents_info = self.get_contents_info(dir_path)
        dir_name = dir_path.rstrip('/').split("/")[-1]
        date_format = "%a, %d %b %Y %H:%M:%S %Z"
        total_size = 0
        date_times = []
        last_modified = None
        if len(contents_info) > 0:
            for content in contents_info:
                total_size += content['size']
                date_times.append(datetime.strptime(content['modified'], date_format))

            last_modified_date = max(date_times)
            last_modified = last_modified_date.strftime(date_format)

        dir_info = {
            'name': dir_name,
            'last_modified': last_modified,
            'total_size': total_size,
            'dir_info': contents_info
        }

        return dir_info

    def get_contents_info(self, dir_path):
        contents = self.handle_request('PROPFIND', dir_path)
        for content in contents['info']:
            path_parts = [part for part in content['path'].split('/') if part]
            if path_parts:
                content['name'] = path_parts[-1]
            if content.get('isdir', False):
                content['size'] = 0
                content['content_type'] = 'Directory'
                subdir_path = content['path'].split(f"{Config.API_USER}/")[-1].rstrip('/')
                content['info'] = self.get_contents_info(subdir_path)
                for sub_content in content['info']:
                    if sub_content['size'] is not None:
                        content['size'] += int(sub_content['size'])
            else:
                if content['size'] is not None:
                    content['size'] = int(content['size'])
            content.pop('created', None)

        return contents['info']

    def delete_directory(self, path):
        """ Delete a directory given a path. """
        return self.handle_request('DELETE', path)

    def upload_file(self, destination_path, file, filename=None):
        """ Post a file given its content and the path where to create it. """
        # Determine whether file is a filepath or a file object
        if filename is None:
            if isinstance(file, str):
                filename = os.path.basename(file)
            else:
                filename = getattr(file, 'name', None)
                if not filename:
                    logging.error("No filename provided")
                    return {'status': 400, 'message': "File name hasn't been provided"}

        if isinstance(file, str):
            with open(file, 'r') as file_to_upload:
                content = file_to_upload.read()
        else:
            content = file.read().decode('utf-8')

        path = os.path.join(destination_path, filename)
        return self.handle_request('PUT', path, content)

    def get_file_info(self, path):
        """ Get information on a file given a path. """
        return self.handle_request('PROPFIND', path)

    def get_file_content(self, path):
        """ Get content of a file given a path. """
        return self.handle_request('GET', path)

    def delete_file(self, path):
        """ Delete a file given its path. """
        return self.handle_request('DELETE', path)

    def modify_file_content(self, path, new_content, disk_file_path=None):
        """ Modify a file content given its path and new content. """
        if disk_file_path:
            with open(disk_file_path, 'w') as file:
                file.write(new_content)
        return self.handle_request('PUT', path, new_content)

    def is_directory(self, path):
        """ Check is arg path leads to a directory or not, returns bool accordingly"""
        return self.handle_request('CHECK', path)

    def is_file(self, path):
        """ Check is arg path leads to a file or not, returns bool accordingly"""
        try:
            info = self.get_file_info(path)
            if info['status'] == 200:
                return True
        except Exception:
            return False
