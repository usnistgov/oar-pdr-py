"""
This module provides a client class, WebDAVClient, designed to perform WebDAV operations that interact with a Nextcloud instance to manage directories and files using the requests library.
"""
import logging
import os
import requests
import xml.etree.ElementTree as ET

from datetime import datetime
from urllib.parse import urlparse, unquote
from collections.abc import Mapping

from nistoar.base.config import ConfigurationException

class WebDAVApi:
    def __init__(self, config: Mapping, log: logging.Logger=None):
        """
        initialize the client

        :param dict config:  the configuration parameters for this client; see class documentation for 
                             the parameter descriptions.
        :param Logger log:   the Logger object to use for messages from this client.  If not provided,
                             a default logger with the name "nextcloudcli" will be used. 
        """
        if not log:
            log = logging.getLogger("webdavcli")
        self.log = log
        
        self.base_url = config.get("service_endpoint")
        if not self.base_url:
            raise ConfigurationException("WebDAVApi: Missing required config parameter: service_endpoint")
        self.authkw = self._prep_auth(config.get("authentication"))

        if config.get("ca_bundle"):
            self.authkw['verify'] = config['ca_bundle']

        self.temp_pwd = None
        self.auth = None

    def _prep_auth(self, authcfg):
        if not authcfg:
            self.log.warning("No authentication parameters provided; assuming none are needed")

        out = {}
        if authcfg.get("client_cert_path"):
            if not os.path.isfile(authcfg["client_cert_path"]):
                raise ConfigurationException(f"{authcfg['client_cert_path']}: client cert file not found")
            if not authcfg.get("client_key_path"):
                raise ConfigurationException("NextclouApi: missing required config parameter: "
                                             "authentication.client_key_path")
            if not os.path.isfile(authcfg["client_key_path"]):
                raise ConfigurationException(f"{authcfg['client_key_path']}: client key file not found")
            out['cert'] = (authcfg["client_cert_path"], authcfg["client_key_path"])

#            if authcfg.get("user"):
#                # Certificate must have CN matching nextcloud admin username
#                try:
#                    certuser = self._get_cert_cn(authcfg['client_cert_path'])
#                except Exception as ex:
#                    raise ConfigurationException("%s: trouble reading client cert: %s" %
#                                                 (authcfg['client_cert_path'], str(ex))) from ex
#
#                if authcfg['user'] != certuser:
#                    raise ConfigurationException("%s: CN does not match %s" %
#                                                 (authcfg['client_cert_path'], certuser))

        elif authcfg.get("user"):
            if not authcfg.get("pass"):
                raise ConfigurationException("NextclouApi: missing required config parameter: "
                                             "authentication.pass")
            out['auth'] = (authcfg['user'], authcfg['pass'])

        return out

    def authenticate(self):
        """Authenticate using the client certificate and get the temporary password from Nextcloud"""
        auth_url = f"{self.nextcloud_api_url}/auth"
        try:
            response = requests.post(auth_url, cert=(self.cert_path, self.key_path), verify=self.ca_path)
            response.raise_for_status()
            temp_password_data = response.json()
            self.temp_pwd = temp_password_data.get('temporary_password')
            self.auth = (self.api_user, self.temp_pwd)
            logging.info(f"Authenticated successfully. Temporary password: {self.temp_pwd}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error during authentication: {e}")
            raise Exception("Failed to authenticate with client certificate.")

    def parse_propfind_response(self, response_content):
        """Parse the PROPFIND XML response and extract file/folder info"""
        try:
            namespaces = {'d': 'DAV:'}
            tree = ET.fromstring(response_content)
            info = []
            for response in tree.findall('d:response', namespaces):
                href = response.find('d:href', namespaces).text
                parsed_url = urlparse(href)
                path = unquote(parsed_url.path)
                # Remove base path
                relative_path = path.replace(f"/remote.php/dav/files/{self.api_user}/", '', 1)

                propstat = response.find('d:propstat', namespaces)
                prop = propstat.find('d:prop', namespaces)
                resourcetype = prop.find('d:resourcetype', namespaces)
                isdir = resourcetype.find('d:collection', namespaces) is not None
                size_elem = prop.find('d:getcontentlength', namespaces)
                size = int(size_elem.text) if size_elem is not None else 0
                modified_elem = prop.find('d:getlastmodified', namespaces)
                modified = modified_elem.text if modified_elem is not None else ''
                info.append({
                    'path': relative_path,
                    'isdir': isdir,
                    'size': size,
                    'modified': modified
                })
            return info
        except Exception as e:
            logging.error(f"Error parsing PROPFIND response: {e}")
            return []

    def handle_request(self, method, target, content=None):
        """Generic request handler."""
        if self.auth is None:
            self.authenticate()

        try:
            path = f"/remote.php/dav/files/{self.api_user}/{target.lstrip('/')}"
            full_url = self.webdav_url + path

            if method == 'MKCOL':
                response = requests.request(method='MKCOL', url=full_url, cert=(self.cert_path, self.key_path),
                                            verify=self.ca_path, auth=self.auth)
                if response.status_code in [201, 200]:
                    logging.info(f"Directory created: {path}")
                    return {'status': response.status_code, 'message': 'Directory created'}
                else:
                    logging.error(f"Failed to create directory: {response.status_code} {response.reason}")
                    return {'status': response.status_code, 'message': 'Failed to create directory'}

            elif method == 'PROPFIND':
                headers = {'Depth': '1'}
                response = requests.request(method='PROPFIND', url=full_url, headers=headers,
                                            cert=(self.cert_path, self.key_path), verify=self.ca_path, auth=self.auth)
                if response.status_code == 207:
                    info = self.parse_propfind_response(response.content)
                    return {'status': response.status_code, 'info': info}
                else:
                    logging.error(f"Failed to get directory info: {response.status_code} {response.reason}")
                    return {'status': response.status_code, 'message': 'Failed to get directory info'}

            elif method == 'DELETE':
                response = requests.request(method='DELETE', url=full_url, cert=(self.cert_path, self.key_path),
                                            verify=self.ca_path, auth=self.auth)
                if response.status_code in [204, 200]:
                    logging.info(f"File/Folder deleted: {path}")
                    return {'status': response.status_code, 'message': 'File/Folder deleted'}
                else:
                    logging.error(f"Failed to delete File/Folder: {response.status_code} {response.reason}")
                    return {'status': response.status_code, 'message': 'Failed to delete File/Folder'}

            elif method == 'CHECK':
                headers = {'Depth': '0'}
                response = requests.request(method='PROPFIND', url=full_url, headers=headers,
                                            cert=(self.cert_path, self.key_path), verify=self.ca_path, auth=self.auth)
                if response.status_code == 207:
                    info = self.parse_propfind_response(response.content)
                    if info:
                        isdir = info[0]['isdir']
                        return isdir
                    else:
                        return False
                else:
                    return False

            elif method == 'PUT':
                if content is not None:
                    response = requests.request(method='PUT', url=full_url, data=content,
                                                cert=(self.cert_path, self.key_path), verify=self.ca_path,
                                                auth=self.auth)
                    if response.status_code in [200, 201, 204]:
                        logging.info(f"File uploaded/modified: {path}")
                        return {'status': response.status_code, 'message': 'File uploaded/modified'}
                    else:
                        logging.error(f"Failed to upload/modify file: {response.status_code} {response.reason}")
                        return {'status': response.status_code, 'message': 'Failed to upload/modify file'}
                else:
                    raise ValueError("Content must be provided to upload or modify a file!")

            elif method == 'GET':
                response = requests.request(method='GET', url=full_url, cert=(self.cert_path, self.key_path),
                                            verify=self.ca_path, auth=self.auth)
                if response.status_code == 200:
                    content = response.content.decode('utf-8')
                    logging.info(f"File content retrieved: {content}")
                    return {'status': response.status_code, 'content': content}
                else:
                    logging.error(f"Failed to get file content: {response.status_code} {response.reason}")
                    return {'status': response.status_code, 'message': 'Failed to get file content'}

            else:
                raise ValueError(f"Unsupported method: {method}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error occurred while handling request: {e}")
            return {'status': 500, 'message': "Uncaught error occurred while handling request."}
        except Exception as e:
            logging.error(f"Uncaught error occurred while handling request: {e}")
            return {'status': 500, 'message': "Uncaught error occurred while handling request."}

    def create_directory(self, path):
        """Create a directory given a path."""
        return self.handle_request('MKCOL', path)

    def get_directory_info(self, dir_path):
        """Get information on a directory given a path."""
        contents_info = self.get_contents_info(dir_path)
        dir_name = dir_path.rstrip('/').split("/")[-1]
        date_format = "%a, %d %b %Y %H:%M:%S %Z"
        total_size = 0
        date_times = []
        last_modified = None
        if len(contents_info) > 0:
            for content in contents_info:
                if content['size'] is None:
                    content['size'] = 0
                total_size += content['size']
                date_times.append(datetime.strptime(content['modified'], date_format))

            last_modified_date = max(date_times)
            last_modified = last_modified_date.strftime(date_format)
        else:
            dir_info = self.handle_request('PROPFIND', dir_path)
            if 'info' in dir_info and dir_info['info']:
                last_modified = dir_info['info'][0]['modified']

        dir_info = {
            'name': dir_name,
            'last_modified': last_modified,
            'total_size': total_size,
            'dir_info': contents_info
        }

        return dir_info

    def get_contents_info(self, dir_path):
        contents = self.handle_request('PROPFIND', dir_path)
        contents_info = []

        for content in contents.get('info', []):
            content['name'] = content['path'].rstrip('/').split('/')[-1]
            if content['isdir']:
                subdir_path = content['path'].split(f"{self.api_user}/", 1)[-1].rstrip('/')
                if subdir_path != dir_path.rstrip('/'):
                    content['size'] = 0
                    content['content_type'] = 'Directory'
                    subdir_contents = self.get_contents_info(subdir_path)
                    content['info'] = [sub_content for sub_content in subdir_contents if
                                       sub_content['path'].rstrip('/') != subdir_path]
                    for sub_content in content['info']:
                        if sub_content['size'] is not None:
                            content['size'] += int(sub_content['size'])
                    if subdir_path != dir_path.rstrip('/'):
                        content.pop('created', None)
                        contents_info.append(content)
            else:
                if content['size'] is not None:
                    content['size'] = int(content['size'])
                else:
                    content['size'] = 0
                content.pop('created', None)
                contents_info.append(content)

        return contents_info

    def delete_directory(self, path):
        """Delete a directory given a path."""
        return self.handle_request('DELETE', path)

    def upload_file(self, destination_path, file, filename=None):
        """Post a file given its content and the path where to create it."""
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
        """Get information on a file given a path."""
        return self.handle_request('PROPFIND', path)

    def get_file_content(self, path):
        """Get content of a file given a path."""
        return self.handle_request('GET', path)

    def delete_file(self, path):
        """Delete a file given its path."""
        return self.handle_request('DELETE', path)

    def modify_file_content(self, path, new_content, disk_file_path=None):
        """Modify a file content given its path and new content."""
        if disk_file_path:
            with open(disk_file_path, 'w') as file:
                file.write(new_content)
        return self.handle_request('PUT', path, new_content)

    def is_directory(self, path):
        """Check if arg path leads to a directory, returns bool accordingly"""
        return self.handle_request('CHECK', path)

    def is_file(self, path):
        """Check if arg path leads to a file, returns bool accordingly"""
        result = self.handle_request('CHECK', path)
        if result is False:
            return False
        else:
            return not result  # if result is True (i.e., isdir), then it's not a file
