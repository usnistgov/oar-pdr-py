"""
This module provides a client class, NextcloudApi, designed to interact with the File Manager's 
Generic Layer API.  This API makes allows manipulation of file and folder permissions as well 
as manage file scanning operations.  This client is used by the MIDAS file manager layer to manage
DAP file spaces.  
"""
import json
import logging

import requests
import OpenSSL
import os
from collections.abc import Mapping

from nistoar.base.config import ConfigurationException
from ..exceptions import *

class NextcloudApi:
    """
    a client for the file manager's generic layer.  

    This class supports the following configuration parameters:
    
    ``service_endpoint``
        (str) _required_.  the base URL for the nextcloud's generic API layer.
    ``ca_bundle``
        (str) _optional_.  the path to a CA certificate bundle that should be used to validate the 
        remote server's site certificate.  If not provided, the CAs installed into the OS will be used. 
    ``authentication``
        (dict) _optional_. a dictionary containing the data required for authenticating to the service.
        If not provided, it will be assumed that authentication is not required (usually not the case).  
        See below for sub-parameter details.

    The ``authentication`` object supports the following sub-parameters:

    ``client_cert_path``
        (str) _optional_.  a file path to the client x509 certificate (in PEM format) that should be 
        used to connect to the service with.  
    ``client_key_path``
        (str) _optional_.  a file path to the private key (in PEM format) that matches the client x509 
        certificate given in ``client_cert_path``.
    ``user``
        (str) _optional_.  the user identity to connect to the service as.  If ``client_cert_path`` is 
        also given, it must match the common name (CN) for that certificate or a 
        :py:class:`~nistoar.base.config.ConfigurationException` will be raised.
    ``pass``
        (str) _optional_.  the password that can be used with the ``user`` to authenticate to the 
        service.  This password will be used instead if ``client_cert_path`` is not provided.  

    The ``authentication`` object (when required) must either set ``client_cert_path`` and 
    ``client_key_path`` or ``user`` and ``pass``, depending on the authentication method provided by 
    the service.
    """

    def __init__(self, config: Mapping, log: logging.Logger=None):
        """
        initialize the client

        :param dict config:  the configuration parameters for this client; see class documentation for 
                             the parameter descriptions.
        :param Logger log:   the Logger object to use for messages from this client.  If not provided,
                             a default logger with the name "nextcloudcli" will be used. 
        """
        if not log:
            log = logging.getLogger("nextcloudcli")
        self.log = log
        
        self.base_url = config.get("service_endpoint")
        if not self.base_url:
            raise ConfigurationException("NextclouApi: Missing required config parameter: service_endpoint")
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        self.authkw = self._prep_auth(config.get("authentication"))

        if not config.get('site_cert_verify', True):
            self.authkw['verify'] = False
        elif config.get("ca_bundle"):
            self.authkw['verify'] = config['ca_bundle']

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

            if authcfg.get("user"):
                # Certificate must have CN matching nextcloud admin username
                try:
                    certuser = self._get_cert_cn(authcfg['client_cert_path'])
                except Exception as ex:
                    raise ConfigurationException("%s: trouble reading client cert: %s" %
                                                 (authcfg['client_cert_path'], str(ex))) from ex

                if authcfg['user'] != certuser:
                    raise ConfigurationException("%s: CN does not match %s" %
                                                 (authcfg['client_cert_path'], certuser))

        elif authcfg.get("user"):
            if not authcfg.get("pass"):
                raise ConfigurationException("NextclouApi: missing required config parameter: "
                                             "authentication.pass")
            out['auth'] = (authcfg['user'], authcfg['pass'])

        return out

    def _get_cert_cn(self, cert_path):
        """ Extract CN (Common Name) from the client certificate """
        with open(cert_path, 'rb') as cert_file:
            cert_data = cert_file.read()

        cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert_data)
        subject = cert.get_subject()
        return subject.CN

    def _handle_request(self, method, url, **kwargs):
        """ Generic request handler. """

        full_url = f"{self.base_url}{url}"
        kw = dict(kwargs)
        kw.update(self.authkw)

        try:
            response = requests.request(
                method,
                full_url,
                **kw
            )

            err_msg = None
            if response.status_code >= 400 and response.text:
                try:
                    err_msg = response.json().get('error')
                except ValueError as ex:
                    pass

            if response.status_code >= 500:
                if err_msg:
                    err_msg = "File Manager Server Error: "+err_msg
                raise FileManagerServerError(response.status_code, url, response.text, err_msg)
            elif response.status_code == 404:
                raise FileManagerResourceNotFound(full_url, resptext=err_msg)
            elif response.status_code >= 400:
                raise FileManagerClientError(err_msg, response.status_code, url)
            elif response.status_code < 200 or response.status_code >= 300:
                if not err_msg:
                    err_msg = response.reason
                raise UnexpectedFileManagerResponse("Unexpected response (%d): %s" %
                                                    (response.status_code, err_msg))

            return response

        except requests.RequestException as ex:
            raise FileManagerCommError(str(ex), full_url) from ex

    def _get_json(self, method, url, **kwargs):
        response = self._handle_request(method, url, **kwargs)

        try:
            return response.json()
        except ValueError as ex:
            raise UnexpectedFileManagerResponse("File manager response could not be decoded " +
                                                "as decoded as JSON: " + str(ex)) from ex

    def test(self):
        """ Test the API connection. """
        return self._handle_request('GET', 'test')

    def headers(self):
        """ Fetch headers for debugging purposes. """
        return self._get_json('GET', 'headers')

    def get_user_permissions(self, dir_name):
        """ Get all users permissions for a directory. """
        return self._get_json('GET', f'files/userpermissions/{dir_name}')

    def set_user_permissions(self, user_name, perm_type, dir_name):
        """ Set user permissions for a directory. """
        return self._get_json('POST', f'files/userpermissions/{user_name}/{perm_type}/{dir_name}')

    def delete_user_permissions(self, user_name, dir_name):
        """ Delete user permissions for a directory. """
        return self._get_json('DELETE', f'files/userpermissions/{user_name}/{dir_name}')

    def scan_all_files(self):
        """ Trigger a scan for all files. """
        return self._get_json('PUT', 'files/scan')

    def scan_user_files(self, user_name):
        """ Trigger a scan for all files from a user. """
        return self._get_json('PUT', f'files/scan/{user_name}')

    def scan_directory_files(self, dir_path):
        """ Trigger a scan for all files inside a directory. """
        res = self._handle_request('PUT', f'files/scan/directory/{dir_path}')
        return res.text

    def get_users(self):
        """ Get all users. """
        return self._get_json('GET', 'files/users')

    def get_user(self, user_name):
        """ Get a single user. """
        try:
            return self._get_json('GET', f'users/{user_name}')
        except FileManagerResourceNotFound as ex:
            return {}
        except Exception as e:
            self.log.error(f"Error getting user {user_name}: {e}")
            return {}

    def create_user(self, user_name):
        """ Create a user. """
        return self._handle_request('POST', f'users/{user_name}')

    def disable_user(self, user_name):
        """ Disable a user. """
        return self._handle_request('PUT', f'users/{user_name}/disable')

    def enable_user(self, user_name):
        """ Enable a user. """
        return self._handle_request('PUT', f'users/{user_name}/enable')

    def is_user(self, user):
        """ Check is arg user is tied to an existing user or not, returns bool accordingly"""
        response = self.get_user(user)
        return bool(response)
