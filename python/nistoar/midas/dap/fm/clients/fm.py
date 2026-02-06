"""
a client to the File Manager intended for use by the MIDAS application for managing file spaces on 
behalf of DBIO/DAP users.
"""
import json
from collections.abc import Mapping
from typing import List
from urllib.parse import urlsplit, unquote, urljoin, urlparse

from nistoar.base.config import ConfigurationException
from ..exceptions import *

import requests

class FileManager:
    """
    FileManager is a client class designed to interact with the File Manager
    Application Layer REST API that provides file management capabilities.
    It offers methods to authenticate, manage records, scan files, and handle
    permissions.

    This client looks for the following parameters in the configuration passed in at 
    construction time:

    ``dap_app_base_url`` 
         (str) _required_.  the base URL to the MIDAS-DAP application layer API
    ``auth``
         (dict) _required_.  a dictionary containing authentication/authorization parameters 
         (see below).

    Authentication to the MIDAS-DAP application layer depends on the parameters provided in
    the ``auth`` config dictionary.  X.509 authentication is enabled (and should be 
    used when ``dap_app_base_url`` points to a public endpoint) if the following parameters are 
    provided:

    ``client_cert``
         (str) a file path to the client X.509 certificate (in PEM format) to use to authenticate 
         the service endpoint.  
    ``client_key``
         (str) a file path to the private key to the client certficate

    A username and password will be used to authenticate the client if the following parameters 
    appear (and the ``client_*`` do not):

    ``user``
         (str) the user name for the identity to authenticate in as
    ``pw``
         (str) _optional_.  the associated password to use.  

    This authentication method is only recommended when connecting to the service via a private 
    network, especially if a password is _not_ provided.  
    """

    def __init__(self, config: Mapping):
        """
        Initializes the File Manager with configuration details.

        :param dict config:  the configuration dictionary to use
        """
        self.cfg = config
        self.base_url = self.cfg.get('dap_app_base_url').rstrip('/')
        if not self.base_url:
            raise ConfigurationException("Missing required config param: dap_app_base_url")

        self.authcfg = config.get('auth', {})
        self.token = None

    def _get_token(self, token_ep: str, req_headers: Mapping = {}, **kwargs):
        """
        retrieve an authentication token via a means prescribed in the instance configuration.
        If this client is not configured for this type of configuration, None is returned.
        :raises NotAuthorized:  if authentication fails or the authenticated user is otherwise 
                                not authorized to access this service.
        """
        pass

    def _handle_request(self, meth="GET", resource: str=None, input: Mapping=None):
        respath = "/spaces"
        if resource:
            respath += f"/{resource.strip('/')}"
        url = self.base_url + respath

        output = None
        hdrs = {}
        if meth in ("POST", "PUT", "PATCH"):
            hdrs = { "Content-type": "application/json" }
        else:
            input = None

        # set up authentication
        kwargs = {}
        if self.authcfg.get('client_cert'):
            kwargs['cert'] = (self.authcfg['client_cert'], self.authcfg['client_key'])
        elif self.authcfg.get('user'):
            if self.authcfg.get('pw'):
                # username/password required
                kwargs['auth'] = (self.authcfg['user'], self.authcfg['pw'])
            else:
                # Assume the user is an admin account using an internal service
                hdrs.update({ "X_CLIENT_VERIFY": "SUCCESS", "X_CLIENT_CN": self.authcfg['user'] })

        if self.authcfg.get('token_service_endpoint'):
            # a separately retrieved token is required
            if not self.token:
                self.token = self._get_token(self.authcfg['token_service_endpoint'], hdrs, **kwargs)
            hdrs['Authentication'] = f"Bearer {self.token}"
            kwargs = {}

        try:
            resp = requests.request(meth, url, headers=hdrs, json=input, **kwargs)
            if meth in ["HEAD", "DELETE"]:
                output = resp.status_code >= 200 and resp.status_code < 300
            else:
                output = resp.json()

            if resp.status_code == 404:
                if not isinstance(output, bool):
                    message = "Resource not found (is base URL correct?)"
                    if id:
                        message = "Space not found"
                    raise FileManagerResourceNotFound(respath, message)

            elif resp.status_code == 401:
                raise FileManagerUserUnauthorized(respath)

            elif input and resp.status_code == 400:
                msg = f": {output['message']}" if output and output.get('message') else resp.reason
                raise FileManagerClientError(msg, 400, respath, resp.reason)

            elif input and resp.status_code == 409:
                msg = f": {output['message']}" if output and output.get('message') else resp.reason
                raise FileManagerOpConflict(msg)

            elif resp.status_code > 500:
                raise FileManagerServerError(resp.status_code, respath, resp.reason)

            elif resp.status_code < 200 or resp.status_code >= 300:
                msg = f": {output['message']}" if output and output.get('message') else resp.reason
                raise UnexpectedFileManagerResponse(msg, respath, resp.reason, resp.status_code)                

            return output

        except requests.exceptions.JSONDecodeError as ex:
            raise UnexpectedFileManagerResponse("Expected JSON response; got "+
                                                resp.text) from ex
            
        except requests.RequestException as ex:
            raise FileManagerCommError("Failed to connect to remote file manager service: "+
                                       str(ex)) from ex

    def create_space(self, id: str, foruser: str) -> Mapping:
        """
        create and set up the file space for a DAP record with the given ID.  

        :param str      id:  the identifier of the DAP being drafted that needs record space
        :param str foruser:  the identifier for the primary user of the space.  If this user does 
                             not known to nextcloud, it will be created.
        :return: a dictionary summarizing the current state of the newly created space.  See 
                 :py:meth:`summarize_space` for a description of the properties included in the 
                 summary.
                 :rtype:  dict
        """
        input = { "id": id, "for_user": foruser }
        return self._handle_request(meth="POST", input=input)

    def test(self) -> bool:
        """
        return True if the service is responsive
        """
        return self._handle_request("HEAD")

    def space_ids(self) -> List[str]:
        """
        return a list of the known spaces by their identifiers
        """
        return self._handle_request("GET")

    def space_exists(self, id: str) -> bool:
        """
        return True if the space with the given identifier has been created.
        """
        return self._handle_request("HEAD", id)

    def summarize_space(self, id: str) -> Mapping:
        """
        return a summary of the current state of the space with the given identifier.
        
        The value returned is a dictionary that includes the following properties:

        :rtype: dict
        """
        return self._handle_request("GET", id)

    def delete_space(self, id: str):
        """
        clean out the identified space making it inaccessible.
        """
        return self._handle_request("DELETE", id)

    def get_space_permissions(self, id: str):
        """
        return a mapping of access permissions that have been defined for the identified space.
        These permissions apply specifically to the uploads folder as a whole.
        :return:  a mapping of users to permission labels
                  :rtype: dict[str,str]
        """
        return self._handle_request("GET", id+"/perms")

    def set_space_permissions(self, id: str, perms: Mapping):
        """
        update the access permissions on a space's uploads folder for one or more users.  
        :param str     id:  the identifier of the space to update permissions for.
        :param dict perms:  a mapping of identifiers for the users whose permissions will be 
                            changed to labels for the new permissions they should be assigned.  
                            The supported labels are "None", "Read", "Write", "Delete", "Share",
                            and "All".
        :return:  a mapping of users to permission labels; this includes both users that were 
                  updated and those that weren't.
                  :rtype: dict[str,str]
        """
        return self._handle_request("PATCH", id+"/perms", input=perms)

    def last_scan_id(self, spaceid: str):
        """
        return the identifier for the last file scan requested.  This ID can be used to retrieve the 
        scan report (if it still exists).
        :param str spaceid:   the identifier for the space that has been scanned
        """
        summ = self.summarize_space(spaceid)
        return summ.get('last_scan_id')

    def get_scan(self, spaceid: str, scanid: str):
        """
        return the scan report for a particular identied scan.
        """
        return self._handle_request("GET", spaceid+"/scans/"+scanid)

    def delete_scan(self, spaceid: str, scanid: str):
        """
        delete the scan report for a particular identied scan.
        """
        return self._handle_request("DELETE", spaceid+"/scans/"+scanid)

    def start_scan(self, spaceid: str):
        """
        request a new scan to commence asynchronously.  This function will return an initial scan 
        report which includes a list of the files found in the space and some initial metadata for them.  
        One of the properties returned is ``scan_id`` which can be used to retrieve updated versions of 
        the report later.  The ``is_complete`` field, if False, indicates that the scan is still in 
        progress, meaning updates to the report are still expected.
        :param str spaceid:  the identifier for the space to scan.
        :return:  the initial version of the scan report.
                  :rtype: dict
        """
        return self._handle_request("POST", spaceid+"/scans", input={})




        


