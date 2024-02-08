"""
apiclient.py

This module provides a client class, FileManager, designed to interact with the File Manager Application Layer
which is a REST API offering file management capabilities. The client handles authentication, manages records,
scans files, and controls permissions.
"""
from urllib.parse import urlsplit, unquote, urljoin, urlparse

from nistoar.base.config import ConfigurationException
from . import webdav

import requests
from lxml import etree

__all__ = [ "FileManager", "FileSpaceException", "FileSpaceServerError", "FileSpaceConnectionError",
            "FileSpaceNotFound", "AuthenticationFailure" ]

class FileSpaceException(Exception):
    """
    an exception indicating a failure access a file space
    """

    def __init__(self, message: str, status: int=0, space_id: str=None):
        super(FileSpaceException, self).__init__(message)
        self.status = status
        self.space_id = space_id

class FileSpaceServerError(FileSpaceException):
    """
    an exception indicating that a failure occured in the remote file space server
    while trying to access the space.
    """
    pass

class FileSpaceConnectionError(FileSpaceServerError):
    """
    an exception indicating that a failure occured in the remote file space server
    while trying to access the space.
    """
    pass

class FileSpaceNotFound(FileSpaceException):
    """
    an exception reflecting access to a file space that does not exist in the file store
    """
    def __init__(self, space_id: str=None, message: str=None, status: int=404):
        if not message:
            message = f"{space_id}: " if space_id else ''
            message += "file space not found"
        super(FileSpaceNotFound, self).__init__(message, status, space_id)

class AuthenticationFailure(FileSpaceException):
    """
    an exception indicating that invalid authentication credentials were sent with a 
    request
    """
    def __init__(self, message: str=None, status: int=401):
        if not message:
            message = "Invalid authentication credentials"
        super(AuthenticationFailure, self).__init__(message, status)

    

class FileManager:
    """
    FileManager is a client class designed to interact with the File Manager
    Application Layer REST API that provides file management capabilities.
    It offers methods to authenticate, manage records, scan files, and handle
    permissions.

    Attributes:
    - base_url (str): The base URL for the API.
    - auth_user (str): The service username for basic authentication.
    - auth_pass (str): The service password for basic authentication.
    - token (str): The JWT token obtained after authentication.
    """

    def __init__(self, config):
        """
        Initializes the File Manager with configuration details.

        Args:
        - config (dict): A configuration dictionary with keys:
            - base_url (str): The base URL for the API.
            - auth.user (str): The username for authentication (Nextcloud instance superuser).
            - auth.password (str): The password for authentication (Nextcloud instance superuser password).
          The config may contain other keys that can provide other information to users of this 
          client via its cfg attribute
        """
        self.cfg = config
        self.base_url = config['dap_app_base_url'].rstrip('/')
        self.dav_base = config['dav_base_url'].rstrip('/')
        self.web_base = config.get('web_base_url', '').rstrip('/')
        authcfg = config.get('auth', {})
        if not authcfg.get('username') or not authcfg.get('password'):
            raise ConfigurationException("FileManager: Missing required config param: "+
                                         "auth.username and/or auth.password")
        self.auth_user = authcfg['username']
        self.auth_pass = authcfg['password']
        self.token = None

    def authenticate(self):
        """
        Authenticates the client using basic authentication and retrieves the JWT token.

        Returns:
        - str: The JWT token for subsequent requests.

        Raises:
        - Exception: If authentication fails or an unknown error occurs.
        """
        response = requests.post(
            f"{self.base_url}/auth",
            json={"user": self.auth_user, "pwd": self.auth_pass}
        )
        data = response.json()

        if 'message' in data and response.status_code == 200:
            return data['message']
        elif response.status_code == 401:
            raise AuthenticationFailure(data.get('message', 'Authentication failed'))
        else:
            raise FileSpaceException('Failed to obtain authentication token from server: '+
                                     response.reason, response.status_code)

    def headers(self):
        """
        Constructs headers for API requests, including the JWT token.

        Returns:
        - dict: Dictionary containing the headers.
        """
        if not self.token:
            self.token = self.authenticate()
        return {
            'Authorization': f"Bearer {self.token}"
        }

    def handle_request(self, method, url, **kwargs):
        """
        Sends an API request, handles potential errors, and re-authenticates if necessary.

        Args:
        - method (function): The HTTP method from the `requests` module (e.g., requests.get, requests.post).
        - url (str): The full endpoint URL.
        - **kwargs: Additional arguments to pass to the HTTP method (e.g., json, params).

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        response = method(url, headers=self.headers(), **kwargs)

        if response.status_code == 401:  # Expired token or authentication failure
            self.token = self.authenticate()  # Re-authenticate
            response = method(url, headers=self.headers(), **kwargs)  # Retry the request
        elif response.status_code == 400:  # Bad Request
            error_msg = response.json().get('message', 'API request failed with a Bad Request')
            raise FileSpaceException(error_msg)
        elif response.status_code == 404:  # Not Found
            error_msg = response.json().get('message', 'Record space not found')
            raise FileSpaceNotFound(message=error_msg)
        elif response.status_code >= 500:
            error_msg = response.json().get('message', response.reason)
            raise FileSpaceException(error_msg, response.status_code)
        elif response.status_code >= 400:
            error_msg = response.json().get('message', response.reason)
            raise FileSpaceException(error_msg, response.status_code)

        return response.json()

    def test(self):
        """
        Tests the connection to the API.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(requests.get, f"{self.base_url}/test")

    def create_record_space(self, user_name, record_name):
        """
        Creates a record space for a given user and unique record name.
        Creates the user if it doesn't exist.
        Gives 'Share' permissions to user.

        Args:
        - user_name (str): The username associated with the record space.
        - record_name (str): The name of the unique record space to be created.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.post,
            f"{self.base_url}/record-space/{record_name}/user/{user_name}"
        )

    def get_record_space(self, record_name):
        """
        Retrieves details of a record space by its name.

        Args:
        - record_name (str): The name of the unique record space to be retrieved.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        try:
            return self.handle_request(
                requests.get,
                f"{self.base_url}/record-space/{record_name}"
            )
        except FileSpaceNotFound as ex:
            if not ex.space_id:
                raise FileSpaceNotFound(record_name, str(ex))
            raise

    def get_uploads_directory(self, record_name):
        """
        return information about the uploads directory
        """
        path = f"{record_name}/{record_name}"
        url = f"{self.dav_base}/{path}"
        auth = (self.auth_user, self.auth_pass)
        header = {"Depth": "0", "Content-type": "application/xml"}

        try:
            resp = requests.request("PROPFIND", url, data=webdav.info_request,
                                    headers=header, auth=auth)
        except requests.RequestException as ex:
            raise FileSpaceConnectionError("Problem communicating with file manaager: "+str(ex))

        if resp.status_code == 401:  # Expired token or authentication failure
            raise AuthenticationFailure("File manager credentials not accepted")
        elif resp.status_code == 404:  # Not Found
            raise FileSpaceNotFound("Requested file-space (or uploads folder) does not exist")
        elif resp.status_code >= 500:
            raise FileSpaceException("File manager server error: {resp.reason}")
        elif resp.status_code >= 400:
            msg = resp.reason
            if '/json' in resp.headers.get("content-header"):
                body = response.json()
                if isinstance(body, Mapping) and 'message' in body:
                    msg = body['message']
            elif '/xml' in resp.headers.get("content-header"):
                try:
                    body = etree.parse(resp.text).getroot()
                    msgel = body.find(".//{DAV:}message")
                    if msgel:
                        msg = msgel.text
                except Exception as ex:
                    msg += " (no parseable message in response body)"
            raise FileSpaceException(msg, resp.status_code)

        base = self.dav_base
        if self.cfg.get('public_prefix'):
            base = urljoin(self.cfg['public_prefix'], urlparse(base).path.lstrip('/'))

        try:
            return webdav.parse_propfind(resp.text, path, self.dav_base)
        except webdav.RemoteResourceNotFound as ex:
            raise FileSpaceNotFound(record_name)
        except etree.XMLSyntaxError as ex:
            raise FileSpaceServerError("Server returned unparseable XML")


    def determine_uploads_url(self, record_name):
        """
        return the expected URL for the browser-based view of a record space's uploads directory.
        """
        # the nextcloud URL requires the directory's file ID
        fileid = self.get_uploads_directory(record_name).get('fileid')
        if not fileid:
            raise FileSpaceException(f"Unable to obtain the upload folder's ID for space={record_name}")
        return f"{self.web_base}/{fileid}?dir=/{record_name}/{record_name}"


    def delete_record_space(self, record_name):
        """
        Deletes a record space by its name.

        Args:
        - record_name (str): The name of the unique record space to be deleted.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.delete,
            f"{self.base_url}/record-space/{record_name}"
        )

    def post_scan_files(self, record_name):
        """
        Initiates a scan of all files for a given record name.

        Args:
        - record_name (str): The name of the unique record to be scanned.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.post,
            f"{self.base_url}/record-space/{record_name}/scan"
        )

    def get_scan_files(self, record_name, scan_id):
        """
        Retrieves the status and content of a scan by its scan ID.

        Args:
        - record_name (str): The name of the unique record scanned.
        - scan_id (str): The unique identifier of the scan.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.get,
            f"{self.base_url}/record-space/{record_name}/scan/{scan_id}"
        )

    def delete_scan_files(self, record_name, scan_id):
        """
        Delete the report of a scan by its scan ID.

        Args:
        - record_name (str): The name of the unique record to be scanned.
        - scan_id (str): The unique identifier of the scan.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.delete,
            f"{self.base_url}/record-space/{record_name}/scan/{scan_id}"
        )

    def manage_permissions(self, user_name, record_name, perm_type="No permissions (No access to the file or folder)",
                           method="POST"):
        """
        Manages permissions associated with a given user and unique record name.

        Args:
        - user_name (str): The username associated with the record.
        - record_name (str): The name of the unique record for which permissions are managed.
        - perm_type (str, optional): Permissions types are organized hierarchically from the weakest to the strongest.
        Each subsequent permission level includes the rights of the previous levels.
            - No permissions (No access to the file or folder)
            - Read
            - Write
            - Delete
            - Share
            - All
        - method (str): The HTTP method to use ("POST", "PUT", "GET", "DELETE"). Default is "POST".

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        url = f"{self.base_url}/record-space/{record_name}/user/{user_name}/permissions"
        if perm_type and method in ["POST", "PUT"]:
            url += f"/{perm_type}"

        request_method = getattr(requests, method.lower())
        return self.handle_request(
            request_method,
            url
        )

    def upload_file(self, file_obj, destination_path=''):
        """
        Upload a file in a given directory.
        If the directory doesn't exist, an error is thrown.

        Args:
        - file_obj (file-like object): The file to be uploaded.
        - destination_path (str, optional): The path to the directory where the file will be uploaded.
            Defaults to an empty string, which means the root directory.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        # Determine the correct URL based on the destination_path
        if destination_path:
            url = f"{self.base_url}/file/{destination_path}"
        else:
            url = f"{self.base_url}/file"

        # Use files parameter of requests to send file data
        files = {'file': file_obj}

        return self.handle_request(
            requests.post,
            url,
            files=files
        )
