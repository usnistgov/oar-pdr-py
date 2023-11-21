"""
apiclient.py

This module provides a client class, FileManager, designed to interact with the File Manager Application Layer
which is a REST API offering file management capabilities. The client handles authentication, manages records,
scans files, and controls permissions.
"""

import requests


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
            - authentication_user (str): The username for authentication (Nextcloud instance superuser).
            - authentication_password (str): The password for authentication (Nextcloud instance superuser password).
        """
        self.base_url = config['base_url']
        self.auth_user = config['authentication_user']
        self.auth_pass = config['authentication_password']
        self.token = self.authenticate()

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
            raise Exception(data.get('message', 'Authentication failed'))
        else:
            raise Exception('Unknown error during authentication.')

    def headers(self):
        """
        Constructs headers for API requests, including the JWT token.

        Returns:
        - dict: Dictionary containing the headers.
        """
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
            raise Exception(error_msg)
        elif response.status_code >= 400:
            error_msg = response.json().get('message', 'API request failed')
            raise Exception(error_msg)

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
        Creates a record space for a given user and record name.
        Creates the user if it doesn't exist.
        Gives 'Share' permissions to user.

        Args:
        - user_name (str): The username associated with the record space.
        - record_name (str): The name of the record space to be created.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.post,
            f"{self.base_url}/record-space/{user_name}/{record_name}"
        )

    def get_record_space(self, record_name):
        """
        Retrieves details of a record space by its name.

        Args:
        - record_name (str): The name of the record space to be retrieved.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.get,
            f"{self.base_url}/record-space/{record_name}"
        )

    def delete_record_space(self, record_name):
        """
        Deletes a record space by its name.

        Args:
        - record_name (str): The name of the record space to be deleted.

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
        - record_name (str): The name of the record to be scanned.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.post,
            f"{self.base_url}/scans/{record_name}"
        )

    def get_scan_files(self, scan_id):
        """
        Retrieves the status and content of a scan by its scan ID.

        Args:
        - scan_id (str): The unique identifier of the scan.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.get,
            f"{self.base_url}/scans/id/{scan_id}"
        )

    def delete_scan_files(self, scan_id):
        """
        Delete the report of a scan by its scan ID.

        Args:
        - scan_id (str): The unique identifier of the scan.

        Returns:
        - dict: The parsed JSON response from the API.

        Raises:
        - Exception: If the API request results in an error.
        """
        return self.handle_request(
            requests.delete,
            f"{self.base_url}/scans/id/{scan_id}"
        )

    def manage_permissions(self, user_name, record_name, perm_type="No permissions (No access to the file or folder)",
                           method="POST"):
        """
        Manages permissions associated with a given user and record name.

        Args:
        - user_name (str): The username associated with the record.
        - record_name (str): The name of the record for which permissions are managed.
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
        url = f"{self.base_url}/permissions/{user_name}/{record_name}"
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
