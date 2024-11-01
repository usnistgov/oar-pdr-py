"""
This module provides a client class, NextcloudApi, designed to interact with the File Manager Generic Layer which is a REST API for Nextcloud.
"""
import json
import logging

import requests
import OpenSSL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class NextcloudApi:
    def __init__(self, config):
        self.is_prod = config.PROD
        if self.is_prod:
            self.base_url = config.NEXTCLOUD_API_PROD_URL + config.NEXTCLOUD_ADMIN_USER + '/'
        else:
            self.base_url = config.NEXTCLOUD_API_DEV_URL
        self.cert_path = config.CLIENT_CERT_PATH
        self.key_path = config.CLIENT_KEY_PATH
        self.ca_path = config.SERVER_CA_PATH
        self.nextcloud_superuser = config.NEXTCLOUD_ADMIN_USER

    def get_cert_cn(self, cert_path):
        """ Extract CN (Common Name) from the client certificate """
        with open(cert_path, 'rb') as cert_file:
            cert_data = cert_file.read()

        cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert_data)
        subject = cert.get_subject()
        return subject.CN

    def handle_request(self, method, url, **kwargs):
        """ Generic request handler. """
        # Certificate must have CN matching nextcloud admin username
        cert_cn = self.get_cert_cn(self.cert_path)
        if cert_cn != self.nextcloud_superuser:
            logging.error(f"CN '{cert_cn}' does not match the superuser name '{self.nextcloud_superuser}'.")
            raise Exception(f"CN '{cert_cn}' does not match the superuser name '{self.nextcloud_superuser}'.")

        full_url = f"{self.base_url}/{url}"
        try:
            response = requests.request(
                method,
                full_url,
                cert=(self.cert_path, self.key_path),
                verify=self.ca_path,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            err_msg = "Error occurred"
            if response.text:
                try:
                    err_msg = response.json().get('error', str(e))
                except json.JSONDecodeError:
                    err_msg = response.text
            logging.error(f"HTTP Error {response.status_code} for {method} {full_url}: {err_msg}")
            if response.status_code == 404:
                return response
            raise Exception(f"HTTP Error {response.status_code}: {err_msg}")

    def test(self):
        """ Test the API connection. """
        response = self.handle_request('GET', 'test')
        return response

    def headers(self):
        """ Fetch headers for debugging purposes. """
        response = self.handle_request('GET', 'headers')
        return response.json()

    def get_user_permissions(self, dir_name):
        """ Get all users permissions for a directory. """
        response = self.handle_request('GET', f'files/userpermissions/{dir_name}')
        return response.json()

    def set_user_permissions(self, user_name, perm_type, dir_name):
        """ Set user permissions for a directory. """
        response = self.handle_request('POST', f'files/userpermissions/{user_name}/{perm_type}/{dir_name}')
        return response.json()

    def delete_user_permissions(self, user_name, dir_name):
        """ Delete user permissions for a directory. """
        response = self.handle_request('DELETE', f'files/userpermissions/{user_name}/{dir_name}')
        return response.json()

    def scan_all_files(self):
        """ Trigger a scan for all files. """
        response = self.handle_request('PUT', 'files/scan')
        return response.json()

    def scan_user_files(self, user_name):
        """ Trigger a scan for all files from a user. """
        response = self.handle_request('PUT', f'files/scan/{user_name}')
        return response.json()

    def scan_directory_files(self, dir_path):
        """ Trigger a scan for all files inside a directory. """
        response = self.handle_request('PUT', f'files/scan/directory/{dir_path}')
        return response.text

    def get_users(self):
        """ Get all users. """
        response = self.handle_request('GET', 'files/users')
        return response.json()

    def get_user(self, user_name):
        """ Get a single user. """
        try:
            response = self.handle_request('GET', f'users/{user_name}')
            if response.status_code == 404:
                return {}
            return response.json()
        except Exception as e:
            logging.error(f"Error getting user {user_name}: {e}")
            return {}

    def create_user(self, user_name):
        """ Create a user. """
        return self.handle_request('POST', f'users/{user_name}')

    def disable_user(self, user_name):
        """ Disable a user. """
        return self.handle_request('PUT', f'users/{user_name}/disable')

    def enable_user(self, user_name):
        """ Enable a user. """
        return self.handle_request('PUT', f'users/{user_name}/enable')

    def is_user(self, user):
        """ Check is arg user is tied to an existing user or not, returns bool accordingly"""
        response = self.get_user(user)
        return bool(response)