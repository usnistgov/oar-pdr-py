"""
This module provides a client class, NextcloudApi, designed to interact with the File Manager Generic Layer which is a REST API for Nextcloud.
"""
import json
import logging

import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class NextcloudApi:
    def __init__(self, config):
        self.is_prod = config.PROD
        if self.is_prod:
            self.base_url = config.NEXTCLOUD_API_PROD_URL + config.API_USER + '/'
        else:
            self.base_url = config.NEXTCLOUD_API_DEV_URL
        self.auth_user = config.API_USER
        self.auth_pass = config.API_PWD

    def handle_request(self, method, url, **kwargs):
        """ Generic request handler. """
        full_url = f"{self.base_url}/{url}"
        try:
            auth = HTTPBasicAuth(self.auth_user, self.auth_pass)
            response = requests.request(method, full_url, auth=auth, verify=self.is_prod, **kwargs)
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
