"""
Generic layer client for /files endpoints
"""

import os

import requests
from flask import current_app
from requests.auth import HTTPBasicAuth

from config import Config

generic_api_endpoint = f"{Config.NEXTCLOUD_API_DEV_URL}/files"


def is_directory(dir_name):
    response = get_directory(dir_name)
    return "HTTP/1.1 200 OK" in response[1]


def post_directory(dir_name):
    response = requests.post(f"{generic_api_endpoint}/directory/{dir_name}",
                             auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                             verify=Config.PROD)
    # response may not be json convertible
    if response.status_code == 200 and response.content:
        return response.json()
    return {'status': response.status_code}


def get_directory(dir_name):
    response = requests.get(f"{generic_api_endpoint}/directory/{dir_name}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def delete_directory(dir_name):
    response = requests.delete(f"{generic_api_endpoint}/directory/{dir_name}",
                               auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                               verify=Config.PROD)

    # response may not be json convertible
    if response.status_code == 200 and response.content:
        return response.json()
    return {'status': response.status_code}


def put_scan(user):
    response = requests.put(f"{generic_api_endpoint}/scan/{user}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def post_userpermissions(user, permissions, directory):
    response = requests.post(f"{generic_api_endpoint}/userpermissions/{user}/{permissions}/{directory}",
                             auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                             verify=Config.PROD)
    return response.json()


def get_userpermissions(directory):
    response = requests.get(f"{generic_api_endpoint}/userpermissions/{directory}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def put_userpermissions(user, permissions, directory):
    response = requests.put(f"{generic_api_endpoint}/userpermissions/{user}/{permissions}/{directory}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)

    # response may not be json convertible
    return response


def delete_userpermissions(user, directory):
    response = requests.delete(f"{generic_api_endpoint}/userpermissions/{user}/{directory}",
                               auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                               verify=Config.PROD)
    return response.json()


def post_file(file, directory_path, filename=None):
    """Uploads a file to a specified directory path.

        This function handles uploading by accepting either a file path
        or a file object and an optional filename for the uploaded file.

        Args:
            file (str|FileStorage): The file path or file object to upload.
            directory_path (str): The directory path where the file will be uploaded on the server.
            filename (str, optional): The filename to be used when uploading the file.
                                      If not provided and the file is a file object, file.filename is used.

        Returns:
            dict: A dictionary with the status code and content or json response from the server.

        Raises:
            ValueError: If filename is not provided and the file object doesn't have a 'filename' attribute.
        """
    # Determine whether file is a filepath or a file object
    if isinstance(file, str):
        with open(file, 'rb') as file_to_upload:
            files = {'file': (filename or os.path.basename(file), file_to_upload)}
            response = requests.post(
                f"{generic_api_endpoint}/file/{directory_path}",
                files=files,
                auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                verify=Config.PROD
            )
    else:
        # filename must be provided if file doesn't have a 'filename' attribute
        filename = filename or getattr(file, 'filename', None)
        if not filename:
            raise ValueError("Filename must be provided or file object must have a 'filename' attribute")
        files = {'file': (filename, file.stream)}
        response = requests.post(
            f"{generic_api_endpoint}/file/{directory_path}",
            files=files,
            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
            verify=Config.PROD
        )

    # Check if the response is JSON and return appropriately
    if response.headers.get('Content-Type') == 'application/json':
        return response.json()
    else:
        return {'status': response.status_code, 'content': response.content}


def get_file(file_path):
    response = requests.get(f"{generic_api_endpoint}/file/{file_path}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def put_file(json_data, file_path, disk_file_path):
    # Modify file in shared directory
    with open(disk_file_path, 'w') as file:
        file.write(json_data)

    # Modify file in Nextcloud directory
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.put(f"{generic_api_endpoint}/file/{file_path}",
                            data=json_data,
                            headers=headers,
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def delete_file(file_path):
    response = requests.delete(f"{generic_api_endpoint}/file/{file_path}",
                               auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                               verify=Config.PROD)
    return response.json()


def put_scandir(destination_path):
    response = requests.put(f"{generic_api_endpoint}/scan/directory/{destination_path}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()
