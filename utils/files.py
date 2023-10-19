"""
Connect with generic layer /files endpoints
"""

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
