"""
Connect with generic layer /users endpoints
"""

import requests
from flask import current_app
from requests.auth import HTTPBasicAuth

from config import Config

generic_api_endpoint = f"{Config.NEXTCLOUD_API_DEV_URL}/users"


def is_user(user):
    response = get_user(user)
    return "user_id" in response


def get_users():
    response = requests.get(f"{generic_api_endpoint}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)
    return response.json()


def post_user(user):
    response = requests.post(f"{generic_api_endpoint}/{user}",
                             auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                             verify=Config.PROD)
    # Response is not json serializable
    return response


def get_user(user):
    response = requests.get(f"{generic_api_endpoint}/{user}",
                            auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
                            verify=Config.PROD)

    if response.content.decode() == "user not found":
        return "User does not exist"
    return response.json()
