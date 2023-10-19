"""
Connect with generic layer /test endpoint
"""

import requests
from flask import current_app
from requests.auth import HTTPBasicAuth

from config import Config

generic_api_endpoint = f"{Config.NEXTCLOUD_API_DEV_URL}/test"


def get_test():
    # if dev mode: (no verify argument in prod, true by default)
    response = requests.get(
        f"{generic_api_endpoint}",
        auth=HTTPBasicAuth(current_app.config['API_USER'], current_app.config['API_PWD']),
        verify=Config.PROD
    )
    return response.json()
