"""
/test endpoint tests if API is working
"""

import logging

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.clients.nextcloud.api import NextcloudApi
from config import Config

logging.basicConfig(level=logging.INFO)


class Test(Resource):
    @jwt_required()
    def get(self):
        try:
            nextcloud_client = NextcloudApi(Config)
            response = nextcloud_client.test()
            if response is None:
                logging.error("Test resource not found")
                return {"error": "Not Found", "message": "Test resource not found"}, 404
            if response.status_code == 200:
                logging.info("Test resource retrieved successfully")
                return {'success': 'GET', 'message': response.text}, 200
            else:
                logging.error("Test resource error: " + response.text)
                return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            error_response = {
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred'
            }
            return error_response, 500
