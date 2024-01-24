"""
/test endpoint tests if API is working
"""

import logging

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils.test import get_test

logging.basicConfig(level=logging.INFO)


class Test(Resource):
    @jwt_required()
    def get(self):
        try:
            response = get_test()
            if response is None:
                logging.error("Test resource not found")
                return {"error": "Not Found", "message": "Test resource not found"}, 404

            logging.info("Test resource retrieved successfully")
            return response, 200

        except Exception as error:
            logging.exception("An unexpected error occurred")
            error_response = {
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred: ' + str(error)
            }
            return error_response, 500
