"""
/test endpoint tests if API is working
"""

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils.test import get_test


class Test(Resource):
    @jwt_required()
    def get(self):
        try:
            return get_test()

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400
