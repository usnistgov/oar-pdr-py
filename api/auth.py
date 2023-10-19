"""
/auth endpoint returns identification token necessary to use other API endpoints
"""
from flask import request
from flask_jwt_extended import create_access_token
from flask_restful import Resource

from config import Config


class Authentication(Resource):
    __user = Config.API_USER
    __pwd = Config.API_PWD

    def post(self):
        try:
            user = request.json.get("user")
            pwd = request.json.get("pwd")

            if user == self.__user and pwd == self.__pwd:
                access_token = create_access_token(identity=user)
                success_response = {
                    'success': 'POST',
                    'message': access_token
                }
                return success_response, 200
            else:
                message = "Invalid credentials"
                raise Exception(message)

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 401
