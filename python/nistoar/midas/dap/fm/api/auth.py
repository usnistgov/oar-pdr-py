"""
/auth endpoint returns identification token necessary to use other API endpoints
"""
import logging

from flask import request
from flask_jwt_extended import create_access_token
from flask_restful import Resource

from config import Config

logging.basicConfig(level=logging.INFO)


class Authentication(Resource):
    __user = Config.API_USER
    __pwd = Config.API_PWD

    def post(self):
        try:
            user = request.json.get("user")
            pwd = request.json.get("pwd")

            if not user or not pwd:
                logging.error("Missing File Manager Superuser username or password in request")
                return {"error": "Missing credentials", "message": "Please provide both username and password"}, 400

            if user == self.__user and pwd == self.__pwd:
                access_token = create_access_token(identity=user)
                logging.info("File Manager Superuser authenticated successfully")
                success_response = {
                    'success': 'POST',
                    'message': access_token
                }
                return success_response, 200
            else:
                logging.error("Invalid credentials attempted")
                return {"error": "Unauthorized", "message": "Invalid credentials"}, 401

        except KeyError:
            logging.error("Invalid request format")
            return {"error": "Bad Request", "message": "Invalid request format"}, 400
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500
