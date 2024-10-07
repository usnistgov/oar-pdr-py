"""
/record-space endpoint manages user record spaces
"""
import logging
import re

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.clients.nextcloud.api import NextcloudApi
from app.clients.webdav.api import WebDAVApi
from config import Config

logging.basicConfig(level=logging.INFO)


class RecordSpace(Resource):
    @jwt_required()
    def post(self, user_name, record_name):
        try:
            logging.info(Config.NEXTCLOUD_API_DEV_URL)
            webdav_client = WebDAVApi(Config)
            nextcloud_client = NextcloudApi(Config)
            # Instantiate dirs
            parent_dir = record_name
            system_dir = f"{parent_dir}/{record_name}-sys"
            user_dir = f"{parent_dir}/{record_name}"

            # Check if parent_dir exists
            if webdav_client.is_directory(parent_dir):
                logging.error(f"Record name '{record_name}' already exists")
                return {"error": "Conflict", "message": f"Record name '{record_name}' already exists!"}, 409

            # Create user if user_name does not exist
            if not nextcloud_client.is_user(user_name):
                user_creation_response = nextcloud_client.create_user(user_name)
                if not user_creation_response:
                    logging.error(f"Failed to create user '{user_name}'")
                    return {"error": "Internal Server Error", "message": "Failed to create user"}, 500
                else:
                    logging.info(f"Successfully created user '{user_name}'")

            # Create record space
            parent_dir_response = webdav_client.create_directory(parent_dir)
            system_dir_response = webdav_client.create_directory(system_dir)
            user_dir_response = webdav_client.create_directory(user_dir)

            # Check requests were successful
            if not all(response['status'] == 200 for response in
                       [parent_dir_response, system_dir_response, user_dir_response]):
                logging.error("Failed to create directories")
                return {"error": "Internal Server Error", "message": "Failed to create directories"}, 500

            # Share space with user
            if user_name != Config.API_USER:
                permissions_response = nextcloud_client.set_user_permissions(user_name, 31, user_dir)
                if permissions_response['ocs']['meta']['statuscode'] != 200:
                    logging.error("Error setting user permissions")
                    return {"error": "Internal Server Error", "message": "Failed to set user permissions"}, 500

            success_response = {
                'success': 'POST',
                'message': f"Created user '{user_name}' Record space '{record_name}' successfully!"
            }
            logging.info(f"Record space '{record_name}' created successfully for user '{user_name}'")
            return success_response, 201

        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def get(self, record_name):
        try:
            webdav_client = WebDAVApi(Config)
            dir_name = f"{record_name}/{record_name}"
            if not webdav_client.is_directory(dir_name):
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            dir_info = webdav_client.get_directory_info(dir_name)

            success_response = {
                'success': 'GET',
                'message': dir_info
            }

            logging.info(f"Directory information retrieved for '{record_name}'")
            return success_response, 200

        except re.error as regex_error:
            logging.error(f"Regex error while processing response: {regex_error}")
            return {"error": "Internal Server Error", "message": "Failed to process directory information"}, 500
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def delete(self, record_name):
        try:
            webdav_client = WebDAVApi(Config)
            dir_name = record_name
            response = webdav_client.delete_directory(dir_name)

            if 'not found' in response['message']:
                logging.error(f"Record name '{record_name}' does not exist")
                return {"error": "Not Found", "message": f"Record name '{record_name}' does not exist!"}, 404
            elif response['status'] != 200:
                logging.error(f"Failed to delete '{dir_name}' with status code {response['status']}")
                return {"error": "Internal Server Error",
                        "message": "Unknown error"}, 500

            success_response = {
                'success': 'DELETE',
                'message': f"Record space '{record_name}' deleted successfully!"
            }
            logging.info(f"Record space '{record_name}' deleted successfully")
            return success_response, 200

        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500
