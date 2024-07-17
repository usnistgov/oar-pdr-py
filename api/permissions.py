"""
/permissions endpoint manages user permissions to a record space
"""
import logging
import xml.etree.ElementTree as ET

from flask_jwt_extended import jwt_required
from flask_restful import Resource

import helpers
from app.clients.nextcloud.api import NextcloudApi
from config import Config
from helpers import get_permissions_number

logging.basicConfig(level=logging.INFO)


class Permissions(Resource):

    @staticmethod
    def _parse_xml_response(response):
        try:
            xml_response = "".join(response)
            root = ET.fromstring(xml_response)
            return root
        except ET.ParseError as e:
            logging.exception(f"XML parsing error: {e}")
            raise ValueError("Invalid XML format")

    @staticmethod
    def _get_users_permissions_from_xml(root):
        try:
            shares = root.findall('.//element')
            users_permissions = {}
            for share in shares:
                user = share.find('share_with').text
                permissions = share.find('permissions').text
                users_permissions[user] = permissions
            return users_permissions
        except AttributeError as e:
            logging.exception(f"Error parsing XML: {e}")
            raise ValueError("Error in XML structure")

    @staticmethod
    def _check_response_message(root):
        response_message = root.find(".//message").text
        if response_message != 'OK':
            raise ValueError(response_message)

    @jwt_required()
    def post(self, user_name, record_name, permission_type):
        try:
            nextcloud_client = NextcloudApi(Config)
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                logging.error(f"Invalid permission type: {permission_type}")
                return {"error": "Bad Request", "message": "Invalid permission type"}, 400

            dir_name = f"{record_name}/{record_name}"

            # Create user if user_name does not exist
            if not nextcloud_client.is_user(user_name):
                user_creation_response = nextcloud_client.create_user(user_name)
                if not user_creation_response:
                    logging.error(f"Failed to create user '{user_name}'")
                    return {"error": "Internal Server Error", "message": "Failed to create user"}, 500
                else:
                    logging.info(f"Successfully created user '{user_name}'")

            # Ensure user has no permissions on record
            response = nextcloud_client.get_user_permissions(dir_name)
            dir_users = {}
            logging.info('REPONSE I AM LOOKING FOR')
            logging.info(response)
            if 'ocs' not in response:
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            for data in response['ocs']['data']:
                dir_users[data['share_with']] = helpers.get_permissions_string(data['permissions'])
            if user_name in dir_users:
                message = f"User {user_name} already has permissions {dir_users[user_name]} on record space {record_name}!"
                logging.error(message)
                return {"error": "Conflict", "message": message}, 409

            response = nextcloud_client.set_user_permissions(user_name, nextcloud_permission, dir_name)
            if response['ocs']['meta']['statuscode'] != 200:
                logging.error("Error setting user permissions")
                return {"error": "Internal Server Error", "message": "Failed to set user permissions"}, 500

            success_response = {
                'success': 'POST',
                'message': f"Created User '{user_name}' permissions '{permission_type}'  on directory '{record_name}' successfully!"
            }

            logging.info("Permissions successfully created")
            return success_response, 201

        except ValueError as ve:
            logging.exception(f"Value error: {str(ve)}")
            return {"error": "Bad Request", "message": str(ve)}, 400
        except Exception as error:
            logging.exception(f"Unexpected error: {str(error)}")
        return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def get(self, user_name, record_name):
        try:
            nextcloud_client = NextcloudApi(Config)
            dir_name = f"{record_name}/{record_name}"
            response = nextcloud_client.get_user_permissions(dir_name)

            dir_users = {}
            if 'ocs' not in response:
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            for data in response['ocs']['data']:
                dir_users[data['share_with']] = helpers.get_permissions_string(data['permissions'])
            if user_name not in dir_users:
                logging.error(f"User {user_name} does not exist or does not have permissions")
                return {"error": "Not Found",
                        "message": f"User {user_name} does not exist or does not have any permissions on record space {record_name}"}, 404

            success_response = {
                'success': 'GET',
                'message': dir_users[user_name]
            }

            logging.info(f"Permissions successfully retrieved: {dir_users[user_name]}")
            return success_response, 200
        except ValueError as ve:
            logging.exception(f"Value error: {ve}")
            return {"error": "Bad Request", "message": str(ve)}, 400
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def put(self, user_name, record_name, permission_type):
        try:
            nextcloud_client = NextcloudApi(Config)
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                logging.error("Invalid permission type provided for PUT")
                return {"error": "Bad Request", "message": nextcloud_permission}, 400

            if not nextcloud_client.is_user(user_name):
                logging.error(f"User '{user_name}' does not exist")
                return {"error": "User Not Found",
                        "message": f"Failed to retrieve user. User '{user_name}' does not exist"}, 404

            dir_name = f"{record_name}/{record_name}"
            nextcloud_client.delete_user_permissions(user_name, dir_name)
            response = nextcloud_client.set_user_permissions(user_name, nextcloud_permission, dir_name)
            if response['ocs']['meta']['statuscode'] != 200:
                logging.error("Error setting user permissions")
                return {"error": "Internal Server Error", "message": "Failed to set user permissions"}, 500

            success_response = {
                'success': 'PUT',
                'message': f"Updated User '{user_name}' permissions on directory '{record_name}' to '{permission_type}' successfully!"
            }

            logging.info(
                f"Updated User '{user_name}' permissions on directory '{record_name}' to '{permission_type}' successfully!")
            return success_response, 200
        except ValueError as ve:
            logging.exception(f"Value error in PUT: {ve}")
            return {"error": "Bad Request", "message": str(ve)}, 400
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def delete(self, user_name, record_name):
        try:
            nextcloud_client = NextcloudApi(Config)
            dir_name = f"{record_name}/{record_name}"
            response = nextcloud_client.delete_user_permissions(user_name, dir_name)

            if 'error' in response:
                logging.error("User does not exist or does not have permissions or the file/folder does not exist")
                return {"error": "Not Found",
                        "message": 'User does not exist or does not have permissions or the file/folder does not exist'}, 404

            success_response = {
                'success': 'DELETE',
                'message': f"Deleted User '{user_name}' permissions on directory '{record_name}' successfully!"
            }

            logging.info(f"Deleted User '{user_name}' permissions on directory '{record_name}' successfully!")
            return success_response, 200

        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500
