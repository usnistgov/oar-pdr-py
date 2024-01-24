"""
/permissions endpoint manages user permissions to a record space
"""
import logging
import xml.etree.ElementTree as ET

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils import users, files
from helpers import get_permissions_string, get_permissions_number, extract_failure_msgs, extract_permissions

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
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                logging.error(f"Invalid permission type: {permission_type}")
                return {"error": "Bad Request", "message": "Invalid permission type"}, 400

            dir_name = f"{record_name}/{record_name}"

            # Create user if user_name does not exist
            if not users.is_user(user_name):
                users.post_user(user_name)

            # Ensure user has no permissions on record
            response = files.get_userpermissions(dir_name)
            root = self._parse_xml_response(response)
            self._check_response_message(root)
            users_permissions = self._get_users_permissions_from_xml(root)
            if user_name in users_permissions:
                raw_permissions = int(users_permissions[user_name])
                existing_permissions = get_permissions_string(raw_permissions)
                message = f"User {user_name} already has permissions {existing_permissions} on record space {record_name}!"
                logging.error(message)
                return {"error": "Conflict", "message": message}, 409

            response = files.post_userpermissions(user_name, nextcloud_permission, dir_name)
            failure_msgs = extract_failure_msgs(response)
            if failure_msgs != '':
                logging.error("Error setting user permissions")
                raise Exception(failure_msgs)

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
            dir_name = f"{record_name}/{record_name}"
            response = files.get_userpermissions(dir_name)

            root = self._parse_xml_response(response)
            self._check_response_message(root)

            users_permissions = self._get_users_permissions_from_xml(root)

            if user_name not in users_permissions:
                logging.error(f"User {user_name} does not exist or does not have permissions")
                return {"error": "Not Found",
                        "message": f"User {user_name} does not exist or does not have any permissions on record space {record_name}"}, 404

            raw_permissions = int(users_permissions[user_name])
            user_permissions = get_permissions_string(raw_permissions)

            success_response = {
                'success': 'GET',
                'message': user_permissions
            }

            logging.info(f"Permissions successfully retrieved: {users_permissions}")
            return success_response, 200

        except ValueError as ve:
            logging.exception(f"Value error: {ve}")
            return {"error": "Bad Request", "message": str(ve)}, 400
        except Exception as error:
            logging.exception("An unexpected error occurred in GET")
            return {"error": "Internal Server Error", "message": str(error)}, 500

    @jwt_required()
    def put(self, user_name, record_name, permission_type):
        try:
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                logging.error("Invalid permission type provided for PUT")
                return {"error": "Bad Request", "message": nextcloud_permission}, 400

            dir_name = f"{record_name}/{record_name}"
            response = files.put_userpermissions(user_name, nextcloud_permission, dir_name)

            user_permissions = get_permissions_string(extract_permissions(response))
            if user_permissions != permission_type:
                # Store failure messages
                failure_msgs = extract_failure_msgs(response)

                if failure_msgs != '':
                    logging.error("Expected user permissions are different from the actual permissions")
                    raise ValueError(failure_msgs)

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
            logging.exception("An unexpected error occurred in PUT")
            return {"error": "Internal Server Error", "message": str(error)}, 500

    @jwt_required()
    def delete(self, user_name, record_name):
        try:
            dir_name = f"{record_name}/{record_name}"
            users_permissions = files.delete_userpermissions(user_name, dir_name)

            if 'error' in users_permissions:
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
            logging.exception("An unexpected error occurred in DELETE")
            return {"error": "Internal Server Error", "message": str(error)}, 500
