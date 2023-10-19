"""
/permissions endpoint manages user permissions to a record space
"""
import xml.etree.ElementTree as ET

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils import files
from helpers import get_permissions_string, get_permissions_number, extract_failure_msgs, extract_permissions


class Permissions(Resource):

    @staticmethod
    def _parse_xml_response(response):
        # Join the list elements to form the XML string
        xml_response = "".join(response)

        # Parse XML response
        root = ET.fromstring(xml_response)
        return root

    @staticmethod
    def _get_users_permissions_from_xml(root):
        shares = root.findall('.//element')

        # Instantiate parsed dict
        users_permissions = {}

        # Iterate over shares
        for share in shares:
            user = share.find('share_with').text
            permissions = share.find('permissions').text
            users_permissions[user] = permissions

        return users_permissions

    @staticmethod
    def _check_response_message(root):
        response_message = root.find(".//message").text
        if response_message != 'OK':
            raise Exception(response_message)

    @jwt_required()
    def post(self, user_name, record_name, permission_type):
        try:
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                message = nextcloud_permission
                raise Exception(message)

            dir_name = f"mds2-{record_name}/mds2-{record_name}"

            # Ensure user has no permissions on record
            response = files.get_userpermissions(dir_name)
            root = self._parse_xml_response(response)
            self._check_response_message(root)
            users_permissions = self._get_users_permissions_from_xml(root)
            if user_name in users_permissions:
                raw_permissions = int(users_permissions[user_name])
                existing_permissions = get_permissions_string(raw_permissions)
                message = f"User {user_name} already has permissions {existing_permissions} on record space {record_name}!"
                raise Exception(message)

            response = files.post_userpermissions(user_name, nextcloud_permission, dir_name)

            failure_msgs = extract_failure_msgs(response)
            if failure_msgs != '':
                message = failure_msgs
                raise Exception(message)

            success_response = {
                'success': 'POST',
                'message': f"Created User '{user_name}' permissions '{permission_type}'  on directory '{record_name}' successfully!"
            }

            return success_response, 201

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400

    @jwt_required()
    def get(self, user_name, record_name):
        try:
            dir_name = f"mds2-{record_name}/mds2-{record_name}"
            response = files.get_userpermissions(dir_name)

            root = self._parse_xml_response(response)
            self._check_response_message(root)

            users_permissions = self._get_users_permissions_from_xml(root)

            # Extract user_name permissions
            if user_name in users_permissions:
                raw_permissions = int(users_permissions[user_name])
                user_permissions = get_permissions_string(raw_permissions)
            else:
                message = f"User {user_name} does not exist or does not have any permissions on record space {record_name}!"
                raise Exception(message)

            success_response = {
                'success': 'GET',
                'message': user_permissions
            }

            return success_response, 200

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400

    @jwt_required()
    def put(self, user_name, record_name, permission_type):
        try:
            nextcloud_permission = str(get_permissions_number(permission_type))
            if nextcloud_permission == 'Invalid permissions':
                message = nextcloud_permission
                raise Exception(message)

            dir_name = f"mds2-{record_name}/mds2-{record_name}"
            response = files.put_userpermissions(user_name, nextcloud_permission, dir_name)

            user_permissions = get_permissions_string(extract_permissions(response))
            if user_permissions != permission_type:
                # Store failure messages
                failure_msgs = extract_failure_msgs(response)

                if failure_msgs != '':
                    message = failure_msgs
                    raise Exception(message)

            success_response = {
                'success': 'PUT',
                'message': f"Updated User '{user_name}' permissions on directory '{record_name}' to '{permission_type}' successfully!"
            }

            return success_response, 200

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400

    @jwt_required()
    def delete(self, user_name, record_name):
        try:
            dir_name = f"mds2-{record_name}/mds2-{record_name}"
            users_permissions = files.delete_userpermissions(user_name, dir_name)

            if 'error' in users_permissions:
                message = 'User does not exist or does not have permissions or the file/folder does not exist'
                raise Exception(message)

            success_response = {
                'success': 'DELETE',
                'message': f"Deleted User '{user_name}' permissions on directory '{record_name}' successfully!"
            }

            return success_response, 200

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400
