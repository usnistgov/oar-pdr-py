"""
/record-space endpoint manages user record spaces
"""
import re
from datetime import datetime

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils import users, files


class RecordSpace(Resource):
    @jwt_required()
    def post(self, user_name, record_name):
        try:
            # Instantiate dirs
            parent_dir = f"mds2-{record_name}"
            system_dir = f"{parent_dir}/mds2-{record_name}-sys"
            user_dir = f"{parent_dir}/mds2-{record_name}"

            # Check if parent_dir exists
            if files.is_directory(parent_dir):
                message = f"Record name '{record_name}' already exists!'"
                raise Exception(message)

            # Create user if user_name does not exist
            if not users.is_user(user_name):
                users.post_user(user_name)

            # Create record space
            parent_dir_response = files.post_directory(parent_dir)
            system_dir_response = files.post_directory(system_dir)
            user_dir_response = files.post_directory(user_dir)

            # Check requests were successful
            if not (parent_dir_response == system_dir_response == user_dir_response and system_dir_response[
                'status'] == 200):
                message = f"parent_dir_response: '{parent_dir_response}',\n" \
                          f"system_dir_response: '{system_dir_response}',\n" \
                          f"user_dir_response: '{user_dir_response}'"
                raise Exception(message)

            # Share space with user
            response = files.post_userpermissions(user_name, 16, user_dir)

            success_response = {
                'success': 'POST',
                'message': f"Created user '{user_name}' Record space '{record_name}' successfully!"
            }

            return success_response, 201

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400

    @jwt_required()
    def get(self, record_name):
        try:
            dir_name = f"mds2-{record_name}/mds2-{record_name}"
            response = files.get_directory(dir_name)
            response = ''.join(response)

            # Extract last modified date
            last_modified_matches = re.findall(r'<d:getlastmodified>(.+?)<\/d:getlastmodified>', response)
            if last_modified_matches:
                date_objects = [datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z') for date in last_modified_matches]
                most_recent_date = max(date_objects)
                last_modified = most_recent_date.strftime('%a, %d %b %Y %H:%M:%S %Z')
            else:
                last_modified = None

            # Extract directory size
            size_matches = re.findall(r'<d:quota-used-bytes>(\d+)<\/d:quota-used-bytes>', response)
            if size_matches:
                size = str(int(size_matches[0]) / 1000) + 'KB'
            else:
                size = None

            if size is None or last_modified is None:
                message = f"Record name '{record_name}' does not exist!'"
                raise Exception(message)

            dir_info = {
                'last_modified': last_modified,
                'total_size': size,
                'dir_info': response
            }

            success_response = {
                'success': 'GET',
                'message': dir_info
            }
            return success_response, 200

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 404

    @jwt_required()
    def delete(self, record_name):
        try:
            dir_name = f"mds2-{record_name}"
            response = files.delete_directory(dir_name)

            if 'status' in response:
                if response['status'] != 200:
                    message = f"Unknown error: status {str(response['status'])}"
                    raise Exception(message)

            response = str(response)
            if 'error' in response:
                message = f"Record name '{record_name}' does not exist!'"
                raise Exception(message)

            success_response = {
                'success': 'DELETE',
                'message': f"Record space '{record_name}' deleted successfully!"
            }
            return success_response, 200

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400
