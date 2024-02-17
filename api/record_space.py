"""
/record-space endpoint manages user record spaces
"""
import logging
import re
from datetime import datetime

from flask_jwt_extended import jwt_required
from flask_restful import Resource

import helpers
from app.utils import users, files

logging.basicConfig(level=logging.INFO)


class RecordSpace(Resource):
    @jwt_required()
    def post(self, user_name, record_name):
        try:
            # Instantiate dirs
            parent_dir = record_name
            system_dir = f"{parent_dir}/{record_name}-sys"
            user_dir = f"{parent_dir}/{record_name}"

            # Check if parent_dir exists
            if files.is_directory(parent_dir):
                logging.error(f"Record name '{record_name}' already exists")
                return {"error": "Conflict", "message": f"Record name '{record_name}' already exists!"}, 409

            # Create user if user_name does not exist
            if not users.is_user(user_name):
                user_creation_response = users.post_user(user_name)
                if not user_creation_response:
                    logging.error(f"Failed to create user '{user_name}'")
                    return {"error": "Internal Server Error", "message": "Failed to create user"}, 500

            # Create record space
            parent_dir_response = files.post_directory(parent_dir)
            system_dir_response = files.post_directory(system_dir)
            user_dir_response = files.post_directory(user_dir)

            # Check requests were successful
            if not all(response['status'] == 200 for response in
                       [parent_dir_response, system_dir_response, user_dir_response]):
                logging.error("Failed to create directories")
                return {"error": "Internal Server Error", "message": "Failed to create directories"}, 500

            # Share space with user
            permissions_response = files.post_userpermissions(user_name, 31, user_dir)
            status_code = helpers.extract_status_code(permissions_response)
            if not permissions_response or status_code != 200:
                logging.error(f"Failed to set permissions for user '{user_name}'")
                return {"error": "Internal Server Error", "message": "Failed to set user permissions"}, 500

            success_response = {
                'success': 'POST',
                'message': f"Created user '{user_name}' Record space '{record_name}' successfully!"
            }
            logging.info(f"Record space '{record_name}' created successfully for user '{user_name}'")
            return success_response, 201

        except Exception as error:
            logging.exception("An unexpected error occurred")
            return {"error": "Internal Server Error", "message": str(error)}, 500

    @jwt_required()
    def get(self, record_name):
        try:
            dir_name = f"{record_name}/{record_name}"
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
            size = str(int(size_matches[0]) / 1000) + 'KB' if size_matches else None

            if size is None or last_modified is None:
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            if size is None or last_modified is None:
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            dir_info = {
                'last_modified': last_modified,
                'total_size': size,
                'dir_info': response
            }

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
            logging.exception("An unexpected error occurred")
            return {"error": "Internal Server Error", "message": str(error)}, 500

    @jwt_required()
    def delete(self, record_name):
        try:
            dir_name = record_name
            response = files.delete_directory(dir_name)

            if 'status' in response:
                if response['status'] != 200:
                    logging.error(f"Failed to delete '{dir_name}' with status code {response['status']}")
                    return {"error": "Internal Server Error",
                            "message": f"Unknown error: status {str(response['status'])}"}, 500

            response = str(response)
            if 'error' in str(response):
                logging.error(f"Record name '{record_name}' does not exist")
                return {"error": "Not Found", "message": f"Record name '{record_name}' does not exist!"}, 404

            success_response = {
                'success': 'DELETE',
                'message': f"Record space '{record_name}' deleted successfully!"
            }
            logging.info(f"Record space '{record_name}' deleted successfully")
            return success_response, 200

        except Exception as error:
            logging.exception("An unexpected error occurred")
            return {"error": "Internal Server Error", "message": str(error)}, 500
