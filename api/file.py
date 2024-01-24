"""
/file endpoint manages files in a user record space
"""
import logging

from flask import request
from flask_jwt_extended import jwt_required
from flask_restful import Resource

logging.basicConfig(level=logging.INFO)

from app.utils import files


class File(Resource):
    @jwt_required()
    def post(self, destination_path=''):
        try:
            # Check if destination directory exists
            if len(destination_path) > 0 and not files.is_directory(destination_path):
                logging.error(f"Directory '{destination_path}' does not exist")
                return {'error': 'Not Found', 'message': f"Directory '{destination_path}' does not exist"}, 404

            # Check for file in request
            if 'file' not in request.files:
                logging.error("No file part in the request")
                return {'error': 'Bad Request', 'message': 'No file part in the request'}, 400

            # Upload file
            file = request.files['file']

            # Check for file name
            if file.filename == '':
                logging.error("No file selected for uploading")
                return {'error': 'Bad Request', 'message': 'No file selected for uploading'}, 400

            files.post_file(file, destination_path)
            logging.info(f"Uploaded file '{file.filename}' to '{destination_path}'")

            success_response = {
                'success': 'POST',
                'message': f"Created file '{file.filename}' in '{destination_path}' successfully!"
            }

            return success_response, 201

        except Exception as error:
            logging.exception("An unexpected error occurred during file upload")
            return {'error': 'Internal Server Error', 'message': str(error)}, 500
