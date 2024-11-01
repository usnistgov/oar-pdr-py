"""
/file endpoint unit tests
"""

import unittest
import io
from http import HTTPStatus
from unittest.mock import patch, Mock

from dotenv import load_dotenv
from flask_testing import TestCase

from app import create_app
from config import Config


class TestFile(TestCase):
    def create_app(self):
        load_dotenv(dotenv_path='../../../.env', override=True)
        app = create_app()
        app.config.from_object(Config)
        return app

    def get_test_jwt(self, user, pwd):
        response = self.client.post(
            '/api/auth',
            json={'user': user, 'pwd': pwd}
        )

        if response.status_code != HTTPStatus.OK:
            return None

        return response.json['message']

    @patch('app.api.file.files.post_file')
    @patch('app.api.file.files.is_directory', return_value=True)
    def test_post_file_success(self, mock_post_file, mock_is_directory):
        mock_file = Mock()
        mock_file.filename = 'testfile.txt'
        mock_file.stream = io.BytesIO(b"this is a test file")

        destination_path = 'test_path'
        user_name = Config.API_USER
        pwd = Config.API_PWD
        test_jwt = self.get_test_jwt(user_name, pwd)
        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt),
        }

        data = {
            'file': (mock_file.stream, mock_file.filename)
        }

        response = self.client.post(f'/api/file/{destination_path}', headers=headers, data=data)
        if response.status_code != 201:
            print(response.data)
        self.assertEqual(response.status_code, 201)

    @patch('app.api.file.files.is_directory', return_value=False)
    def test_post_directory_not_exist(self, mock_is_directory,):
        destination_path = 'non_existent_path'
        user_name = Config.API_USER
        pwd = Config.API_PWD
        test_jwt = self.get_test_jwt(user_name, pwd)
        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.post(f'/api/file/{destination_path}',
                                    headers=headers)
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Directory 'non_existent_path' does not exist",
                      response.data)


if __name__ == '__main__':
    unittest.main()
