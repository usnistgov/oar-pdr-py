"""
/test endpoint unit test
"""

from http import HTTPStatus
from unittest.mock import patch

from dotenv import load_dotenv
from flask_testing import TestCase

from app import create_app
from config import Config


class TestPermissions(TestCase):
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

    @patch('app.utils.test.get_test')
    def test_get(self, mock_get_test):
        mock_get_test.return_value = ["GET", "", "api", "genapi.php", "test"]
        user_name = Config.API_USER
        pwd = Config.API_PWD
        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.get('/api/test', headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, ["GET", "", "api", "genapi.php", "test"])
