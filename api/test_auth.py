"""
/auth endpoint unit tests
"""

import unittest

from dotenv import load_dotenv
from flask_testing import TestCase

from app import create_app
from config import Config


class TestAuthenticationResource(TestCase):
    def create_app(self):
        load_dotenv(dotenv_path='../../../.env', override=True)
        app = create_app()
        app.config.from_object(Config)
        return app

    def test_auth_post_success(self):
        response = self.client.post(
            '/api/auth',
            json={'user': Config.API_USER, 'pwd': Config.API_PWD}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn('success', response.json)
        self.assertIn('message', response.json)

    def test_auth_post_failure(self):
        response = self.client.post(
            '/api/auth',
            json={'user': 'wrong_user', 'pwd': 'wrong_pwd'}
        )
        self.assertEqual(response.status_code, 401)
        self.assertIn('error', response.json)
        self.assertEqual(response.json['message'], 'Invalid credentials')


if __name__ == '__main__':
    unittest.main()
