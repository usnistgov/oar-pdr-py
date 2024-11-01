"""
/record-space endpoint unit tests
"""

import unittest
from http import HTTPStatus
from unittest.mock import patch

from dotenv import load_dotenv
from flask_testing import TestCase

from app import create_app
from config import Config


class TestRecordSpace(TestCase):
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

    @patch('app.api.record_space.files.is_directory')
    @patch('app.api.record_space.users.is_user')
    @patch('app.api.record_space.files.post_directory')
    @patch('app.api.record_space.files.post_userpermissions')
    def test_post(self, mock_post_userpermissions, mock_post_directory, mock_is_user, mock_is_directory):
        mock_is_directory.return_value = False
        mock_is_user.return_value = True
        mock_post_directory.return_value = {'status': HTTPStatus.OK}
        mock_post_userpermissions.return_value = {'status': HTTPStatus.CREATED}

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.post('/api/record-space/{}/{}'.format(user_name, record_name), headers=headers)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json, {
            'success': 'POST',
            'message': f"Created user '{user_name}' Record space '{record_name}' successfully!"
        })

    @patch('app.api.record_space.files.get_directory')
    def test_get(self, mock_get_directory):
        mock_get_directory.return_value = ['<?xml version="1.0"?>',
                                           '<d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns"><d:response><d:href>/remote.php/dav/files/oar_api/mds2-recordTest5/mds2-recordTest5/</d:href><d:propstat><d:prop><d:getlastmodified>Tue, 18 Jul 2023 19:29:36 GMT</d:getlastmodified><d:resourcetype><d:collection/></d:resourcetype><d:quota-used-bytes>140460</d:quota-used-bytes><d:quota-available-bytes>-3</d:quota-available-bytes><d:getetag>&quot;64b6e82082349&quot;</d:getetag></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response><d:response><d:href>/remote.php/dav/files/oar_api/mds2-recordTest5/mds2-recordTest5/boxplot_cno.png</d:href><d:propstat><d:prop><d:getlastmodified>Wed, 25 Jan 2023 14:37:30 GMT</d:getlastmodified><d:getcontentlength>38040</d:getcontentlength><d:resourcetype/><d:getetag>&quot;bcc1e6a15c96c423c91c86dccaab83b7&quot;</d:getetag><d:getcontenttype>image/png</d:getcontenttype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response><d:response><d:href>/remote.php/dav/files/oar_api/mds2-recordTest5/mds2-recordTest5/test/</d:href><d:propstat><d:prop><d:getlastmodified>Tue, 18 Jul 2023 19:05:55 GMT</d:getlastmodified><d:resourcetype><d:collection/></d:resourcetype><d:quota-used-bytes>102420</d:quota-used-bytes><d:quota-available-bytes>-3</d:quota-available-bytes><d:getetag>&quot;64b6e2930f0e6&quot;</d:getetag></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>']

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.get('/api/record-space/{}'.format(record_name), headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['success'], 'GET')

    @patch('app.api.record_space.files.delete_directory')
    def test_delete(self, mock_delete_directory):
        mock_delete_directory.return_value = {'status': 200}

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.delete('/api/record-space/{}'.format(record_name), headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['success'], 'DELETE')
        self.assertEqual(response.json['message'], f"Record space '{record_name}' deleted successfully!")


if __name__ == '__main__':
    unittest.main()
