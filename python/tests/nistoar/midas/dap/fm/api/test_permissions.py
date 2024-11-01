"""
/permissions endpoint unit tests
"""

import unittest
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

    @patch('app.api.permissions.files.post_userpermissions')
    def test_post(self, mock_post_userpermissions):
        mock_post_userpermissions.return_value = ['<?xml version="1.0"?>', '<ocs>', ' <meta>', '  <status>ok</status>',
                                                  '  <statuscode>200</statuscode>', '  <message>OK</message>',
                                                  ' </meta>', ' <data>', '  <id>46</id>',
                                                  '  <share_type>0</share_type>', '  <uid_owner>oar_api</uid_owner>',
                                                  '  <displayname_owner>oar_api</displayname_owner>',
                                                  '  <permissions>1</permissions>', '  <can_edit>1</can_edit>',
                                                  '  <can_delete>1</can_delete>', '  <stime>1689791721</stime>',
                                                  '  <parent/>', '  <expiration/>', '  <token/>',
                                                  '  <uid_file_owner>oar_api</uid_file_owner>', '  <note></note>',
                                                  '  <label/>',
                                                  '  <displayname_file_owner>oar_api</displayname_file_owner>',
                                                  '  <path>/mds2-recordTest5/mds2-recordTest5</path>',
                                                  '  <item_type>folder</item_type>',
                                                  '  <mimetype>httpd/unix-directory</mimetype>',
                                                  '  <has_preview></has_preview>',
                                                  '  <storage_id>home::oar_api</storage_id>', '  <storage>1</storage>',
                                                  '  <item_source>281</item_source>',
                                                  '  <file_source>281</file_source>',
                                                  '  <file_parent>234</file_parent>',
                                                  '  <file_target>/mds2-recordTest5</file_target>',
                                                  '  <share_with>userTest</share_with>',
                                                  '  <share_with_displayname>userTest</share_with_displayname>',
                                                  '  <share_with_displayname_unique>userTest</share_with_displayname_unique>',
                                                  '  <status/>', '  <mail_send>0</mail_send>',
                                                  '  <hide_download>0</hide_download>', '  <attributes/>', ' </data>',
                                                  '</ocs>']

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'
        permission_type = 'Read'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.post('/api/permissions/{}/{}/{}'.format(user_name, record_name, permission_type),
                                    headers=headers, json={'permission_type': permission_type})

        self.assertEqual(response.status_code, 201)

    @patch('app.api.permissions.files.get_userpermissions')
    def test_get(self, mock_get_userpermissions):
        mock_get_userpermissions.return_value = ['<?xml version="1.0"?>', '<ocs>', ' <meta>', '  <status>ok</status>',
                                                 '  <statuscode>200</statuscode>', '  <message>OK</message>',
                                                 ' </meta>', ' <data>', '  <element>', '   <id>46</id>',
                                                 '   <share_type>0</share_type>', '   <uid_owner>oar_api</uid_owner>',
                                                 '   <displayname_owner>oar_api</displayname_owner>',
                                                 '   <permissions>1</permissions>', '   <can_edit>1</can_edit>',
                                                 '   <can_delete>1</can_delete>', '   <stime>1689791721</stime>',
                                                 '   <parent/>', '   <expiration/>', '   <token/>',
                                                 '   <uid_file_owner>oar_api</uid_file_owner>', '   <note></note>',
                                                 '   <label/>',
                                                 '   <displayname_file_owner>oar_api</displayname_file_owner>',
                                                 '   <path>/mds2-recordTest5/mds2-recordTest5</path>',
                                                 '   <item_type>folder</item_type>',
                                                 '   <mimetype>httpd/unix-directory</mimetype>',
                                                 '   <has_preview></has_preview>',
                                                 '   <storage_id>home::oar_api</storage_id>', '   <storage>1</storage>',
                                                 '   <item_source>281</item_source>',
                                                 '   <file_source>281</file_source>',
                                                 '   <file_parent>234</file_parent>',
                                                 '   <file_target>/mds2-recordTest5</file_target>',
                                                 '   <share_with>oar_api</share_with>',
                                                 '   <share_with_displayname>oar_api</share_with_displayname>',
                                                 '   <share_with_displayname_unique>oar_api</share_with_displayname_unique>',
                                                 '   <status/>', '   <mail_send>0</mail_send>',
                                                 '   <hide_download>0</hide_download>', '   <attributes/>',
                                                 '  </element>', ' </data>', '</ocs>']

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.get('/api/permissions/{}/{}'.format(user_name, record_name), headers=headers)
        response_data = response.data.decode()
        expected_response = '{"success": "GET", "message": "Read"}\n'

        self.assertEqual(response_data, expected_response)

    @patch('app.api.permissions.files.put_userpermissions')
    def test_put(self, mock_put_userpermissions):
        mock_put_userpermissions.return_value = ["<?xml version=\"1.0\"?>", "<ocs>", " <meta>",
                                                 "  <status>ok<\/status>", "  <statuscode>200<\/statuscode>",
                                                 "  <message>OK<\/message>", " <\/meta>", " <data>", "  <id>52<\/id>",
                                                 "  <share_type>0<\/share_type>", "  <uid_owner>oar_api<\/uid_owner>",
                                                 "  <displayname_owner>oar_api<\/displayname_owner>",
                                                 "  <permissions>1<\/permissions>", "  <can_edit>1<\/can_edit>",
                                                 "  <can_delete>1<\/can_delete>", "  <stime>1689882881<\/stime>",
                                                 "  <parent\/>", "  <expiration\/>", "  <token\/>",
                                                 "  <uid_file_owner>oar_api<\/uid_file_owner>", "  <note><\/note>",
                                                 "  <label\/>",
                                                 "  <displayname_file_owner>oar_api<\/displayname_file_owner>",
                                                 "  <path>\/mds2-test_record\/mds2-test_record<\/path>",
                                                 "  <item_type>folder<\/item_type>",
                                                 "  <mimetype>httpd\/unix-directory<\/mimetype>",
                                                 "  <has_preview><\/has_preview>",
                                                 "  <storage_id>home::oar_api<\/storage_id>", "  <storage>1<\/storage>",
                                                 "  <item_source>281<\/item_source>",
                                                 "  <file_source>281<\/file_source>",
                                                 "  <file_parent>234<\/file_parent>",
                                                 "  <file_target>\/mds2-test_record<\/file_target>",
                                                 "  <share_with>oar_api<\/share_with>",
                                                 "  <share_with_displayname>oar_api<\/share_with_displayname>",
                                                 "  <share_with_displayname_unique>oar_api<\/share_with_displayname_unique>",
                                                 "  <status\/>", "  <mail_send>1<\/mail_send>",
                                                 "  <hide_download>0<\/hide_download>", "  <attributes\/>", " <\/data>",
                                                 "<\/ocs>"]

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'
        permission_type = 'Read'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.put('/api/permissions/{}/{}/{}'.format(user_name, record_name, permission_type),
                                   headers=headers, json={'permission_type': permission_type})

        self.assertEqual(response.status_code, 200)

    @patch('app.api.permissions.files.delete_userpermissions')
    def test_delete(self, mock_delete_userpermissions):
        mock_delete_userpermissions.return_value = {'status': HTTPStatus.OK}

        user_name = Config.API_USER
        pwd = Config.API_PWD
        record_name = 'test_record'

        test_jwt = self.get_test_jwt(user_name, pwd)

        if test_jwt is None:
            self.fail('Authentication failed during test setup')

        headers = {
            'Authorization': 'Bearer {}'.format(test_jwt)
        }

        response = self.client.delete('/api/permissions/{}/{}'.format(user_name, record_name), headers=headers)

        self.assertEqual(response.status_code, 200)


if __name__ == '__main__':
    unittest.main()
