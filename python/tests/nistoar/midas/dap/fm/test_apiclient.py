import json
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from nistoar.midas.dap.fm.apiclient import FileManager

datadir = Path(__file__).parents[1] / "data"  # tests/nistoar/midas/dap/data
scanackf = datadir / "scan-req-ack.json"
scanrepf = datadir / "scan-report.json"

class FileManagerTest(test.TestCase):
    def setUp(self):
        self.config = {
            'dap_app_base_url': 'http://localhost:5000/api',
            'auth': {
                'username': 'service_api',
                'password': 'service_pwd'
            },
            'dav_base_url': 'http://localhost:8000/remote.php/dav/files/oar_api'
        }

        self.mock_response_200 = Mock()
        self.mock_response_200.status_code = 200

        # Mock the authenticate method to prevent the FileManager constructor
        # from making a real HTTP request upon object instantiation
        with patch.object(FileManager, 'authenticate', return_value='mock_token'):
            self.file_manager = FileManager(self.config)

    @patch('requests.post')
    def test_authenticate_success(self, mock_post):
        self.mock_response_200.json.return_value = {'message': 'test_token'}
        mock_post.return_value = self.mock_response_200

        file_manager = FileManager(self.config)
        token = file_manager.authenticate()

        self.assertEqual(token, 'test_token')

    @patch('requests.post')
    def test_authenticate_failure(self, mock_post):
        mock_response = Mock()
        mock_response.json.return_value = {'message': 'Authentication failed'}
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        with self.assertRaises(Exception) as context:
            file_manager = FileManager(self.config)
            file_manager.authenticate()

        self.assertTrue('Authentication failed' in str(context.exception))

    @patch('requests.get')
    def test_test_connection(self, mock_get):
        self.mock_response_200.json.return_value = {'message': 'connected'}
        mock_get.return_value = self.mock_response_200

        response = self.file_manager.test()

        self.assertEqual(response['message'], 'connected')

    @patch('requests.post')
    def test_create_record_space(self, mock_post):
        self.mock_response_200.json.return_value = {'message': 'record created'}
        mock_post.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        response = self.file_manager.create_record_space(user_name, record_name)

        self.assertEqual(response['message'], 'record created')

    @patch('requests.get')
    def test_get_record_space(self, mock_get):
        self.mock_response_200.json.return_value = {'record': 'details'}
        mock_get.return_value = self.mock_response_200

        record_name = "TestRecord"
        response = self.file_manager.get_record_space(record_name)

        self.assertEqual(response['record'], 'details')

    @patch('requests.delete')
    def test_delete_record_space(self, mock_delete):
        self.mock_response_200.json.return_value = {'message': 'record deleted'}
        mock_delete.return_value = self.mock_response_200

        record_name = "TestRecord"
        response = self.file_manager.delete_record_space(record_name)

        self.assertEqual(response['message'], 'record deleted')

    @patch('requests.post')
    def test_manage_permissions_post(self, mock_post):
        self.mock_response_200.json.return_value = {'message': 'permission updated'}
        mock_post.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        perm_type = "Write"
        response = self.file_manager.manage_permissions(user_name, record_name, perm_type, method="POST")

        self.assertEqual(response['message'], 'permission updated')

    @patch('requests.put')
    def test_manage_permissions_put(self, mock_put):
        self.mock_response_200.json.return_value = {'message': 'permission updated'}
        mock_put.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        perm_type = "Write"
        response = self.file_manager.manage_permissions(user_name, record_name, perm_type, method="PUT")

        self.assertEqual(response['message'], 'permission updated')

    @patch('requests.get')
    def test_manage_permissions_get(self, mock_get):
        self.mock_response_200.json.return_value = {'message': 'permission retrieved'}
        mock_get.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        response = self.file_manager.manage_permissions(user_name, record_name, method="GET")

        self.assertEqual(response['message'], 'permission retrieved')

    @patch('requests.delete')
    def test_manage_permissions_delete(self, mock_delete):
        self.mock_response_200.json.return_value = {'message': 'permission deleted'}
        mock_delete.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        response = self.file_manager.manage_permissions(user_name, record_name, method="DELETE")

        self.assertEqual(response['message'], 'permission deleted')

    @patch('requests.get')
    def test_get_scan_files(self, mock_get):
        with open(scanrepf) as fd:
            scanrep = json.load(fd)
        
        self.mock_response_200.json.return_value = {
            "success": "GET",
            "message": scanrep,
        }
        mock_get.return_value = self.mock_response_200

        task_id = "914e479a-e344-4152-a340-f62947d7adbd"
        response = self.file_manager.get_scan_files("TestRecord", task_id)

        self.assertTrue(response['message']["is_complete"])

    @patch('requests.post')
    def test_scan_files(self, mock_put):
        with open(scanackf) as fd:
            ackmsg = json.load(fd)
        self.mock_response_200.json.return_value = ackmsg
        mock_put.return_value = self.mock_response_200

        user_name = "TestUser"
        record_name = "TestRecord"
        response = self.file_manager.post_scan_files(record_name)

        self.assertEqual(response['message'], 'Scanning successfully started!')
        self.assertIn("scan_id", response)

    @patch('requests.post')
    def test_upload_file(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            'success': 'POST',
            'message': "Created file 'sample.txt' in '/path/to/dir' successfully!"
        }
        mock_post.return_value = mock_response

        # Invoke the upload_file method
        destination_path = "/path/to/dir"
        response = self.file_manager.upload_file(destination_path)

        # Verify that the response matches the expected message
        self.assertEqual(response['message'], "Created file 'sample.txt' in '/path/to/dir' successfully!")


if __name__ == '__main__':
    test.main()
