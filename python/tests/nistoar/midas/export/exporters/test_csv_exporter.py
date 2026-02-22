import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
from nistoar.midas.export.exporters.csv_exporter import CSVExporter


class TestCSVExporter(unittest.TestCase):

    def setUp(self):
        self.test_data = [
            {
                'id': 'mdm1-test1',
                'name': 'Test DMP 1',
                'type': 'dmp',
                'owner': {'fn': 'John Doe'},
                'meta': {
                    'status': 'active', 
                    'creationDate': '2023-01-01',
                    'lastModified': '2023-01-15'
                },
                'data': {
                    'title': 'Test Data Management Plan',
                    'description': 'This is a test DMP',
                    'contact': {'fn': 'Jane Smith'},
                    'keyword': ['test', 'data', 'research'],
                    'dataProduct': [{'title': 'Dataset 1'}, {'title': 'Dataset 2'}]
                }
            }
        ]
        self.exporter = CSVExporter()

    @patch('nistoar.midas.export.exporters.csv_exporter.preppy')
    def test_render_csv_with_template(self, mock_preppy):
        # Mock preppy module
        mock_template = MagicMock()
        mock_template.getOutput.return_value = "Name,ID,Type\nTest DMP 1,mdm1-test1,dmp\n"
        mock_preppy.getModule.return_value = mock_template

        with tempfile.NamedTemporaryFile(mode='w', suffix='.prep', delete=False) as tmp_file:
            tmp_file.write("test template")
            template_path = tmp_file.name

        try:
            result = self.exporter.render(self.test_data, template_path)

            self.assertIsInstance(result, dict)
            self.assertEqual(result['format'], 'csv')
            self.assertIn('text', result)
            self.assertEqual(result['mimetype'], 'text/csv')
            self.assertEqual(result['file_extension'], '.csv')
            self.assertIn('Name,ID,Type', result['text'])

            # Verify preppy was called correctly
            mock_preppy.getModule.assert_called_once_with(template_path)
            mock_template.getOutput.assert_called_once()

        finally:
            os.unlink(template_path)

    @patch('nistoar.midas.export.exporters.csv_exporter.preppy')
    def test_render_csv_without_template_dmp(self, mock_preppy):
        # Mock preppy for default DMP template
        mock_template = MagicMock()
        mock_template.getOutput.return_value = "Name,ID,Type\nTest DMP 1,mdm1-test1,dmp\n"
        mock_preppy.getModule.return_value = mock_template

        result = self.exporter.render(self.test_data)

        self.assertIsInstance(result, dict)
        self.assertEqual(result['format'], 'csv')
        self.assertIn('text', result)
        self.assertEqual(result['mimetype'], 'text/csv')
        
        # Should use default DMP template
        mock_preppy.getModule.assert_called_once()
        call_args = mock_preppy.getModule.call_args[0][0]
        self.assertIn('dmp_csv_template.prep', call_args)

    @patch('nistoar.midas.export.exporters.csv_exporter.preppy')
    def test_render_csv_without_template_dap(self, mock_preppy):
        # Modify test data for DAP type
        dap_data = self.test_data.copy()
        dap_data[0]['id'] = 'mds3-test1'
        dap_data[0]['type'] = 'dap'

        mock_template = MagicMock()
        mock_template.getOutput.return_value = "Name,ID,Type\nTest DAP 1,mds3-test1,dap\n"
        mock_preppy.getModule.return_value = mock_template

        result = self.exporter.render(dap_data)

        self.assertIsInstance(result, dict)
        self.assertEqual(result['format'], 'csv')
        
        # Should use DAP template
        mock_preppy.getModule.assert_called_once()
        call_args = mock_preppy.getModule.call_args[0][0]
        self.assertIn('dap_csv_template.prep', call_args)

    @patch('nistoar.midas.export.exporters.csv_exporter.preppy')
    def test_render_csv_with_error(self, mock_preppy):
        # Mock preppy to raise an exception
        mock_preppy.getModule.side_effect = Exception("Template not found")

        with self.assertRaises(Exception) as context:
            self.exporter.render(self.test_data)

        self.assertIn("Template not found", str(context.exception))

    def test_render_empty_data(self):
        with patch('nistoar.midas.export.exporters.csv_exporter.preppy') as mock_preppy:
            mock_template = MagicMock()
            mock_template.getOutput.return_value = "Name,ID,Type\n"
            mock_preppy.getModule.return_value = mock_template

            result = self.exporter.render([])

            self.assertIsInstance(result, dict)
            self.assertEqual(result['format'], 'csv')
            self.assertEqual(result['text'], "Name,ID,Type\n")

    def test_detect_record_type_dmp(self):
        self.assertEqual(self.exporter._detect_record_type(self.test_data), 'dmp')

    def test_detect_record_type_dap(self):
        dap_data = [{'id': 'mds3-test', 'type': 'dap'}]
        self.assertEqual(self.exporter._detect_record_type(dap_data), 'dap')

    def test_detect_record_type_default(self):
        unknown_data = [{'id': 'unknown-test'}]
        self.assertEqual(self.exporter._detect_record_type(unknown_data), 'dmp')


if __name__ == '__main__':
    unittest.main()
