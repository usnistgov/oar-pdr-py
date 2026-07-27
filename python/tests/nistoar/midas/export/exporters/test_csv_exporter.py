import unittest
from unittest.mock import patch, MagicMock
import csv
import io
from pathlib import Path
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

    def csv_rows(self, text):
        return list(csv.reader(io.StringIO(text)))

    def test_render_csv_without_template_dmp(self):
        result = self.exporter.render("json", self.test_data, "test-dmp")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['format'], 'csv')
        self.assertEqual(result['filename'], 'test-dmp.csv')
        self.assertIn('text', result)
        self.assertEqual(result['mimetype'], 'text/csv')

        rows = self.csv_rows(result['text'])
        self.assertEqual(rows[0][:3], ['Name', 'ID', 'Type'])
        self.assertEqual(rows[1][0], 'Test DMP 1')
        self.assertEqual(rows[1][1], 'mdm1-test1')
        self.assertEqual(rows[1][2], 'DMP')

    def test_render_csv_with_report_template(self):
        mock_template = MagicMock()
        mock_template.get.return_value = "Name,ID,Type\nTest DMP 1,mdm1-test1,dmp\n"
        template_path = Path("/tmp/export_report_csv_template.prep")

        with patch.object(self.exporter, 'resolve_template_path', return_value=template_path), \
             patch('nistoar.midas.export.exporters.csv_exporter.preppy.getModule',
                   return_value=mock_template) as mock_get_module:
            result = self.exporter.render(
                "json", self.test_data[0], "test-report", "export_report_csv_template.prep")

        self.assertIsInstance(result, dict)
        self.assertEqual(result['format'], 'csv')
        self.assertEqual(result['filename'], 'test-report.csv')
        self.assertEqual(result['text'], "Name,ID,Type\nTest DMP 1,mdm1-test1,dmp\n")
        mock_get_module.assert_called_once_with(str(template_path))
        mock_template.get.assert_called_once_with(self.test_data[0]['data'])

    def test_render_csv_without_template_dap(self):
        dap_data = [dict(self.test_data[0])]
        dap_data[0]['id'] = 'mds3-test1'
        dap_data[0]['type'] = 'dap'
        dap_data[0]['data'] = {
            'title': 'Test DAP 1',
            'doi': 'doi:10.18434/mds3-test1',
            'contactPoint': {'fn': 'Jane Smith', 'hasEmail': 'jane.smith@nist.gov'},
            'keywords': ['test', 'data']
        }

        result = self.exporter.render("json", dap_data, "test-dap")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['format'], 'csv')

        rows = self.csv_rows(result['text'])
        self.assertEqual(rows[0][:3], ['Name', 'ID', 'Type'])
        self.assertEqual(rows[0][10], 'DOI')
        self.assertEqual(rows[1][1], 'mds3-test1')
        self.assertEqual(rows[1][2], 'DAP')

    def test_render_csv_with_error(self):
        with patch.object(self.exporter, 'resolve_template_path',
                          return_value=Path("/tmp/export_report_csv_template.prep")), \
             patch('nistoar.midas.export.exporters.csv_exporter.preppy.getModule',
                   side_effect=Exception("Template not found")):
            with self.assertRaises(Exception) as context:
                self.exporter.render(
                    "json", self.test_data[0], "test-report", "export_report_csv_template.prep")

        self.assertIn("Template not found", str(context.exception))

    def test_render_rejects_unsupported_input_type(self):
        with self.assertRaises(TypeError) as context:
            self.exporter.render("xml", self.test_data, "test")
        self.assertIn("unsupported payload type", str(context.exception))

    def test_render_empty_data(self):
        with self.assertRaises(TypeError) as context:
            self.exporter.render("json", [], "empty")
        self.assertIn("CSVExporter expects", str(context.exception))

    def test_render_unknown_record_id_defaults_to_dmp(self):
        unknown_data = [dict(self.test_data[0])]
        unknown_data[0]['id'] = 'unknown-test'

        result = self.exporter.render("json", unknown_data, "test-default")
        rows = self.csv_rows(result['text'])
        self.assertEqual(rows[1][2], 'DMP')

    def test_dmp_csv_new_fields(self):
        record = {
            'id': 'mdm1-test-fields',
            'name': 'Test DMP',
            'owner': 'testuser',
            'status': {},
            'meta': {},
            'data': {
                'dataSizeDescription': 'Annual',
                'pathsURLs': [' /data/project ', ' https://example.com/data '],
            }
        }
        result = self.exporter.render_json(record, 'test')
        text = result['text']
        self.assertIn('DataSizeFrequency', text)
        self.assertIn('Annual', text)
        self.assertIn('DataFilePaths', text)
        self.assertIn('/data/project; https://example.com/data', text)

    def test_dmp_csv_empty_size_description_shows_not_provided(self):
        record = {
            'id': 'mdm1-test-empty',
            'name': 'Test DMP',
            'owner': 'testuser',
            'status': {},
            'meta': {},
            'data': {'dataSizeDescription': ''}
        }
        result = self.exporter.render_json(record, 'test')
        # empty string must not silently pass through as a blank cell
        rows = list(result['text'].splitlines())
        data_row = rows[1]
        self.assertIn('Not Provided', data_row)


if __name__ == '__main__':
    unittest.main()
