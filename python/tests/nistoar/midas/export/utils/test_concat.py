import unittest
from nistoar.midas.export.utils.concat import concat_csv, REGISTRY


class TestConcatCSV(unittest.TestCase):

    def test_concat_csv_single_record(self):
        rendered_results = [
            {"text": "Name,ID,Type\nTest Record,test1,dmp\n"}
        ]
        
        result = concat_csv(rendered_results, "output")
        
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["filename"], "output.csv")
        self.assertEqual(result["mimetype"], "text/csv")
        self.assertEqual(result["file_extension"], ".csv")
        expected_text = "Name,ID,Type\nTest Record,test1,dmp"
        self.assertEqual(result["text"], expected_text)

    def test_concat_csv_multiple_records(self):
        rendered_results = [
            {"text": "Name,ID,Type\nRecord 1,test1,dmp\n"},
            {"text": "Name,ID,Type\nRecord 2,test2,dap\n"}
        ]
        
        result = concat_csv(rendered_results, "combined")
        
        self.assertEqual(result["format"], "csv")
        expected_text = "Name,ID,Type\nRecord 1,test1,dmp\nRecord 2,test2,dap"
        self.assertEqual(result["text"], expected_text)

    def test_concat_csv_empty_input(self):
        rendered_results = []
        
        result = concat_csv(rendered_results, "empty")
        
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["text"], "")

    def test_concat_csv_with_extension(self):
        rendered_results = [
            {"text": "Name,ID,Type\nTest,test1,dmp\n"}
        ]
        
        result = concat_csv(rendered_results, "test.csv")
        
        self.assertEqual(result["filename"], "test.csv")

    def test_concat_csv_invalid_input(self):
        rendered_results = [
            {"no_text_key": "invalid"}
        ]
        
        with self.assertRaises(TypeError):
            concat_csv(rendered_results, "output")

    def test_concat_csv_with_quotes_in_data(self):
        rendered_results = [
            {"text": 'Name,Description\n"Test Record","Contains ""quoted"" text"\n'},
            {"text": 'Name,Description\n"Another Record","Also ""quoted"" data"\n'}
        ]
        
        result = concat_csv(rendered_results, "quoted")
        
        expected_text = 'Name,Description\n"Test Record","Contains ""quoted"" text"\n"Another Record","Also ""quoted"" data"'
        self.assertEqual(result["text"], expected_text)

    def test_concat_csv_skip_empty_lines(self):
        rendered_results = [
            {"text": "Name,ID\nRecord 1,test1\n\n"},
            {"text": "Name,ID\n\nRecord 2,test2\n"}
        ]
        
        result = concat_csv(rendered_results, "clean")
        
        expected_text = "Name,ID\nRecord 1,test1\nRecord 2,test2"
        self.assertEqual(result["text"], expected_text)

    def test_registry_includes_csv(self):
        self.assertIn("csv", REGISTRY)
        self.assertEqual(REGISTRY["csv"], concat_csv)


if __name__ == '__main__':
    unittest.main()
