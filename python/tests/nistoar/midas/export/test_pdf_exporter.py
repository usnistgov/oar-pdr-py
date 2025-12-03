import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path

from nistoar.midas.export.exporters.pdf_exporter import PDFExporter


class PDFExporterTest(test.TestCase):
    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_render_json_ok(self, mock_get_module, mock_parse_string):
        # Mock preppy template.get(...) to return RML XML
        mock_template = Mock()
        mock_template.get.return_value = "<document>rml</document>"
        mock_get_module.return_value = mock_template

        # Mock trml2pdf
        mock_parse_string.return_value = b"%PDF-sample%"

        exporter = PDFExporter(template_dir="/tmp/does-not-matter")
        payload = {"data": {"title": "My DMP"}}
        result = exporter.render("json", payload, "mydoc", template_name="dmp_pdf_template.prep")

        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["filename"], "mydoc.pdf")
        self.assertEqual(result["mimetype"], "application/pdf")
        self.assertEqual(result["file_extension"], ".pdf")
        self.assertEqual(result["bytes"], b"%PDF-sample%")

        # Ensure preppy was called with the .prep filename (full path is fine)
        args, _ = mock_get_module.call_args
        self.assertTrue(str(args[0]).endswith("dmp_pdf_template.prep"))

    def test_render_json_type_error_on_non_mapping(self):
        exporter = PDFExporter()
        with self.assertRaises(TypeError):
            exporter.render("json", ["not-a-dict"], "x")


if __name__ == "__main__":
    test.main()
