import unittest as test
from unittest.mock import patch, Mock

from nistoar.midas.export.exporters.md_exporter import MarkdownExporter


class MarkdownExporterTest(test.TestCase):

    @patch("nistoar.midas.export.exporters.md_exporter.preppy.getModule")
    def test_render_json_ok(self, mock_get_module):
        # Mock preppy template.get(...) to return Markdown text
        mock_template = Mock()
        mock_template.get.return_value = "# My DMP\nSome content\n"
        mock_get_module.return_value = mock_template

        exporter = MarkdownExporter(template_dir="/tmp/does-not-matter")

        payload = {"data": {"title": "My DMP"}}
        result = exporter.render("json", payload, "mydoc_md", template_name="dmp_md_template.prep")

        self.assertEqual(result["format"], "markdown")
        self.assertEqual(result["filename"], "mydoc_md.md")
        self.assertEqual(result["mimetype"], "text/markdown")
        self.assertEqual(result["file_extension"], ".md")

        # MarkdownExporter returns text, not bytes
        self.assertEqual(result["text"], "# My DMP\nSome content\n")

        # Ensure preppy was called with the correct template path
        args, _ = mock_get_module.call_args
        # args[0] is the template path string passed to preppy.getModule(...)
        self.assertTrue(str(args[0]).endswith("dmp_md_template.prep"))

        # Making sure template.get(...) was fed the inner "data" dict
        mock_template.get.assert_called_once_with({"title": "My DMP"})

    def test_render_json_type_error_on_non_mapping(self):
        exporter = MarkdownExporter()
        # if payload is a list instead of a mapping should raise error
        with self.assertRaises(TypeError):
            exporter.render("json", ["not-a-dict"], "x_md")


if __name__ == "__main__":
    test.main()
