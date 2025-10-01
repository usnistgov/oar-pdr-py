import json
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
import tempfile

from nistoar.midas.export.export import export, run


class ExportFlowTest(test.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.outdir = Path(self.tmp.name)
        self.datadir = Path(__file__).parent / "data"
        self.json_path = self.datadir / "exampleDMP.json"
        self.payload = json.loads(self.json_path.read_text(encoding="utf-8"))

    def tearDown(self):
        self.tmp.cleanup()

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_single_from_path(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        result = export(
            input_item=self.json_path,
            output_format="pdf",
            output_directory=self.outdir,
            template_dir=str(self.outdir),  # not really used since preppy is mocked
            template_name="dmp_pdf_template.prep",
        )
        self.assertEqual(result["format"], "pdf")
        self.assertTrue((self.outdir / result["filename"]).is_file())

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_run_batch_two_inputs(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        inputs = [self.json_path, {"input_type": "json", "source": self.json_path, "output_filename": "x"}]
        results = run(
            input_data=inputs,
            output_format="pdf",
            output_directory=self.outdir,
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
        )
        # run(...) returns list of dicts
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        for r in results:
            self.assertEqual(r["format"], "pdf")
            self.assertTrue((self.outdir / r["filename"]).is_file())


if __name__ == "__main__":
    test.main()
