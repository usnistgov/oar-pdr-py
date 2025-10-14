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


class _FakeProjectRecord:
    def __init__(self, name=None, rec_id=None, data=None):
        self.data = data or {"title": "Demo from record"}
        if name is not None:
            self.name = name
        if rec_id is not None:
            self.id = rec_id


def _iter_fake_records(n=3):
    for i in range(n):
        yield _FakeProjectRecord(name=f"rec{i}", data={"title": f"Record {i}"})


class ExportFlowProjectRecordTest(test.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.outdir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_single_project_record(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>" # template output is irrelevant to the writer
        mock_get_module.return_value = mock_template

        rec = _FakeProjectRecord(name="alpha", data={"title": "Alpha"})
        result = export(
            input_item=rec,
            output_format="pdf",
            output_directory=self.outdir,
            template_dir=str(self.outdir), # not used because preppy is mocked
            template_name="dmp_pdf_template.prep",
        )
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["filename"], "alpha.pdf")
        self.assertTrue((self.outdir / result["filename"]).is_file())

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_run_with_iterator_of_project_records(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        rec_iter = _iter_fake_records(3) # generator, not a list
        results = run(
            input_data=rec_iter,
            output_format="pdf",
            output_directory=self.outdir,
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
        )
        self.assertEqual(len(results), 3)
        # filenames should align with record names
        expected = {"rec0.pdf", "rec1.pdf", "rec2.pdf"}
        got = {r["filename"] for r in results}
        self.assertSetEqual(got, expected)
        for r in results:
            self.assertTrue((self.outdir / r["filename"]).is_file())

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_run_with_project_records_missing_name_uses_id_then_random(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        rec_with_id = _FakeProjectRecord(rec_id="xyz")
        rec_no_name_id = _FakeProjectRecord()
        # Ensure no attributes
        if hasattr(rec_no_name_id, "name"):
            delattr(rec_no_name_id, "name")
        if hasattr(rec_no_name_id, "id"):
            delattr(rec_no_name_id, "id")

        results = run(
            input_data=[rec_with_id, rec_no_name_id],
            output_format="pdf",
            output_directory=self.outdir,
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
        )

        # First uses id
        self.assertEqual(results[0]["filename"], "xyz.pdf")
        self.assertTrue((self.outdir / "xyz.pdf").is_file())

        # Second gets fallback
        self.assertRegex(results[1]["filename"], r"^record_[0-9a-f]{8}_1\.pdf$")
        self.assertTrue((self.outdir / results[1]["filename"]).is_file())


if __name__ == "__main__":
    test.main()
