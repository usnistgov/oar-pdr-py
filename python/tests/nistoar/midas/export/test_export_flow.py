import json
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
import tempfile

from nistoar.midas.export.export import export, run
from nistoar.midas.export import export as export_mod


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
            template_dir=str(self.outdir),  # not really used since preppy is mocked
            template_name="dmp_pdf_template.prep",
        )
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["filename"], "exampleDMP.pdf")
        self.assertIn("bytes", result)
        self.assertIsInstance(result["bytes"], (bytes, bytearray))

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_run_batch_two_inputs(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        def fake_concat(results, output_filename):
            return {
                "format": "pdf",
                "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                "mimetype": "application/pdf",
                "file_extension": ".pdf",
                "bytes": b"FAKEPDF",
            }

        inputs = [self.json_path, {"input_type": "json", "source": self.json_path, "filename": "x"}]

        # Temporarily override the registry entry for "pdf"
        from unittest.mock import patch as patch_module
        with patch_module.dict(export_mod.CONCAT_REGISTRY, {"pdf": fake_concat}):
            result = run(
                input_data=inputs,
                output_format="pdf",
                output_directory=self.outdir,
                template_dir=str(self.outdir),
                template_name="dmp_pdf_template.prep",
                generate_file=True,
            )
        # run(...) now returns a single combined dict
        self.assertIsInstance(result, dict)
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["file_extension"], ".pdf")
        outpath = self.outdir / result["filename"]
        self.assertTrue(outpath.is_file())
        self.assertEqual(str(outpath), result["path"])


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
            template_dir=str(self.outdir), # not used because preppy is mocked
            template_name="dmp_pdf_template.prep",
        )
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["filename"], "alpha.pdf")
        self.assertIn("bytes", result)

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_run_with_iterator_of_project_records(self, mock_get_module, _):
        mock_template = Mock()
        mock_template.get.return_value = "<document/>"
        mock_get_module.return_value = mock_template

        def fake_concat(results, output_filename):
            return {
                "format": "pdf",
                "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                "mimetype": "application/pdf",
                "file_extension": ".pdf",
                "bytes": b"FAKEPDF-ITER",
            }

        rec_iter = _iter_fake_records(3)  # generator, not a list

        from unittest.mock import patch as patch_module
        with patch_module.dict(export_mod.CONCAT_REGISTRY, {"pdf": fake_concat}):
            result = run(
                input_data=rec_iter,
                output_format="pdf",
                output_directory=self.outdir,
                template_dir=str(self.outdir),
                template_name="dmp_pdf_template.prep",
                generate_file=True,
            )
        self.assertIsInstance(result, dict)
        self.assertEqual(result["format"], "pdf")
        outpath = self.outdir / result["filename"]
        self.assertTrue(outpath.is_file())


    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_with_project_records_missing_name_uses_id_then_random(self, mock_get_module, _):
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

        result1 = export(
            input_item=rec_with_id,
            output_format="pdf",
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
            _index=0,
        )
        self.assertEqual(result1["filename"], "xyz.pdf")

        result2 = export(
            input_item=rec_no_name_id,
            output_format="pdf",
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
            _index=1,
        )
        self.assertRegex(result2["filename"], r"^record_[0-9a-f]{8}_1\.pdf$")
    
    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_bytes_output_single(self, mock_get_module, _):
        """Test that single record export without file generation produces usable PDF bytes"""
        mock_template = Mock()
        mock_template.get.return_value = "<document>PDF Content</document>"
        mock_get_module.return_value = mock_template

        # Create a fake project record
        rec = _FakeProjectRecord(name="pdf_test", data={"title": "PDF Export Test", "description": "Testing PDF bytes"})
        
        # Call export directly with your export function (not run)
        result = export(
            input_item=rec,
            output_format="pdf",
            template_dir=str(self.outdir),
            template_name="dmp_pdf_template.prep",
        )
        
        # Verify we got bytes (export function always returns bytes)
        self.assertIn("bytes", result)
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["filename"], "pdf_test.pdf")
        
        # Verify PDF bytes are valid (start with PDF header)
        pdf_bytes = result["bytes"]
        self.assertTrue(pdf_bytes.startswith(b'%PDF'))
        self.assertIsInstance(pdf_bytes, (bytes, bytearray))

    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_bytes_output_batch_no_file(self, mock_get_module, _):
        """Test that batch export without file generation produces usable PDF bytes"""
        mock_template = Mock()
        mock_template.get.return_value = "<document>Combined PDF Content</document>"
        mock_get_module.return_value = mock_template

        def fake_concat(results, output_filename):
            # Combine the bytes from all individual results
            combined_bytes = b"COMBINED-PDF-" + str(len(results)).encode() + b"-RECORDS"
            return {
                "format": "pdf",
                "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                "mimetype": "application/pdf",
                "file_extension": ".pdf",
                "bytes": combined_bytes,  # Combined PDF bytes
            }

        rec_iter = _iter_fake_records(3)  # Create 3 fake records

        from unittest.mock import patch as patch_module
        with patch_module.dict(export_mod.CONCAT_REGISTRY, {"pdf": fake_concat}):
            result = run(
                input_data=rec_iter,
                output_format="pdf",
                output_directory=self.outdir,
                template_dir=str(self.outdir),
                template_name="dmp_pdf_template.prep",
                generate_file=False,  # ✅ Don't write file - return bytes
            )
        
        # Verify we got bytes (not file path)
        self.assertIn("bytes", result)
        self.assertEqual(result["format"], "pdf")
        
        # Should NOT have path since no file was written
        self.assertNotIn("path", result)
        
        # Verify combined PDF bytes
        pdf_bytes = result["bytes"]
        self.assertEqual(pdf_bytes, b"COMBINED-PDF-3-RECORDS")  # From our fake concat
        self.assertIsInstance(pdf_bytes, (bytes, bytearray))
        


    @patch("nistoar.midas.export.exporters.pdf_exporter.trml2pdf.parseString", return_value=b"%PDF%")
    @patch("nistoar.midas.export.exporters.pdf_exporter.preppy.getModule")
    def test_export_bytes_vs_file_comparison(self, mock_get_module, _):
        """Compare bytes output vs file output to ensure they're consistent"""
        mock_template = Mock()
        mock_template.get.return_value = "<document>Comparison Test</document>"
        mock_get_module.return_value = mock_template

        def fake_concat(results, output_filename):
            return {
                "format": "pdf",
                "filename": output_filename if output_filename.endswith(".pdf") else output_filename + ".pdf",
                "mimetype": "application/pdf",
                "file_extension": ".pdf",
                "bytes": b"COMPARISON-PDF-CONTENT",
            }

        rec_iter1 = _iter_fake_records(2)
        rec_iter2 = _iter_fake_records(2)  # Same records for comparison

        from unittest.mock import patch as patch_module
        with patch_module.dict(export_mod.CONCAT_REGISTRY, {"pdf": fake_concat}):
            # Test 1: Get bytes only
            result_bytes = run(
                input_data=rec_iter1,
                output_format="pdf",
                output_directory=self.outdir,
                template_name="dmp_pdf_template.prep",
                generate_file=False,  # ✅ Return bytes
            )
            
            # Test 2: Write file and get path
            result_file = run(
                input_data=rec_iter2,
                output_format="pdf",
                output_directory=self.outdir,
                template_name="dmp_pdf_template.prep",
                generate_file=True,  # ✅ Write file
            )
        
        
        # Bytes result should have bytes but no path
        self.assertIn("bytes", result_bytes)
        self.assertNotIn("path", result_bytes)
        self.assertEqual(result_bytes["bytes"], b"COMPARISON-PDF-CONTENT")
        
        # File result should have path but no bytes
        self.assertIn("path", result_file)
        self.assertNotIn("bytes", result_file)
        
        # File should exist and contain the same content
        file_path = Path(result_file["path"])
        self.assertTrue(file_path.exists())
        self.assertTrue(file_path.is_file())
        
        # Read file content and compare
        file_content = file_path.read_bytes()
        self.assertEqual(file_content, b"COMPARISON-PDF-CONTENT")



if __name__ == "__main__":
    test.main()
