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
                template_name="dmp_pdf_template.prep"            )
        # run(...) now returns a single combined dict
        self.assertIsInstance(result, dict)
        self.assertEqual(result["format"], "pdf")
        self.assertEqual(result["file_extension"], ".pdf")
        outpath = self.outdir / result["filename"]
        self.assertTrue(outpath.is_file())
        self.assertEqual(str(outpath), result["path"])


class _FakeProjectRecord:
    def __init__(self, name=None, rec_id=None, data=None, status=None, meta=None, owner=None):
        self.data = data or {"title": "Demo from record"}
        self.status = status or {}
        self.meta = meta or {}
        self.owner = owner or "TestUser"
        if name is not None:
            self.name = name
        if rec_id is not None:
            self.id = rec_id


def _iter_fake_records(n=3):
    """Generate realistic fake records alternating between DMP and DAP types"""
    for i in range(n):
        if i % 2 == 0:  # Even indices: DMP records
            dmp_data = {
                "title": f"Iterator DMP Project {i}",
                "startDate": f"2026-0{(i%9)+1}-01",
                "dmpSearchable": "yes",
                "funding": {"grant_source": f"Agency{i}", "grant_id": f"GRANT-{1000+i}"},
                "projectDescription": f"Automated test project {i} for comprehensive testing of DMP export functionality.",
                "keywords": [f"keyword{i}", f"test{i}", "automation"],
                "dataSize": (i+1) * 100.0,
                "sizeUnit": "GB",
                "softwareDevelopment": {"development": "yes" if i > 0 else "no"},
                "technicalResources": [f"Resource {i}", "Standard equipment"],
                "dataDescription": f"Test data description for project {i}",
                "dataCategories": [f"category{i}", "test data"],
                "preservationDescription": f"Preservation plan for project {i}"
            }
            status = {
                "state": "edit", "action": "update",
                "createdDate": f"2026-01-{10+i:02d}T10:00:00",
                "modifiedDate": f"2026-01-{10+i:02d}T15:30:00"
            }
            yield _FakeProjectRecord(
                name=f"iter_dmp_{i}", 
                rec_id=f"mdm1-iter-{i:03d}", 
                data=dmp_data, 
                status=status,
                owner=f"User{i}"
            )
        else:  # Odd indices: DAP records
            dap_data = {
                "@id": f"ark:/88434/mds3-iter-{i:03d}",
                "title": f"Iterator DAP Dataset {i}",
                "doi": f"doi:10.18434/mds3-iter-{i:03d}",
                "contactPoint": {"fn": f"Contact Person {i}", "hasEmail": f"contact{i}@nist.gov"},
                "keywords": [f"dataset{i}", f"iterator{i}", "test"],
                "theme": [f"Theme {i}", "Test Data"],
                "authors": [
                    {"fn": f"Author {i}", "orcid": f"0000-000{i}-000{i}-000{i}"}
                ],
                "file_count": (i+1) * 50,
                "nonfile_count": (i+1) * 5,
                "description": [f"Automated test dataset {i} for comprehensive DAP export testing."]
            }
            status = {
                "state": "published", "action": "publish",
                "createdDate": f"2026-01-{10+i:02d}T12:00:00",
                "modifiedDate": f"2026-01-{10+i:02d}T16:45:00"
            }
            meta = {"resourceType": "dataset", "version": f"{i}.0"}
            yield _FakeProjectRecord(
                name=f"iter_dap_{i}",
                rec_id=f"mds3-iter-{i:03d}",
                data=dap_data,
                status=status,
                meta=meta,
                owner=f"User{i}"
            )


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
    
    @patch("nistoar.midas.export.exporters.csv_exporter.preppy.getModule")
    def test_export_csv_single_dmp(self, mock_get_module):
        """Test CSV export for DMP record with comprehensive data"""
        mock_template = Mock()
        mock_template.getOutput.return_value = "Name,ID,Type\nTest DMP,mdm1-test,dmp\n"
        mock_get_module.return_value = mock_template

        # Create comprehensive DMP record data based on real structure
        dmp_data = {
            "title": "Comprehensive DMP Test Project",
            "startDate": "2026-01-01",
            "dmpSearchable": "yes",
            "funding": {
                "grant_source": "NSF",
                "grant_id": "NSF-123456789"
            },
            "projectDescription": "A comprehensive test project for data management planning with multiple contributors and detailed metadata.",
            "organizations": [
                {
                    "groupName": "Research Data and Computing Office – HQ",
                    "divisionName": "Research Data and Computing Office",
                    "ouName": "Laboratory Programs"
                },
                {
                    "groupName": "Materials Testing Group",
                    "divisionName": "Materials Science Division",
                    "ouName": "Material Measurement Laboratory"
                }
            ],
            "contributors": [
                {
                    "firstName": "John",
                    "lastName": "Smith",
                    "orcid": "0000-0002-1234-5678",
                    "emailAddress": "john.smith@nist.gov",
                    "groupOrgID": 14293,
                    "groupNumber": "60600",
                    "groupName": "Research Data and Computing Office – HQ",
                    "divisionOrgID": 14291,
                    "divisionNumber": "606",
                    "divisionName": "Research Data and Computing Office",
                    "ouOrgID": 13210,
                    "ouNumber": "60",
                    "ouName": "Laboratory Programs",
                    "primary_contact": "Yes",
                    "institution": "NIST",
                    "role": "Principal Investigator"
                }
            ],
            "keywords": ["materials science", "data management", "testing", "metadata"],
            "dataSize": 500.5,
            "sizeUnit": "GB",
            "softwareDevelopment": {
                "development": "yes",
                "softwareUse": "Python scripts for data analysis",
                "softwareDatabase": "PostgreSQL for data storage",
                "softwareWebsite": "https://github.com/test-project/analysis"
            },
            "technicalResources": ["High-performance computing cluster", "Specialized measurement equipment", "Cloud storage"],
            "instruments": ["X-ray diffractometer", "Scanning electron microscope"],
            "ethical_issues": {
                "irb_number": "IRB-2026-001",
                "ethical_issues_exist": "no",
                "ethical_issues_description": "",
                "ethical_issues_report": ""
            },
            "security_and_privacy": {
                "data_sensitivity": ["public"],
                "cui": []
            },
            "dataDescription": "High-resolution measurements of material properties including crystalline structure analysis and surface morphology data.",
            "dataCategories": ["experimental data", "computational simulations", "image data", "measurement results"],
            "preservationDescription": "Data will be preserved in NIST data repository for minimum 10 years with full metadata documentation.",
            "dataAccess": "Data will be made publicly available through NIST Data Portal",
            "pathsURLs": ["https://data.nist.gov/test-project", "https://github.com/test-project/data"]
        }
        
        dmp_status = {
            "created_by": "midas/TestUser",
            "state": "edit",
            "action": "update",
            "created": 1740408241.2504392,
            "since": 1740408241.2504392,
            "modified": 1753379083.1024814,
            "message": "",
            "byWho": "TestUser",
            "createdDate": "2026-01-13T14:44:01",
            "modifiedDate": "2026-01-13T17:44:43",
            "sinceDate": "2026-01-13T14:44:01"
        }
        
        dmp_meta = {
            "resourceType": "dataset",
            "version": "1.0",
            "lastModified": "2026-01-13T17:44:43"
        }

        rec = _FakeProjectRecord(
            name="comprehensive_dmp_test", 
            rec_id="mdm1-test-comprehensive", 
            data=dmp_data,
            status=dmp_status,
            meta=dmp_meta,
            owner="TestUser"
        )
        
        result = export(
            input_item=rec,
            output_format="csv",
            template_dir=str(self.outdir),
            template_name="dmp_csv_template.prep",
        )
        
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["filename"], "comprehensive_dmp_test.csv")
        self.assertIn("text", result)
        self.assertEqual(result["mimetype"], "text/csv")
        
        # Verify CSV content contains our test data
        csv_content = result["text"]
        self.assertIn("Comprehensive DMP Test Project", csv_content)
        self.assertIn("mdm1-test-comprehensive", csv_content)
        self.assertIn("NSF", csv_content)
        self.assertIn("500.5", csv_content)
        self.assertIn("materials science", csv_content)

    @patch("nistoar.midas.export.exporters.csv_exporter.preppy.getModule")
    def test_export_csv_single_dap(self, mock_get_module):
        """Test CSV export for DAP record with comprehensive data"""
        mock_template = Mock()
        mock_template.getOutput.return_value = "Name,ID,Type\nTest DAP,mds3-test,dap\n"
        mock_get_module.return_value = mock_template

        # Create comprehensive DAP record data based on real structure
        dap_data = {
            "@id": "ark:/88434/mds3-test-comprehensive",
            "title": "Comprehensive DAP Test Dataset - Materials Property Analysis",
            "_schema": "https://data.nist.gov/od/dm/nerdm-schema/v0.7#",
            "@type": [
                "nrdp:PublicDataResource",
                "dcat:Resource"
            ],
            "doi": "doi:10.18434/mds3-test-comprehensive",
            "contactPoint": {
                "fn": "Dr. Jane Research",
                "hasEmail": "jane.research@nist.gov"
            },
            "keywords": ["materials", "properties", "crystallography", "electron microscopy", "data analysis"],
            "theme": ["Materials Science", "Data Analysis", "Characterization"],
            "authors": [
                {
                    "fn": "Dr. Jane Research",
                    "givenName": "Jane",
                    "familyName": "Research",
                    "orcid": "0000-0003-9876-5432",
                    "affiliation": "NIST Materials Measurement Laboratory"
                },
                {
                    "fn": "Dr. Bob Analysis",
                    "givenName": "Bob",
                    "familyName": "Analysis",
                    "orcid": "0000-0004-1234-5678",
                    "affiliation": "NIST Research Computing"
                }
            ],
            "references": [
                {
                    "title": "Advanced Materials Characterization Methods",
                    "citation": "Smith, J. et al. (2025). Advanced Materials Characterization. Nature Materials, 24(5), 123-145.",
                    "doi": "10.1038/nmat2025-123"
                },
                {
                    "title": "Crystallographic Database Standards",
                    "citation": "Jones, A. & Wilson, K. (2024). Database Standards for Materials Science. J. Materials Informatics, 15(3), 67-89."
                }
            ],
            "file_count": 156,
            "nonfile_count": 12,
            "description": [
                "This comprehensive dataset contains detailed materials property measurements including X-ray diffraction patterns, electron microscopy images, and computational analysis results. The data supports research into novel crystalline materials with applications in energy storage and conversion."
            ],
            "landingPage": "https://data.nist.gov/od/id/mds3-test-comprehensive",
            "accessLevel": "public",
            "license": "https://www.nist.gov/open/license",
            "modified": "2026-01-13",
            "issued": "2026-01-10"
        }
        
        dap_status = {
            "created_by": "TestUser",
            "state": "published",
            "action": "publish",
            "created": 1740438227.1065342,
            "since": 1740438227.1065342,
            "modified": 1740438236.38303,
            "message": "Dataset ready for publication",
            "createdDate": "2026-01-13T23:03:47",
            "modifiedDate": "2026-01-13T23:03:56",
            "sinceDate": "2026-01-13T23:03:47"
        }

        dap_meta = {
            "resourceType": "dataset",
            "creatorisContact": True,
            "contactName": "Dr. Jane Research",
            "willUpload": True,
            "assocPageType": "directly-related",
            "agent_vehicle": "midas",
            "version": "2.1",
            "dataQuality": "peer-reviewed"
        }

        rec = _FakeProjectRecord(
            name="comprehensive_dap_test", 
            rec_id="mds3-test-comprehensive", 
            data=dap_data,
            status=dap_status,
            meta=dap_meta,
            owner="TestUser"
        )
        
        result = export(
            input_item=rec,
            output_format="csv",
            template_dir=str(self.outdir),
            template_name="dap_csv_template.prep",
        )
        
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["filename"], "comprehensive_dap_test.csv")
        self.assertIn("text", result)
        self.assertEqual(result["mimetype"], "text/csv")
        
        # Verify CSV content contains our test data
        csv_content = result["text"]
        self.assertIn("Comprehensive DAP Test Dataset", csv_content)
        self.assertIn("mds3-test-comprehensive", csv_content)
        self.assertIn("Dr. Jane Research", csv_content)
        self.assertIn("jane.research@nist.gov", csv_content)
        self.assertIn("materials", csv_content)
        self.assertIn("156", csv_content)  # file_count

    def test_export_csv_comprehensive_batch(self):
        """Test CSV export with mixed DMP and DAP records in batch mode"""
        # Create comprehensive test records for batch processing
        def create_comprehensive_dmp():
            dmp_data = {
                "title": "Batch DMP Test - Advanced Materials Research",
                "startDate": "2026-01-15",
                "dmpSearchable": "yes", 
                "funding": {"grant_source": "DOE", "grant_id": "DOE-987654321"},
                "projectDescription": "Multi-year research project on advanced materials with international collaboration.",
                "keywords": ["materials", "collaboration", "advanced research"],
                "dataSize": 1200.0,
                "sizeUnit": "TB",
                "softwareDevelopment": {"development": "yes", "softwareUse": "Custom analysis tools"},
                "technicalResources": ["Supercomputing facility", "International beam line access"],
                "dataDescription": "Large-scale experimental and computational datasets from materials research.",
                "dataCategories": ["experimental", "computational", "collaborative"],
                "preservationDescription": "Long-term preservation in multiple repositories."
            }
            dmp_status = {
                "state": "active", "action": "update", 
                "createdDate": "2026-01-15T10:00:00", "modifiedDate": "2026-01-15T15:30:00"
            }
            return _FakeProjectRecord(
                name="batch_dmp", rec_id="mdm1-batch-001", 
                data=dmp_data, status=dmp_status, owner="BatchUser"
            )

        def create_comprehensive_dap():
            dap_data = {
                "@id": "ark:/88434/mds3-batch-001",
                "title": "Batch DAP Test - Comprehensive Materials Database",
                "doi": "doi:10.18434/mds3-batch-001",
                "contactPoint": {"fn": "Batch Coordinator", "hasEmail": "batch.coord@nist.gov"},
                "keywords": ["database", "materials", "comprehensive", "batch"],
                "theme": ["Materials Database", "Batch Processing"],
                "authors": [
                    {"fn": "Dr. Batch Leader", "orcid": "0000-0005-1111-2222"},
                    {"fn": "Data Specialist", "orcid": "0000-0006-3333-4444"}
                ],
                "references": [
                    {"title": "Batch Processing Methods", "citation": "Leader, B. (2026). Batch Methods. Data Science Journal."}
                ],
                "file_count": 2500,
                "nonfile_count": 50,
                "description": ["Comprehensive materials database with batch-processed experimental results."]
            }
            dap_status = {
                "state": "published", "action": "publish",
                "createdDate": "2026-01-15T12:00:00", "modifiedDate": "2026-01-15T16:45:00"
            }
            dap_meta = {"resourceType": "database", "version": "3.2", "dataQuality": "validated"}
            return _FakeProjectRecord(
                name="batch_dap", rec_id="mds3-batch-001",
                data=dap_data, status=dap_status, meta=dap_meta, owner="BatchUser"
            )

        # Create test records
        dmp_rec = create_comprehensive_dmp()
        dap_rec = create_comprehensive_dap()

        # Test individual exports to verify data structure
        dmp_result = export(
            input_item=dmp_rec,
            output_format="csv",
            template_dir=str(self.outdir),
            template_name="dmp_csv_template.prep",
        )
        
        dap_result = export(
            input_item=dap_rec,
            output_format="csv",
            template_dir=str(self.outdir),
            template_name="dap_csv_template.prep",
        )

        # Verify both exports succeeded
        self.assertEqual(dmp_result["format"], "csv")
        self.assertEqual(dap_result["format"], "csv")
        
        # Check DMP data extraction
        dmp_csv = dmp_result["text"]
        self.assertIn("Batch DMP Test", dmp_csv)
        self.assertIn("mdm1-batch-001", dmp_csv)
        self.assertIn("DOE", dmp_csv)
        self.assertIn("1200.0", dmp_csv)
        self.assertIn("advanced research", dmp_csv)

        # Check DAP data extraction
        dap_csv = dap_result["text"]
        self.assertIn("Batch DAP Test", dap_csv)
        self.assertIn("mds3-batch-001", dap_csv)
        self.assertIn("Batch Coordinator", dap_csv)
        self.assertIn("2500", dap_csv)  # file_count
        self.assertIn("batch.coord@nist.gov", dap_csv)
    
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
                template_dir=str(self.outdir),
                template_name="dmp_pdf_template.prep",
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
                template_name="dmp_pdf_template.prep",
            )
            
            # Test 2: Write file and get path
            result_file = run(
                input_data=rec_iter2,
                output_format="pdf",
                output_directory=self.outdir,
                template_name="dmp_pdf_template.prep",
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

    def test_run_batch_multiple_dmp_csv(self):
        """Test CSV export with multiple DMP records using run() function"""
        # Create 3 DMP records for batch processing
        dmp_records = []
        for i in range(3):
            dmp_data = {
                "title": f"Batch DMP {i+1} - Materials Research Project",
                "startDate": f"2026-0{i+1}-01",
                "dmpSearchable": "yes",
                "funding": {"grant_source": f"Agency{i+1}", "grant_id": f"GRANT-{2000+i}"},
                "projectDescription": f"Research project {i+1} for batch testing of DMP export functionality.",
                "keywords": [f"materials{i+1}", f"research{i+1}", "batch-test"],
                "dataSize": (i+1) * 250.0,
                "sizeUnit": "GB",
                "softwareDevelopment": {"development": "yes" if i > 0 else "no"},
                "technicalResources": [f"Equipment {i+1}", "Standard lab tools"],
                "dataDescription": f"Experimental data from materials research project {i+1}",
                "dataCategories": [f"experimental-{i+1}", "materials-data"],
                "preservationDescription": f"Data preservation plan for project {i+1}"
            }
            dmp_status = {
                "state": "edit", "action": "update",
                "createdDate": f"2026-01-{15+i:02d}T10:00:00",
                "modifiedDate": f"2026-01-{15+i:02d}T15:30:00"
            }
            dmp_records.append(_FakeProjectRecord(
                name=f"batch_dmp_{i+1}",
                rec_id=f"mdm1-batch-{i+1:03d}",
                data=dmp_data,
                status=dmp_status,
                owner=f"BatchUser{i+1}"
            ))

        # Use run() to export all DMP records as CSV
        result = run(
            input_data=dmp_records,
            output_format="csv",
            output_directory=self.outdir,
            template_name="dmp_csv_template.prep",
        )

        # Verify successful batch export
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["mimetype"], "text/csv")
        self.assertIn("text", result)
        
        # Verify all records are included in the CSV
        csv_content = result["text"]
        
        self.assertIn("Batch DMP 1", csv_content)
        self.assertIn("Batch DMP 2", csv_content)
        self.assertIn("Batch DMP 3", csv_content)
        self.assertIn("mdm1-batch-001", csv_content)
        self.assertIn("mdm1-batch-002", csv_content)
        self.assertIn("mdm1-batch-003", csv_content)
        self.assertIn("Agency1", csv_content)
        self.assertIn("Agency2", csv_content)
        self.assertIn("Agency3", csv_content)

    def test_run_batch_multiple_dap_csv(self):
        """Test CSV export with multiple DAP records using run() function"""
        # Create 3 DAP records for batch processing
        dap_records = []
        for i in range(3):
            dap_data = {
                "@id": f"ark:/88434/mds3-batch-{i+1:03d}",
                "title": f"Batch DAP {i+1} - Comprehensive Dataset Collection",
                "doi": f"doi:10.18434/mds3-batch-{i+1:03d}",
                "contactPoint": {"fn": f"Dataset Coordinator {i+1}", "hasEmail": f"coord{i+1}@nist.gov"},
                "keywords": [f"dataset{i+1}", f"collection{i+1}", "batch-processing"],
                "theme": [f"Dataset Theme {i+1}", "Batch Processing"],
                "authors": [
                    {"fn": f"Dr. Author {i+1}", "orcid": f"0000-000{i+1}-111{i+1}-222{i+1}"}
                ],
                "references": [
                    {"title": f"Reference {i+1}", "citation": f"Author{i+1}, A. (2026). Reference {i+1}. Journal."}
                ],
                "file_count": (i+1) * 100,
                "nonfile_count": (i+1) * 10,
                "description": [f"Comprehensive dataset {i+1} for batch DAP export testing."]
            }
            dap_status = {
                "state": "published", "action": "publish",
                "createdDate": f"2026-01-{18+i:02d}T12:00:00",
                "modifiedDate": f"2026-01-{18+i:02d}T16:45:00"
            }
            dap_meta = {"resourceType": "dataset", "version": f"{i+1}.0", "dataQuality": "validated"}
            dap_records.append(_FakeProjectRecord(
                name=f"batch_dap_{i+1}",
                rec_id=f"mds3-batch-{i+1:03d}",
                data=dap_data,
                status=dap_status,
                meta=dap_meta,
                owner=f"BatchUser{i+1}"
            ))

        # Use run() to export all DAP records as CSV
        result = run(
            input_data=dap_records,
            output_format="csv",
            output_directory=self.outdir,
            template_name="dap_csv_template.prep"
        )

        # Verify successful batch export
        self.assertEqual(result["format"], "csv")
        self.assertEqual(result["mimetype"], "text/csv")
        self.assertIn("text", result)
        
        # Verify all records are included in the CSV
        csv_content = result["text"]
        
        self.assertIn("Batch DAP 1", csv_content)
        self.assertIn("Batch DAP 2", csv_content)
        self.assertIn("Batch DAP 3", csv_content)
        self.assertIn("mds3-batch-001", csv_content)
        self.assertIn("mds3-batch-002", csv_content)
        self.assertIn("mds3-batch-003", csv_content)
        self.assertIn("coord1@nist.gov", csv_content)
        self.assertIn("coord2@nist.gov", csv_content)
        self.assertIn("coord3@nist.gov", csv_content)

    @patch("nistoar.midas.export.exporters.md_exporter.preppy.getModule")
    def test_run_batch_mixed_dmp_dap_markdown(self, mock_get_module):
        """Test Markdown export with mixed DMP and DAP records using run() function"""
        mock_template = Mock()
        mock_template.get.return_value = "# Test Markdown Output"
        mock_get_module.return_value = mock_template

        # Create mixed DMP and DAP records
        mixed_records = []
        
        # Add 2 DMP records
        for i in range(2):
            dmp_data = {
                "title": f"Mixed Batch DMP {i+1}",
                "projectDescription": f"DMP project {i+1} in mixed batch",
                "keywords": [f"dmp{i+1}", "mixed-batch"]
            }
            mixed_records.append(_FakeProjectRecord(
                name=f"mixed_dmp_{i+1}",
                rec_id=f"mdm1-mixed-{i+1:03d}",
                data=dmp_data,
                owner=f"MixedUser{i+1}"
            ))
        
        # Add 2 DAP records  
        for i in range(2):
            dap_data = {
                "@id": f"ark:/88434/mds3-mixed-{i+1:03d}",
                "title": f"Mixed Batch DAP {i+1}",
                "contactPoint": {"fn": f"Mixed Contact {i+1}", "hasEmail": f"mixed{i+1}@nist.gov"},
                "keywords": [f"dap{i+1}", "mixed-batch"]
            }
            mixed_records.append(_FakeProjectRecord(
                name=f"mixed_dap_{i+1}",
                rec_id=f"mds3-mixed-{i+1:03d}",
                data=dap_data,
                owner=f"MixedUser{i+3}"
            ))

        def fake_concat(results, output_filename):
            # Combine markdown results
            combined_text = "\n\n".join([r["text"] for r in results])
            return {
                "format": "markdown",
                "filename": output_filename if output_filename.endswith(".md") else output_filename + ".md",
                "mimetype": "text/markdown",
                "file_extension": ".md",
                "text": combined_text,
            }

        # Use run() to export mixed records as Markdown
        from unittest.mock import patch as patch_module
        with patch_module.dict(export_mod.CONCAT_REGISTRY, {"markdown": fake_concat}):
            result = run(
                input_data=mixed_records,
                output_format="markdown",
                output_directory=self.outdir,
                template_name="mixed_template.prep"
            )

        # Verify successful batch export
        self.assertEqual(result["format"], "markdown")
        self.assertEqual(result["mimetype"], "text/markdown")
        self.assertIn("text", result)
        
        # Should have 4 markdown sections (2 DMP + 2 DAP)
        markdown_content = result["text"]
        markdown_sections = markdown_content.split("\n\n")
        self.assertEqual(len(markdown_sections), 4)
        
        # Each section should be our test markdown content
        for section in markdown_sections:
            self.assertEqual(section, "# Test Markdown Output")


if __name__ == "__main__":
    test.main()
