from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter

DEFAULT_CSV_TEMPLATE = "dmp_csv_template.prep" 


class CSVExporter(Exporter):
    format_name = "csv"
    file_extension = ".csv"

    def render(self, input_type: str, payload: Any, filename: str, template_name: str = None):
        """
        Render a single input payload into a single output file in the format requested.
        Input type must match one of the cases or a type error is raised.

        Args:
            input_type: Initial format of the data.
            payload: Content the exporter can handle.
            filename: Base name for the rendered result (without extension).
            template_name: Optional template filename the exporter uses.

        Returns: A dictionary.
        """
        if input_type == "json":
            return self.render_json(payload, filename, template_name)

        raise TypeError("CSVExporter.render: unsupported payload type.")

    def render_json(self, json_payload: Any, filename: str, template_name: str = None):
        """
        Render JSON data into CSV format using the specified template.
        
        Args:
            json_payload: The record data (ProjectRecord object or dict)
            filename: Base filename for output
            template_name: Template to use (determined by WSGI handler)
            
        Returns: Dict with CSV content and metadata
        """
        import preppy
        
        # This exporter expects a mapping-like payload (DMPS/DAPS style).
        if not isinstance(json_payload, MappingABC):
            raise TypeError("CSVExporter expects a mapping-like payload (e.g., dict)")

        template_filename = template_name or DEFAULT_CSV_TEMPLATE

        # Preppy can take the full .prep path or just the module base
        # We pass the .prep path for clarity
        template_path = self.resolve_template_path("csv", template_filename)
        preppy_template = preppy.getModule(str(template_path))

        # Load template and parse - handle both list of records and single record
        if isinstance(json_payload, list):
            records = [rec.get("data", rec) if isinstance(rec, dict) else rec for rec in json_payload]
        else:
            data_for_template = json_payload.get("data", json_payload)
            records = [data_for_template]

        # Generate CSV content using preppy template
        csv_content = preppy_template.getOutput(records=records)

        # Ensure filename has correct extension
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"

        return {
            "format": self.format_name,
            "filename": filename,
            "mimetype": "text/csv",
            "text": csv_content,
            "file_extension": self.file_extension,
        }
