from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter
import preppy

DEFAULT_CSV_TEMPLATE = "dmp_csv_template.prep"  # Fallback to DMP template for compatibility


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
        Render a single JSON input into a single CSV output
        """
        # This exporter expects a mapping-like payload (DMPS style).
        if not isinstance(json_payload, MappingABC):
            raise TypeError("CSVExporter expects a mapping-like payload (e.g., dict)")

        template_filename = template_name or DEFAULT_CSV_TEMPLATE

        # Preppy can take the full .prep path or just the module base
        # We pass the .prep path for clarity
        template_path = self.resolve_template_path("csv", template_filename)
        preppy_template = preppy.getModule(str(template_path))

        # Load template and parse - CSV templates expect a list of records
        data_for_template = json_payload.get("data", json_payload)
        # Wrap single record in a list since CSV templates use {{for record in records}}
        records_list = [json_payload]  # Pass the full record including metadata
        csv_text = preppy_template.get(records=records_list)

        return {
            "format": self.format_name,
            "filename": f"{filename}{self.file_extension}",
            "mimetype": "text/csv",
            "text": csv_text,
            "file_extension": self.file_extension,
        }
