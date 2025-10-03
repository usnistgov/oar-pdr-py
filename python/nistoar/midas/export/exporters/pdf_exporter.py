from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter
import preppy
import trml2pdf

DEFAULT_PDF_TEMPLATE = "dmp_pdf_template.prep"


class PDFExporter(Exporter):
    format_name = "pdf"
    file_extension = ".pdf"

    def render(self, input_type: str, payload: Any, output_filename: str, template_name: str = None):
        """
        Render a single input payload into a single output file in the format requested.
        Input type must match one of the cases or a type error is raised.

        Args:
            input_type: Initial format of the data.
            payload: Content the exporter can handle.
            output_filename: Base name for the output file (without extension).
            template_name: Optional template filename the exporter uses.

        Returns: A dictionary.

        """
        match input_type:
            case "json":
                return self.render_json(payload, output_filename, template_name)

        raise TypeError("PDFExporter.render: unsupported payload type.")

    def render_json(self, json_payload: Any, output_filename: str, template_name: str = None):
        """
        Render a single JSON input into a single PDF output
        """
        # This exporter expects a mapping-like payload (DMPS style).
        if not isinstance(json_payload, MappingABC):
            raise TypeError("PDFExporter expects a mapping-like payload (e.g., dict)")

        template_filename = template_name or DEFAULT_PDF_TEMPLATE

        # Preppy can take the full .prep path or just the module base
        # We pass the .prep path for clarity
        template_path = self.resolve_template_path("pdf", template_filename)
        preppy_template = preppy.getModule(str(template_path))

        # Load template and parse
        data_for_template = json_payload.get("data", json_payload)
        rml_xml_text = preppy_template.get(data_for_template)
        pdf_bytes = trml2pdf.parseString(rml_xml_text)

        return {
            "format": self.format_name,
            "filename": f"{output_filename}{self.file_extension}",
            "mimetype": "application/pdf",
            "bytes": pdf_bytes,
            "file_extension": self.file_extension,
        }
