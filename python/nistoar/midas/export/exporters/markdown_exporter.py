from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter


class MarkdownExporter(Exporter):
    format_name = "markdown"
    file_extension = ".md"

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

    def render_json(self, json_payload: Any, output_filename: str, template_name: str):
        """
        Render a single JSON input into a single Markdown output
        """
        # This exporter expects a mapping-like payload (DMPS style).
        return NotImplementedError
