from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter
import preppy

DEFAULT_MD_TEMPLATE = "dmp_md_template.prep"


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
        if input_type == "json":
            return self.render_json(payload, output_filename, template_name)

        raise TypeError("MarkdownExporter.render: unsupported payload type.")

    def render_json(self, json_payload: Any, output_filename: str, template_name: str):
        """
        Render a single JSON input into a single Markdown output
        """
        # This exporter expects a mapping-like payload (DMPS style).
        if not isinstance(json_payload, MappingABC):
            raise TypeError("MarkdownExporter expects a mapping-like payload (e.g., dict)")

        template_filename = template_name or DEFAULT_MD_TEMPLATE

        # Preppy can take the full .prep path or just the module base
        # We pass the .prep path for clarity
        template_path = self.resolve_template_path("markdown", template_filename)
        preppy_template = preppy.getModule(str(template_path))

        # Load template and parse
        data_for_template = json_payload.get("data", json_payload)
        md_text = preppy_template.get(data_for_template)

        return {
            "format": self.format_name,
            "filename": f"{output_filename}{self.file_extension}",
            "mimetype": "text/markdown",
            "text": md_text,
            "file_extension": self.file_extension,
        }
