from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter
import preppy

DEFAULT_MD_TEMPLATE = "dmp_markdown_template.prep"  # Fallback to DMP template for compatibility


class MarkdownExporter(Exporter):
    format_name = "markdown"
    file_extension = ".md"

    def _normalize_template_name(self, template_name: str) -> str:
        """
        Normalize template names to handle naming convention variations.
        Maps 'md' to 'markdown' in template names for compatibility with actual file structure.
        """
        if not template_name:
            return template_name
            
        # Handle common template name variations - map from expected names to actual file names
        name_mappings = {
            "dap_md_template.prep": "dap_markdown_template.prep",
            "dmp_md_template.prep": "dmp_markdown_template.prep",
        }
        
        # Direct mapping if exists
        if template_name in name_mappings:
            return name_mappings[template_name]
            
        # General pattern replacement: replace '_md_template.prep' with '_markdown_template.prep'
        if "_md_template.prep" in template_name:
            return template_name.replace("_md_template.prep", "_markdown_template.prep")
            
        return template_name

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

        raise TypeError("MarkdownExporter.render: unsupported payload type.")

    def render_json(self, json_payload: Any, filename: str, template_name: str):
        """
        Render a single JSON input into a single Markdown output
        """
        # This exporter expects a mapping-like payload (DMPS style).
        if not isinstance(json_payload, MappingABC):
            raise TypeError("MarkdownExporter expects a mapping-like payload (e.g., dict)")

        template_filename = template_name or DEFAULT_MD_TEMPLATE
        
        # Normalize template names to handle both "markdown" and "md" naming conventions
        template_filename = self._normalize_template_name(template_filename)

        # Preppy can take the full .prep path or just the module base
        # We pass the .prep path for clarity
        template_path = self.resolve_template_path("markdown", template_filename)
        preppy_template = preppy.getModule(str(template_path))

        # Load template and parse
        data_for_template = json_payload.get("data", json_payload)
        md_text = preppy_template.get(data_for_template)

        return {
            "format": self.format_name,
            "filename": f"{filename}{self.file_extension}",
            "mimetype": "text/markdown",
            "text": md_text,
            "file_extension": self.file_extension,
        }
