from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional


class Exporter(ABC):
    """
    Abstract base for exporters.
    """
    format_name: str = ""
    file_extension: str = ""

    def __init__(self, *, template_dir: str = None):
        # base folder for templates
        self.template_dir = template_dir

    def resolve_template_path(self, format_subdir: str, template_filename: str):
        """
        Full path to a template file under templates/<format_subdirectory>/.
        """
        base_templates = Path(self.template_dir or Path(__file__).resolve().parents[1] / "templates")
        return base_templates / format_subdir / template_filename

    @abstractmethod
    def render(self, input_type: str, payload: Any, output_filename: str, template_name: str = None):
        """
        Render a single input payload into a single output file in the format requested.
        Args:
            input_type: Original format of the data
            payload: Content the exporter can handle.
            output_filename: Base name for the output file (without extension).
            template_name: Optional template filename the exporter uses.

        Returns: dict with at least: format, filename, mimetype, file_extension, and either bytes or text.

        """
        raise NotImplementedError
