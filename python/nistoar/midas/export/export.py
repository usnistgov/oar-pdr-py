from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from pathlib import Path

from utils.loader import normalize_input
from utils.writer import write_file
from exporters.pdf_exporter import PDFExporter
from exporters.markdown_exporter import MarkdownExporter


def run(input_data: List, output_format: str, output_directory: Path, template_dir: str = None, template_name: str = None):
    """ Wrapper that handles 1 to N inputs. The initial format of the inputs, the template (if any) used for rendering,
     the output format and the output directory must be the same for all inputs.

    Args:
        input_data:
        output_format:
        output_directory:
        template_dir:
        template_name:

    Returns:

    """
    results = []
    for i in range(len(input_data)):
        result = export(input_data[i], output_format, output_directory, template_dir, template_name, _index=i)
        results.append(result)


def export(input_item: Any, output_format: str, output_directory: Path, template_dir: str = None, template_name: str = None, _index: int = 0):
    """
    Manage the export for a single input by leveraging the right exporter and the right configurations.

    Args:
        input_item:
        output_format:
        output_directory:
        template_dir:
        template_name:
        _index:

    Returns:

    """
    # Normalize input in a format the exporters understand
    input_type, payload, output_filename = normalize_input(input_item, index=_index)

    # Select the right exporter
    output_format_key = (output_format or "").strip().lower()
    exporters: Dict[str, Any] = {
        "pdf": PDFExporter(template_dir=template_dir),
        "markdown": MarkdownExporter(template_dir=template_dir),
    }
    if output_format_key not in exporters:
        supported = ", ".join(sorted(exporters.keys()))
        raise ValueError(f"Unknown output_format '{output_format}'. Supported: {supported}")

    exporter = exporters[output_format_key]

    # render in memory
    render_result = exporter.render(
        input_type=input_type,
        payload=payload,
        output_filename=output_filename,
        template_name=template_name,
    )

    # write file
    output_path = write_file(output_directory, render_result)

    return {
        "format": render_result["format"],
        "filename": render_result["filename"],
        "mimetype": render_result["mimetype"],
        "file_extension": render_result["file_extension"],
        "path": output_path,
    }
