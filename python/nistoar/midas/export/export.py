from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from pathlib import Path

from .utils.loader import normalize_input
from .utils.writer import write_file
from .exporters.pdf_exporter import PDFExporter
from .exporters.md_exporter import MarkdownExporter
from .utils.concat import REGISTRY as CONCAT_REGISTRY


def run(input_data: Iterable[Any], output_format: str, output_directory: Path, template_dir: str = None, template_name: str = None, output_filename: str = None, generate_file: bool = False):
    """ Wrapper that handles 1 to N inputs. The initial format of the inputs, the template (if any) used for rendering,
     the output format and the output directory must be the same for all inputs. Only one output is produced.
     Default behavior doesn't generate output file, it simply returns the rendered result.

    Args:
        input_data:
        output_format:
        output_directory:
        template_dir:
        template_name:
        output_filename:
        generate_file:

    Returns:

    """
    results = []
    if not input_data:
        raise ValueError("No inputs provided. Pass at least one input item.")
    for i, item in enumerate(input_data):
        result = export(item, output_format, template_dir, template_name, _index=i)
        results.append(result)

    # Determine output name
    if output_filename is None:
        # Use the first rendered filename as a fallback
        output_filename = results[0].get("filename", "combined")

    # Concatenate via registry
    concat_fn = CONCAT_REGISTRY.get(output_format)
    if concat_fn is None:
        raise ValueError(f"Concatenation not supported for format '{output_format}'.")
    combined = concat_fn(results, output_filename)

    # Default result
    if not generate_file:
        return combined

    # Single write
    path = write_file(output_directory, combined)

    return {
        "format": combined["format"],
        "filename": combined["filename"],
        "mimetype": combined["mimetype"],
        "file_extension": combined["file_extension"],
        "path": path,
    }


def export(input_item: Any, output_format: str, template_dir: str = None, template_name: str = None, _index: int = 0):
    """
    Manage the export for a single input by leveraging the right exporter and the right configurations.

    Args:
        input_item:
        output_format:
        template_dir:
        template_name:
        _index:

    Returns:

    """
    # Normalize input in a format the exporters understand
    info = normalize_input(input_item, index=_index)
    input_type = info['input_type']
    payload = info['payload']
    filename = info['filename']

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

    render_result = exporter.render(
        input_type=input_type,
        payload=payload,
        filename=filename,
        template_name=template_name,
    )

    return render_result
