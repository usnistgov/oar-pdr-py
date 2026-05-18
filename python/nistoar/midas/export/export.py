from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union
from pathlib import Path
import logging

from .utils.loader import normalize_input
from .utils.writer import write_file
from .utils.report import build_report_payload, render_report, exported_record_summary, failed_record_summary, \
    REPORT_TEMPLATES
from .exporters.pdf_exporter import PDFExporter
from .exporters.md_exporter import MarkdownExporter
from .exporters.csv_exporter import CSVExporter
from .utils.concat import REGISTRY as CONCAT_REGISTRY

LOG = logging.getLogger(__name__)


def run(input_data: Iterable[Any], output_format: str, output_directory: Optional[Union[str, Path]] = None,
        template_dir: str = None, template_name: str = None, output_filename: str = None):
    """ Wrapper that handles 1 to N inputs. The initial format of the inputs, the template (if any) used for rendering,
     the output format and the output directory must be the same for all inputs. Only one output is produced.
     Default behavior doesn't generate output file, it simply returns the rendered result. output file is generated
     only if output_directory is not None.

    Args:
        input_data:
        output_format:
        output_directory:
        template_dir:
        template_name:
        output_filename:

    Returns:

    """
    results = []
    exported_records = []
    failed_records = []
    if not input_data:
        raise ValueError("No inputs provided. Pass at least one input item.")
    for i, item in enumerate(input_data):
        info = normalize_input(item, index=i)
        try:
            result = _render_from_info(info, output_format, template_dir, template_name)
            results.append(result)
            exported_records.append(exported_record_summary(info, i))
        except Exception as ex:
            LOG.exception("Export rendering failed for record index %s", i)
            failed_records.append(failed_record_summary(info, ex, i))

    if not results and output_format not in REPORT_TEMPLATES:
        raise ValueError("No rendered results produced from the input data.")

    # Determine output name
    if output_filename is None:
        # Use the first rendered filename as a fallback
        if results:
            output_filename = results[0].get("filename", "combined")
        else:
            output_filename = "combined"

    # Concatenate via registry
    concat_fn = CONCAT_REGISTRY.get(output_format)
    if concat_fn is None:
        raise ValueError(f"Concatenation not supported for format '{output_format}'.")
    combined = concat_fn(results, output_filename) if results else None

    if output_format in REPORT_TEMPLATES:
        report_payload = build_report_payload(
            output_format,
            output_filename,
            exported_records,
            failed_records,
            template_name=template_name,
        )
        report_result = render_report(report_payload, output_format, template_dir)
        combined = concat_fn([report_result] + results, output_filename) if results else concat_fn([report_result],
                                                                                                   output_filename)

    # Default result
    if output_directory is None:
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
    return _render_from_info(info, output_format, template_dir, template_name)


def _render_from_info(info: Dict[str, Any], output_format: str, template_dir: str = None, template_name: str = None):
    input_type = info['input_type']
    payload = info['payload']
    filename = info['filename']

    # Select the right exporter
    output_format_key = (output_format or "").strip().lower()
    roots = [template_dir] if template_dir else None
    exporters: Dict[str, Any] = {
        "pdf": PDFExporter(template_roots=roots),
        "markdown": MarkdownExporter(template_roots=roots),
        "csv": CSVExporter(template_roots=roots),

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
