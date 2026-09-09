from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..exporters.pdf_exporter import PDFExporter
from ..exporters.md_exporter import MarkdownExporter
from ..exporters.csv_exporter import CSVExporter


REPORT_TEMPLATES = {
    "pdf": "export_report_pdf_template.prep",
    "markdown": "export_report_markdown_template.prep",
    "csv": "export_report_csv_template.prep",
}
DEFAULT_TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "templates"


def _export_label(total: int, template_name: str | None = None,
                  output_format: str | None = None) -> str:
    name = (template_name or "").lower()
    if not name and output_format:
        defaults = {
            "pdf": "dmp_pdf_template.prep",
            "markdown": "dmp_markdown_template.prep",
            "csv": "dmp_csv_template.prep",
        }
        name = defaults.get(output_format, "")

    if "dap" in name:
        label = "DAP record(s)"
    elif "dmp" in name:
        label = "DMP record(s)"
    else:
        label = "record(s)"

    return f"Exporting {total} {label}"


def build_report_payload(output_format: str, output_filename: str, exported: List[Dict[str, Any]],
                         failed: List[Dict[str, Any]], template_name: str | None = None):
    total = len(exported) + len(failed)
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "export_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "output_format": output_format,
        "output_filename": output_filename,
        "export_label": _export_label(total, template_name=template_name, output_format=output_format),
        "exported": exported,
        "failed": failed,
        "summary": {
            "total_inputs": total,
            "exported_count": len(exported),
            "failed_count": len(failed),
        },
    }


def render_report(report_payload: Dict[str, Any], output_format: str, template_dir: str = None):
    template_name = REPORT_TEMPLATES.get(output_format)
    if not template_name:
        raise ValueError(f"No report template defined for format '{output_format}'.")

    roots = []
    if template_dir:
        roots.append(template_dir)
    roots.append(DEFAULT_TEMPLATE_ROOT)
    if output_format == "markdown":
        exporter = MarkdownExporter(template_roots=roots)
    elif output_format == "pdf":
        exporter = PDFExporter(template_roots=roots)
    elif output_format == "csv":
        exporter = CSVExporter(template_roots=roots)
    else:
        raise ValueError(f"Report rendering not supported for format '{output_format}'.")

    result = exporter.render(
        input_type="json",
        payload={"data": report_payload},
        filename="export_report",
        template_name=template_name,
    )
    if output_format == "csv":
        result["section"] = "report"
    return result


def exported_record_summary(info: Dict[str, Any], index: int):
    payload = info.get("payload", {})
    filename = info.get("filename")
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}

    title = ""
    if isinstance(payload, dict):
        title = data.get("title") or data.get("name") or payload.get("name") or ""
    if not title:
        title = filename or ""

    rec_id = ""
    if isinstance(payload, dict):
        rec_id = payload.get("id") or ""
    if not rec_id:
        rec_id = data.get("@id") or data.get("id") or ""
    return {
        "index": index + 1,
        "title": str(title).strip(),
        "id": str(rec_id).strip(),
        "filename": filename or "",
    }


def failed_record_summary(info: Dict[str, Any], ex: Exception, index: int):
    summary = exported_record_summary(info, index)
    summary["reason"] = friendly_error(ex)
    return summary


def friendly_error(ex: Exception) -> str:
    if isinstance(ex, FileNotFoundError):
        return "Template file not found."
    if isinstance(ex, KeyError):
        return "Record is missing required fields."
    if isinstance(ex, TypeError):
        return "Record data is not compatible with the exporter."
    if isinstance(ex, ValueError):
        return "Record data is not valid for export."
    return "Export failed during rendering."
