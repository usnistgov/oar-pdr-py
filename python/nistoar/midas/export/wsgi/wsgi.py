"""
v1 of the WSGI layer for the MIDAS Export service.

This service provides endpoints to export project records or JSON payloads
into supported formats (Markdown, PDF, etc.).

Usage (example):
- POST /midas/export/v1
- Body: {
            "output_format": "pdf",
            "inputs": ["proj-id-1", "proj-id-2", ...],
            "template_dir": "/midas/export/templates",
            "template_name": "dmp_pdf_template.prep",
            "output_dir": "/midas/export/out"
        }

"""
from __future__ import annotations
from logging import Logger
from typing import Mapping, Callable, Any, Dict
import json
from pathlib import Path

from nistoar.web.rest import ServiceApp, Handler
from ..export import run as run_export


class ExportHandler(Handler):
    """
    POST /export/v1
    Body:
      {
        "output_format": "pdf" | "markdown",
        "inputs": [...], # list of ProjectRecord ids (strings),
                         # or list of dicts with 'id' key and ProjectRecord id value,
                         # or list of dicts with 'data' key containing JSON project data.
        "template_dir": "...", # optional
        "template_name": "...", # optional
        "output_dir": "...", # requires server output dir where file will be created
        "output_filename": "...", # optional
        "generate_file": boolean # default is false
      }
    """
    def do_OPTIONS(self, path):
        return self.send_options(["POST"])

    def _get_project_record_by_id(self, dbcli, rec_id: str):
        """Get ProjectRecord object from id using DBClient"""
        if hasattr(dbcli, "select_records_by_ids"):
            for rec in dbcli.select_records_by_ids([rec_id]):
                return rec
            return None
        if hasattr(dbcli, "get_record"):
            return dbcli.get_record(rec_id)
        # fallback: scan select_records
        if hasattr(dbcli, "select_records"):
            for rec in dbcli.select_records():
                if getattr(rec, "id", None) == rec_id:
                    return rec
        return None

    def _resolve_inputs(self, inputs):
        """Resolve HTTP inputs into dicts or ProjectRecord objects"""
        resolved = []
        dbcli = getattr(self.app, "dbcli", None)  # DBClient attached to ExportApp if available
        for idx, item in enumerate(inputs):
            # Case 1: dict with JSON in data key
            if isinstance(item, dict) and "data" in item:
                resolved.append(item)
                continue

            # Case 2: project id
            if isinstance(item, str):
                if not dbcli:
                    raise RuntimeError("DB client is not configured on ExportApp")
                rec_id = item.strip()
                if not rec_id:
                    raise ValueError(f"inputs[{idx}]: empty project id")
                rec = self._get_project_record_by_id(dbcli, rec_id)
                if rec is None:
                    raise LookupError(f"Project record not found: {rec_id}")
                resolved.append(rec)
                continue

            # Case 3: dict with id key but no data
            if isinstance(item, dict) and "id" in item and "data" not in item:
                if not dbcli:
                    raise RuntimeError("DB client is not configured on ExportApp")
                rec_id = str(item["id"]).strip()
                if not rec_id:
                    raise ValueError(f"inputs[{idx}]: project record must include non-empty 'id'")
                rec = self._get_project_record_by_id(dbcli, rec_id)
                if rec is None:
                    raise LookupError(f"Project record not found: {rec_id}")
                resolved.append(rec)
                continue

            # any other shape is unsupported for export
            raise TypeError(f"inputs[{idx}]: unsupported input shape")
        return resolved

    def do_POST(self, path):
        if path.strip("/"):
            return self.send_error(404, "Not Found")

        bodyin = self._env.get("wsgi.input")
        if bodyin is None:
            return self.send_error(400, "Missing input", "Missing expected JSON body")

        try:
            raw = bodyin.read()
            data = json.loads(raw or b"{}")
        except Exception as ex:
            return self.send_error(400, "Input not parseable as JSON", f"JSON parse error: {ex}")

        output_format = (data.get("output_format") or "").strip().lower()
        if output_format not in {"pdf", "markdown"}:
            return self.send_error(400, "Bad Input", "output_format must be one of: pdf, markdown")

        inputs = data.get("inputs")
        if not isinstance(inputs, list) or not inputs:
            return self.send_error(400, "Bad Input", "inputs must be a non-empty array")

        generate_file = bool(data.get("generate_file") or False)
        output_dir = data.get("output_dir")

        if generate_file:
            if not isinstance(output_dir, str) or not output_dir.strip():
                return self.send_error(400, "Bad Input", "output_dir is required")

        template_dir = data.get("template_dir")
        template_name = data.get("template_name")
        output_filename = data.get("output_filename")

        # resolve IDs and other allowed input shapes
        try:
            resolved_inputs = self._resolve_inputs(inputs)
        except ValueError as ex:
            return self.send_error(400, "Bad Input", str(ex))
        except LookupError as ex:
            return self.send_error(404, "Not Found", str(ex))
        except TypeError as ex:
            return self.send_error(400, "Bad Input", str(ex))
        except RuntimeError as ex:
            return self.send_error(500, "Server Error", str(ex))

        # Safe Path
        output_dir_path = Path(output_dir) if output_dir else Path(".")

        try:
            result = run_export(
                input_data=resolved_inputs,
                output_format=output_format,
                output_directory=output_dir_path,
                template_dir=template_dir,
                template_name=template_name,
                output_filename=output_filename,
                generate_file=generate_file
            )
        except Exception as ex:
            if self.log:
                self.log.exception("export POST failed: %s", ex)
            return self.send_error(500, "Server Error")

        # Return file content as stream
        if not generate_file:
            filename = result.get("filename", "output")
            mimetype = result.get("mimetype", "application/octet-stream")

            # return file bytes as response body
            if "bytes" in result:
                body = result["bytes"]
                headers = [
                    ("Content-Type", mimetype),
                    ("Content-Disposition", f'inline; filename="{filename}"'),
                ]
                self._start("200 OK", headers)
                return [body]

            # return encoded text as response body
            elif "text" in result:
                body = result["text"].encode("utf-8")
                headers = [
                    ("Content-Type", f"{mimetype}; charset=utf-8"),
                    ("Content-Disposition", f'inline; filename="{filename}"'),
                ]
                self._start("200 OK", headers)
                return [body]

            else:
                return self.send_error(500, "Server Error", "No content to stream")

        return self.send_json(result)


class ExportApp(ServiceApp):
    """
    App that exposes /export/v1 (POST only).
    """
    def __init__(self, log: Logger, config: Mapping = {}):
        super().__init__("export", log, config)

    def create_handler(self, env: Mapping, start_resp: Callable, path: str, who) -> Handler:
        return ExportHandler(path or "", dict(env), start_resp, who, dict(self.cfg or {}), self.log, self)
