"""
v1 of the WSGI layer for the MIDAS Export service.

This service provides endpoints to export project records or JSON payloads
into supported formats (Markdown, PDF, etc.).

Usage (example):
- POST /midas/export/v1
- Body: {
            "output_format": "pdf",
            "inputs": [ProjectRecord_object1, ProjectRecord_object2, ProjectRecord_object3, etc.],
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
        "inputs": [...], # whatever iterable of items with the item supported by ``utils.loader.normalize_input``
        "template_dir": "...", # optional
        "template_name": "...", # optional
        "output_dir": "...", # requires server output dir where file will be created
        "output_filename": "...", # optional
        "generate_file": boolean # default is false
      }
    """
    def do_OPTIONS(self, path):
        return self.send_options(["POST"])

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

        # Safe Path
        output_dir_path = Path(output_dir) if output_dir else Path(".")

        try:
            result = run_export(
                input_data=inputs,
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
