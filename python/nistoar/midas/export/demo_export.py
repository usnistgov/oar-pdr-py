from __future__ import annotations

from pathlib import Path
from nistoar.midas.export.export import export, run
import json
import sys


class DemoProjectRecord:
    def __init__(self, *, data: dict, meta: dict | None = None, name: str | None = None, rec_id: str | None = None):
        self.data = data
        self.meta = meta or {}
        if name is not None:
            self.name = name
        if rec_id is not None:
            self.id = rec_id


def load_demo_record(json_path: Path, *, name: str | None = None) -> DemoProjectRecord:
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    data = raw.get("data", raw)
    meta = raw.get("meta", {})
    auto_name = (
        (data.get("title") or data.get("name") or json_path.stem)
        if isinstance(data, dict) else json_path.stem
    )
    return DemoProjectRecord(data=data, meta=meta, name=name or auto_name)


def iter_demo_records(json_path: Path, count: int = 3):
    for i in range(count):
        rec = load_demo_record(json_path, name=f"exampleDMP_{i+1}")
        yield rec


def record_to_wire(rec: DemoProjectRecord) -> dict:
    out = {
        "data": rec.data,
        "meta": rec.meta,
    }
    if hasattr(rec, "name"):
        out["name"] = rec.name
    if hasattr(rec, "id"):
        out["id"] = rec.id
    return out


def post_export_via_wsgi(
    *,
    base_url: str,
    output_format: str, # "pdf"
    records: list[DemoProjectRecord],
    templates_dir: Path,
    output_directory: Path,
    template_name: str | None = None
):
    import requests

    body = {
        "output_format": output_format,
        "inputs": [record_to_wire(r) for r in records],
        "template_dir": str(templates_dir),
        "output_dir": str(output_directory),
    }
    if template_name:
        body["template_name"] = template_name

    url = f"{base_url.rstrip('/')}/midas/export/v1"
    r = requests.post(url, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    # project paths
    project_root = Path(__file__).resolve().parent
    templates_dir = project_root / "templates"
    output_directory = project_root / "out"

    # Example JSON file.
    example_json = project_root / "data/exampleDMP.json"
    if not example_json.exists():
        print(f"ERROR: Cannot find example file: {example_json}")
        sys.exit(1)

    # Single export: one ProjectRecord to one PDF
    print("\nSingle: Exporting one ProjectRecord to PDF...")
    try:
        rec = load_demo_record(example_json, name="exampleDMP_single")
        single_result = export(
            input_item=rec,
            output_format="pdf",
            output_directory=output_directory,
            template_dir=str(templates_dir),
            template_name="dmp_pdf_template.prep",
        )
        print("Wrote:", single_result["path"])
    except Exception as exc:
        print("Single export failed:", repr(exc))
        sys.exit(2)

    # Batch export: iterator (generator) of ProjectRecords to PDFs
    print("\nBatch: Exporting multiple ProjectRecords to PDF...")
    try:
        rec_items = list(iter_demo_records(example_json, count=3))
        batch_results = run(
            input_data=rec_items,
            output_format="pdf",
            output_directory=output_directory,
            template_dir=str(templates_dir),
            template_name="dmp_pdf_template.prep",
        )
        for i, res in enumerate(batch_results, start=1):
            print(f"[{i}] Wrote: {res['path']}")
    except Exception as exc:
        print("Batch export failed:", repr(exc))
        sys.exit(3)

    # Single export: one ProjectRecord to one Markdown (with template)
    print("\nSingle: Exporting one ProjectRecord to Markdown (template)...")
    try:
        rec = load_demo_record(example_json, name="exampleDMP_single_md")
        md_single_tpl = export(
            input_item=rec,
            output_format="markdown",
            output_directory=output_directory,
            template_dir=str(templates_dir),
            template_name="dmp_md_template.prep",
        )
        print("Wrote:", md_single_tpl["path"])
    except Exception as exc:
        print("Single Markdown (template) failed:", repr(exc))
        sys.exit(4)

    # Batch export: iterator to Markdown (with template)
    print("\nBatch: Exporting multiple ProjectRecords to Markdown (template)...")
    try:
        md_batch_tpl = run(
            input_data=rec_items,
            output_format="markdown",
            output_directory=output_directory,
            template_dir=str(templates_dir),
            template_name="dmp_md_template.prep",
        )
        for i, res in enumerate(md_batch_tpl, start=1):
            print(f"[MD {i}] Wrote: {res['path']}")
    except Exception as exc:
        print("Batch Markdown (template) failed:", repr(exc))
        sys.exit(5)

    # POST request to export projectRecord to PDF
    try:
        base_url = "http://localhost:9091"
        rec_for_post = load_demo_record(example_json, name="exampleDMP_wsgi")

        # PDF via WSGI
        resp_pdf = post_export_via_wsgi(
            base_url=base_url,
            output_format="pdf",
            records=[rec_for_post],
            templates_dir=templates_dir,
            output_directory=output_directory,
            template_name="dmp_pdf_template.prep",
        )
        print("WSGI PDF POST: ", resp_pdf)

    except Exception as exc:
        print("WSGI POST failed:", repr(exc))

    print("\nDone.\n")




if __name__ == "__main__":
    main()
