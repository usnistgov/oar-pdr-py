from __future__ import annotations
from typing import Dict, List
from io import BytesIO


def concat_markdown(rendered_results: List[Dict], output_filename: str) -> Dict:
    """
    Join multiple markdown texts.
    Rendered_results is a list of dicts that must contain the key 'text'
    """
    texts = []
    for result in rendered_results:
        txt = result.get("text")
        if not isinstance(txt, str):
            raise TypeError("concat_markdown expects all inputs to have a 'text' key.")
        texts.append(txt.rstrip() + "\n")

    combined_text = ("\n\n---\n\n").join(texts)

    return {
        "format": "markdown",
        "filename": f"{output_filename}.md" if not output_filename.endswith(".md") else output_filename,
        "mimetype": "text/markdown",
        "text": combined_text,
        "file_extension": ".md",
    }


def concat_pdf(rendered_results: List[Dict], output_filename: str) -> Dict:
    """
    Merge multiple PDF byte streams.
    Rendered_results is a list of dicts that must contain the key 'bytes'
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception as ex:
        raise RuntimeError("PDF concatenation requires 'pypdf' to be installed") from ex

    writer = PdfWriter()
    for result in rendered_results:
        data = result.get("bytes")
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("concat_pdf expects all inputs to have a 'bytes' key.")
        reader = PdfReader(BytesIO(data))
        for page in reader.pages:
            writer.add_page(page)

    buffer = BytesIO()
    writer.write(buffer)
    buffer.seek(0)
    pdf_bytes = buffer.read()

    return {
        "format": "pdf",
        "filename": f"{output_filename}.pdf" if not output_filename.endswith(".pdf") else output_filename,
        "mimetype": "application/pdf",
        "bytes": pdf_bytes,
        "file_extension": ".pdf",
    }


def _extract_csv_text(result: Dict) -> str:
    txt = result.get("text")
    if not isinstance(txt, str):
        raise TypeError("concat_csv expects all inputs to have a 'text' key.")
    return txt.strip()


def _concat_csv_data(rendered_results: List[Dict]) -> str:
    if not rendered_results:
        return ""
    csv_texts = [_extract_csv_text(result) for result in rendered_results]
    if not csv_texts:
        return ""
    # Extract header from first CSV and combine all data rows
    lines = csv_texts[0].split('\n')
    header = lines[0] if lines else ""
    combined_rows = []
    for csv_text in csv_texts:
        text_lines = csv_text.split('\n')
        # Skip header (first line) and add data rows
        for line in text_lines[1:]:
            if line.strip():  # Skip empty lines
                combined_rows.append(line)
    return header + '\n' + '\n'.join(combined_rows) if combined_rows else header


def concat_csv(rendered_results: List[Dict], output_filename: str) -> Dict:
    """
    Join multiple CSV texts by merging headers and combining rows.
    Rendered_results is a list of dicts that must contain the key 'text'
    """
    report_text = ""
    data_results = rendered_results
    if rendered_results and rendered_results[0].get("section") == "report":
        report_text = _extract_csv_text(rendered_results[0])
        data_results = rendered_results[1:]
    data_text = _concat_csv_data(data_results)
    if report_text and data_text:
        combined_csv = report_text + "\n\n" + data_text
    elif report_text:
        combined_csv = report_text
    else:
        combined_csv = data_text

    return {
        "format": "csv",
        "filename": f"{output_filename}.csv" if not output_filename.endswith(".csv") else output_filename,
        "mimetype": "text/csv",
        "text": combined_csv,
        "file_extension": ".csv",
    }


# Registry of exporters mapped to their respective concat function
REGISTRY = {
    "markdown": concat_markdown,
    "pdf": concat_pdf,
    "csv": concat_csv,
}
