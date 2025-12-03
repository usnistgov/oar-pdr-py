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


# Registry of exporters mapped to their respective concat function
REGISTRY = {
    "markdown": concat_markdown,
    "pdf": concat_pdf,
}
