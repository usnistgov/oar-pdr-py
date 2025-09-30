from __future__ import annotations
from typing import Any, Dict
from pathlib import Path


def write_file(output_directory: Path, result: Dict):
    """
    Write one rendered output file to `output_directory` and return the absolute path as a string.

    Args:
        output_directory:
        result:

    Returns:

    """
    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / result["filename"]
    # If the result carries `bytes`, write in binary mode
    if result.get("bytes") is not None:
        output_path.write_bytes(result["bytes"])  # binary
    # If it carries `text`, write in UTF-8 text mode.
    elif result.get("text") is not None:
        output_path.write_text(result["text"], encoding="utf-8")  # text
    # Otherwise, raise error
    else:
        raise ValueError("Render result has neither 'bytes' nor 'text' content.")

    return str(output_path)
