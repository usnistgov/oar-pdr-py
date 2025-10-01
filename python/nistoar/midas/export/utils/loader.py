from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import secrets


def _load_json_source(source: Any):
    """
    Json loader: Takes a path to an existing .json file and loads it.
    It also accepts a dict but will return as is.

    Args:
        source:

    Returns:

    """
    if isinstance(source, dict):
        # Assumes json is already loaded
        return source
    if isinstance(source, Path):
        if not source.exists():
            raise FileNotFoundError(f"JSON file not found: {source}")
        if source.suffix.lower() != ".json":
            raise TypeError(f"Expected a .json file, got: {source}")
        return json.loads(source.read_text(encoding="utf-8"))
    raise TypeError("For input_type='json', pass a dict or a path to a .json file.")


def _set_filename_output(item: Any, index: int):
    """
    Item is either a path to a file or a loaded json,
    otherwise the output filename is defined with a fallback.

    Args:
        item:
        index:

    Returns:

    """
    if isinstance(item, dict) and item.get("output_filename"):
        return str(item["output_filename"])
    if isinstance(item, Path):
        return Path(item).stem
    # fallback
    random_key = secrets.token_hex(nbytes=4)
    return f"record_{random_key}_{index}"


def normalize_input(item: Any, index: int):
    """
    Normalize the input item into a dict with the keys `input_type`, `payload`, `output_filename`.
    Item must be either:
        - a dict with the keys `input_type` and `source`
        - a loaded json
        - path ending with `.json`

    Args:
        item:
        index:

    Returns:

    """
    # Case 1: dict with the keys `input_type` and `source`
    if isinstance(item, dict) and "input_type" in item and "source" in item:
        input_type = str(item["input_type"]).lower()
        if input_type != "json":
            raise TypeError(
                f"Unsupported input_type '{input_type}'. Only 'json' is implemented here."
            )
        payload = _load_json_source(item["source"])
        output_filename = _set_filename_output(item, index)
        return {
            'input_type': input_type,
            'payload': payload,
            'output_filename': output_filename,
            }

    # Case 2: loaded json
    if isinstance(item, dict):
        return {
            'input_type': "json",
            'payload': item,
            'output_filename': _set_filename_output(item, index),
            }

    # Case 3: Path to a `.json`
    if isinstance(item, Path) and str(item).lower().endswith(".json"):
        return {
            'input_type': "json",
            'payload': _load_json_source(item),
            'output_filename': _set_filename_output(item, index),
        }

    # Unsupported
    raise TypeError(
        "Unsupported input. First argument must either be a dict with the keys `input_type` and `source`, "
        "a loaded json (dict), or path ending with `.json`."
    )
