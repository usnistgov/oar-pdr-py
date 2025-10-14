from __future__ import annotations
from collections.abc import Mapping as MappingABC

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
    Item is either a path to a file or a loaded json or a project record,
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
    if _is_project_record(item):
        name = _name_from_project_record(item)
        if name is not None:
            return name
    # fallback
    random_key = secrets.token_hex(nbytes=4)
    return f"record_{random_key}_{index}"


def _is_project_record(obj: Any):
    """ Check if argument is a Project Record and return a boolean

    Args:
        obj:

    Returns:

    """
    # obj has .data that are mappings
    return (
        hasattr(obj, "data") and isinstance(getattr(obj, "data"), MappingABC)
    )


def _name_from_project_record(rec: Any):
    """ Return project record name, fallback to the ID if present

    Args:
        rec:

    Returns:

    """
    name = getattr(rec, "name", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    rec_id = getattr(rec, "id", None)
    if isinstance(rec_id, str) and rec_id.strip():
        return rec_id.strip()
    return None


def normalize_input(item: Any, index: int):
    """
    Normalize the input item into a dict with the keys `input_type`, `payload`, `output_filename`.
    Item must be either:
        - a dict with the keys `input_type` and `source`
        - a loaded json
        - path ending with `.json`
        - a ProjectRecord object

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

    # Case 4: ProjectRecord
    if _is_project_record(item):
        # Build a mapping that contains a 'data' key for the template
        payload = {
            "data": dict(item.data)
        }
        return {
            "input_type": "json",
            "payload": payload,
            "output_filename": _set_filename_output(item, index),
        }

    # Unsupported
    raise TypeError(
        "Unsupported input. First argument must either be a dict with the keys `input_type` and `source`, "
        "a loaded json (dict), or path ending with `.json`."
    )
