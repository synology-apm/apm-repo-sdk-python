"""JSON parsing for untrusted on-disk content: the ``parse_*`` functions raise
``DataCorruptError`` instead of a bare ``json.JSONDecodeError`` or a later
``AttributeError`` on an unexpected top-level type; the ``try_parse_*``
functions return ``None`` instead, for best-effort enrichment fields whose
drift must not fail the caller."""

from __future__ import annotations

import json
from typing import Any

from ..errors import DataCorruptError

# ValueError covers JSONDecodeError, UnicodeDecodeError and an integer
# literal longer than sys.get_int_max_str_digits(); RecursionError, nesting
# too deep for the decoder.
_JSON_ERRORS = (ValueError, RecursionError)


def _parse(raw: str | bytes, what: str, ref: str | None) -> object:
    try:
        return json.loads(raw)
    except _JSON_ERRORS as exc:
        raise DataCorruptError(f"{what} did not parse as JSON: {exc}", ref=ref) from exc


def parse_json_object(raw: str | bytes, what: str, *, ref: str | None = None) -> dict[str, Any]:
    """``raw`` parsed as a JSON object. ``what`` names the value in the error
    message (e.g. ``"workload_spec"``); field values stay ``Any`` for the
    caller to narrow."""
    parsed = _parse(raw, what, ref)
    if not isinstance(parsed, dict):
        raise DataCorruptError(f"{what} is a JSON {type(parsed).__name__}, expected an object", ref=ref)
    return parsed


def parse_json_array(raw: str | bytes, what: str, *, ref: str | None = None) -> list[Any]:
    """``raw`` parsed as a JSON array; see ``parse_json_object``."""
    parsed = _parse(raw, what, ref)
    if not isinstance(parsed, list):
        raise DataCorruptError(f"{what} is a JSON {type(parsed).__name__}, expected an array", ref=ref)
    return parsed


def json_int(value: object) -> int | None:
    """``value`` if it is a JSON integer (not a ``bool``), else ``None``."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def json_object(value: object) -> dict[str, Any]:
    """``value`` if it is a JSON object, else an empty one."""
    return value if isinstance(value, dict) else {}


def json_bool(value: object) -> bool | None:
    """``value`` if it is a JSON ``true``/``false``, else ``None``."""
    return value if isinstance(value, bool) else None


def _try_parse(raw: object) -> object:
    if not isinstance(raw, (str, bytes, bytearray)) or not raw:
        return None
    try:
        parsed: object = json.loads(raw)
        return parsed
    except _JSON_ERRORS:
        return None


def try_parse_json_object(raw: object) -> dict[str, Any] | None:
    """``raw`` parsed as a JSON object, or ``None`` when it is not non-empty
    ``str``/``bytes``/``bytearray``, not valid JSON, or not an object at the top level."""
    parsed = _try_parse(raw)
    return parsed if isinstance(parsed, dict) else None


def try_parse_json_array(raw: object) -> list[Any] | None:
    """``raw`` parsed as a JSON array, or ``None``; see ``try_parse_json_object``."""
    parsed = _try_parse(raw)
    return parsed if isinstance(parsed, list) else None
