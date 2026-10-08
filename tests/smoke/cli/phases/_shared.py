"""Small helpers shared across ``cli/`` phase modules."""

from __future__ import annotations

import json

from ..._shared_refs import RepresentativeRef


def parses_as_json(text: str) -> bool:
    try:
        json.loads(text)
    except json.JSONDecodeError:
        return False
    return True


def key_args(ref: RepresentativeRef) -> list[str]:
    return ["--key", ref.key] if ref.key else []
