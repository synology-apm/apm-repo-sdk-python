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


def profile_args(ref: RepresentativeRef) -> list[str]:
    """``--profile <name>`` for a ``ProfileSample``-derived ref, ``[]``
    otherwise -- needed alongside ``ref.repo_path``'s store-relative form
    to reopen a profile-based remote sample from a fresh subprocess (see
    ``RepresentativeRef.profile``'s own docstring)."""
    return ["--profile", ref.profile] if ref.profile else []


def common_args(ref: RepresentativeRef) -> list[str]:
    """``profile_args(ref)`` plus ``key_args(ref)`` -- the flags every
    read-only command needs appended after its ``REPO``/ref positional
    argument, regardless of which of ``_samples.py``'s three sample kinds
    ``ref`` came from."""
    return [*profile_args(ref), *key_args(ref)]
