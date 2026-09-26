"""Shared fixtures for ``tests/integration/cli/`` -- ``patch_profile_store``
is used nowhere outside this directory."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from types import ModuleType

import pytest

from synology_apm_repo.sdk.storage.base import ObjectStore


@pytest.fixture
def patch_profile_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> Callable[..., None]:
    """``patch_profile_store(fixture_name, module, allow_content=False)``
    monkeypatches ``module``'s own ``resolve_profile_store`` name to
    return ``record_target(fixture_name, allow_content=allow_content)``
    for any profile name. Call it once per module that needs patching —
    most files patch only ``cli.repo_session``; ``test_cli_export.py``
    patches ``cli.commands.export`` instead, and
    ``test_cli_canonical_ref_roundtrip.py`` patches both.

    Routes through ``record_target`` (not a bare ``ReplayStore.from_path``)
    so these CLI tests get ``--record-against`` support and its real-content
    recording guard for free; ``allow_content`` forwards straight through
    for the rare CLI test that genuinely needs one (e.g. ``cli cat`` reading
    a real MBR/GPT header as a structural oracle)."""

    def _patch(fixture_name: str, module: ModuleType, *, allow_content: bool = False) -> None:
        async def _resolve(name: str) -> ObjectStore:
            return await record_target(fixture_name, allow_content=allow_content)

        monkeypatch.setattr(module, "resolve_profile_store", _resolve)

    return _patch
