"""Fixtures shared by ``tests/integration/cli/``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from types import ModuleType

import pytest

from support.disk_fs_sibling import no_disk_fs_sibling as no_disk_fs_sibling
from synology_apm_repo.cli import repo_session
from synology_apm_repo.sdk.storage.base import ObjectStore


@pytest.fixture
def patch_profile_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> Callable[..., None]:
    """``patch_profile_store(fixture_name, module=cli.repo_session,
    allow_content=False)`` monkeypatches ``module``'s own ``store_from_profile`` name to
    return ``record_target(fixture_name, allow_content=allow_content)``
    for any profile name. Call it once per module that imports
    ``store_from_profile``: every command opens its repository through
    ``cli.repo_session``, except ``dump``, which resolves its own store, so
    ``test_cli_commands_dump.py`` patches ``cli.commands.dump`` instead.

    Routes through ``record_target`` so these tests get ``--record-against``
    support and its real-content recording guard."""

    def _patch(fixture_name: str, module: ModuleType = repo_session, *, allow_content: bool = False) -> None:
        async def _resolve(name: str) -> ObjectStore:
            return await record_target(fixture_name, allow_content=allow_content)

        monkeypatch.setattr(module, "store_from_profile", _resolve)

    return _patch
