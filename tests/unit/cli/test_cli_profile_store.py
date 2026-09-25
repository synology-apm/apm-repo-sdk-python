"""Unit tests for ``synology_apm_repo.cli.profile_store.resolve_profile_store``'s
real body — every ``--profile``-driven CLI test (``test_cli_profile_option.py``
and friends) monkeypatches this function out entirely, since ``ls``/``doctor``/
``key``/``verify`` are all built on ``cli.repo_session.opened_repo()``, which
imports and calls ``resolve_profile_store`` as a bare module-level name, so
those tests patch ``cli.repo_session.resolve_profile_store`` rather than
exercising the real body. Its one-line delegation to
``synology_apm_repo.sdk.profiles.build_store`` is otherwise exercised
nowhere in this suite."""

from __future__ import annotations

from pathlib import Path

import pytest

import synology_apm_repo.sdk.profiles.config_file as config_file_module
from synology_apm_repo.cli.profile_store import resolve_profile_store
from synology_apm_repo.sdk.errors import ProfileNotFoundError
from synology_apm_repo.sdk.profiles import BackendKind, save_profile
from synology_apm_repo.sdk.storage.s3 import S3Store


@pytest.fixture(autouse=True)
def _default_config_dir_is_tmp_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # resolve_profile_store() calls build_store() with no config_dir
    # override, i.e. the real default per-user location — redirect that
    # default to tmp_path so this test never touches the real user config.
    monkeypatch.setattr(config_file_module, "default_config_dir", lambda: tmp_path)


async def test_resolve_profile_store_builds_the_real_store(fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.S3,
        {"bucket": "b", "endpoint": "http://minio:9000", "access_key": "AKIA", "secret_key": "shh"},
    )
    store = await resolve_profile_store("demo")
    assert isinstance(store, S3Store)


async def test_resolve_profile_store_missing_raises_profile_not_found() -> None:
    with pytest.raises(ProfileNotFoundError):
        await resolve_profile_store("nope")


__all__: list[str] = []
