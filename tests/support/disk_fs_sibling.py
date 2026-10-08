"""``no_disk_fs_sibling``, a pytest fixture shared by ``tests/unit/sdk/`` and
``tests/integration/cli/``: each directory's ``conftest.py`` imports it by
name, and a module opts in with ``pytest.mark.usefixtures``."""

from __future__ import annotations

import pytest

import synology_apm_repo.sdk.units.device as device_module
import synology_apm_repo.sdk.units.device_pcps as device_pcps_module


@pytest.fixture
def no_disk_fs_sibling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disables the additive "(filesystem)" sibling node so children of a
    device are exactly its disk-image/file objects; a test of that sibling
    re-enables it. ``device.py`` and ``device_pcps.py`` each import
    ``disk_fs_available``, so both bindings are patched."""
    monkeypatch.setattr(device_module, "disk_fs_available", lambda: False)
    monkeypatch.setattr(device_pcps_module, "disk_fs_available", lambda: False)
