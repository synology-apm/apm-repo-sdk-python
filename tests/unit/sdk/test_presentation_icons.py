"""Unit tests for ``synology_apm_repo.sdk.presentation.icons``."""

from __future__ import annotations

from synology_apm_repo.sdk.presentation.icons import FILE_STATE_ICON, file_state_suffix
from synology_apm_repo.sdk.units.base import FileState


def test_file_state_icon_has_an_entry_for_every_file_state_member() -> None:
    """Catches the day a new ``FileState`` member is added without a
    matching icon — ``file_state_suffix``'s own ``.get(value, "")``
    would otherwise silently render no icon for it forever."""
    assert set(FILE_STATE_ICON) == {state.value for state in FileState}


def test_file_state_suffix_is_empty_for_normal_and_unrecognized_values() -> None:
    assert file_state_suffix(FileState.NORMAL.value) == ""
    assert file_state_suffix("not-a-real-file-state") == ""


def test_file_state_suffix_prefixes_a_leading_space() -> None:
    assert file_state_suffix(FileState.CLOUD_ONLY.value) == " ☁"
    assert file_state_suffix(FileState.ENCRYPTED.value) == " 🔒"
