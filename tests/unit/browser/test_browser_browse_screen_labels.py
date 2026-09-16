"""Unit tests for ``browser.workload_grouping``'s and
``browser.repo_labels``'s pure label-formatting
helpers — faster and more targeted than routing every case through a
full Pilot walkthrough; ``tests/integration/browser/test_browser_pilot_labels.py``
still covers the real, end-to-end version against real recorded sample
data."""

from __future__ import annotations

from typing import cast

import pytest

from synology_apm_repo.browser.repo_labels import (
    _catalog_label,
    _repo_label,
    _repo_path_component,
    _strip_internal_marker,
)
from synology_apm_repo.browser.workload_grouping import _TYPE_LABELS, _humanize_type
from synology_apm_repo.sdk.api import Catalog, KeyStatus, Repository
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_SAAS_SUB_TYPES


def _fake_repo(*, repo_root: str, key_status: KeyStatus) -> Repository:
    """A real ``Repository``, backed by a placeholder store/layout —
    ``Repository.__init__`` does no I/O itself (``catalog_repo_layouts()``
    is a pure function of ``layout``), so a bare placeholder store/keys is
    enough to drive these pure functions, avoiding a real (and here
    pointless) repository open."""
    keys: object | None = None
    key_verification: object | None = None
    encrypted: bool | None = None
    # key_status/is_encrypted are trivial projections of the resolved
    # _key_status Repository.__init__ computes from keys/key_verification/
    # encrypted — construct whichever combination of those three inputs
    # yields the state under test, the same way a real caller would have
    # arrived at it.
    if key_status is KeyStatus.NOT_ENCRYPTED:
        keys = _FakeKeys(is_no_encryption=True)
    elif key_status is KeyStatus.VERIFIED:
        keys = _FakeKeys(is_no_encryption=False)
        key_verification = _FakeVerification(ok=True)
    elif key_status is KeyStatus.INVALID:
        keys = _FakeKeys(is_no_encryption=False)
        key_verification = _FakeVerification(ok=False)
    else:
        # KeyStatus.NO_KEY_PROVIDED: keys stays None, and encrypted
        # must be True (confirmed encrypted, no key tried) — never left
        # at None (the "couldn't tell" edge case), since that's not what
        # this state is standing in for in any of this file's tests.
        assert key_status is KeyStatus.NO_KEY_PROVIDED
        encrypted = True
    return Repository(
        cast(ObjectStore, object()),
        RepositoryLayout(kind=RepoKind.VAULT, repo_root=repo_root),
        keys,  # type: ignore[arg-type]
        key_verification,  # type: ignore[arg-type]
        encrypted=encrypted,
    )


class _FakeKeys:
    def __init__(self, *, is_no_encryption: bool) -> None:
        self.is_no_encryption = is_no_encryption


class _FakeVerification:
    def __init__(self, *, ok: bool) -> None:
        self.ok = ok


class TestStripInternalMarker:
    def test_bare_vault_marker_strips_to_empty(self) -> None:
        assert _strip_internal_marker("@ActiveProtectVault") == ""

    def test_object_store_marker_keeps_the_repo_id(self) -> None:
        assert _strip_internal_marker("@ActiveProtectData/gqDuTMuityBf") == "gqDuTMuityBf"

    def test_unrelated_path_passes_through_unchanged(self) -> None:
        assert _strip_internal_marker("some/other/path") == "some/other/path"

    def test_marker_nested_one_level_down_still_strips(self) -> None:
        """The marker is stripped by segment, not just as a leading
        prefix of ``repo_root``, so a real subdirectory name nested in
        front of it (e.g. ``"apv-sample-1/@ActiveProtectVault"``)
        survives while only the marker segment is removed."""
        assert _strip_internal_marker("apv-sample-1/@ActiveProtectVault") == "apv-sample-1"

    def test_object_store_marker_nested_one_level_down_keeps_both_real_segments(self) -> None:
        assert (
            _strip_internal_marker("s3-sample-2-encrypted/@ActiveProtectData/BikXpRbFNGI1")
            == "s3-sample-2-encrypted/BikXpRbFNGI1"
        )


class TestRepoPathComponent:
    def test_single_vault_shows_just_the_scanned_directory_name(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_path_component(repo, "/Users/someone/samples/apv-sample-1")
        assert label == "apv-sample-1"
        assert "@ActiveProtectVault" not in label

    def test_object_store_appends_the_repo_id_after_the_directory_name(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectData/gqDuTMuityBf", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_path_component(repo, "/Users/someone/samples/sample-1")
        assert label == "sample-1/gqDuTMuityBf"
        assert "@ActiveProtectData" not in label

    def test_trailing_slash_in_scan_path_does_not_leave_an_empty_name(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_path_component(repo, "/Users/someone/samples/apv-sample-1/")
        assert label == "apv-sample-1"

    def test_scanning_a_directory_of_multiple_repos_still_hides_the_marker(self) -> None:
        """Scanning the parent of several sample repositories nests a real
        subdirectory name in front of the marker in ``repo_root``; the
        marker is stripped by segment, not just leading-prefix, so the
        real neighboring segment survives."""
        repo = _fake_repo(repo_root="apv-sample-1/@ActiveProtectVault", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_path_component(repo, "/Users/someone/samples")
        assert label == "samples/apv-sample-1"
        assert "@ActiveProtectVault" not in label


class TestRepoLabel:
    def test_no_key_provided_shows_key_needed(self) -> None:
        """NO_KEY_PROVIDED is only reachable once discover() has resolved
        it, so the hint is honest here."""
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_label(repo, "/samples/apv-sample-2-encrypted", verbose=False)
        assert label == "apv-sample-2-encrypted · key needed"

    def test_not_encrypted_shows_that_fact_once_known(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.NOT_ENCRYPTED)
        label = _repo_label(repo, "/samples/apv-sample-1", verbose=False)
        assert label == "apv-sample-1 · not encrypted"

    def test_verified_key_shows_that_fact(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.VERIFIED)
        label = _repo_label(repo, "/samples/apv-sample-2-encrypted", verbose=False)
        assert label == "apv-sample-2-encrypted · key verified"

    def test_invalid_key_shows_that_fact(self) -> None:
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.INVALID)
        label = _repo_label(repo, "/samples/apv-sample-2-encrypted", verbose=False)
        assert label == "apv-sample-2-encrypted · invalid key"

    def test_verbose_mode_appends_layout_regardless_of_key_status(self) -> None:
        # No per-repository uuid any more -- that moved to per-catalog (see
        # Catalog.info) -- only the layout-kind suffix is added here.
        repo = _fake_repo(repo_root="@ActiveProtectVault", key_status=KeyStatus.NO_KEY_PROVIDED)
        label = _repo_label(repo, "/samples/apv-sample-1", verbose=True)
        assert "layout: vault" in label
        assert "apv-sample-1" in label


class _FakeCatalogInfo:
    def __init__(self, *, uuid: str) -> None:
        self.uuid = uuid


class _FakeConnectionForLabel:
    def __init__(self, *, connection_id: str) -> None:
        self.connection_id = connection_id


class _FakeCatalogForLabel:
    """Duck-typed stand-in for ``api.Catalog`` — ``_catalog_label`` only
    ever reads ``.info.uuid``/``.connection.connection_id``."""

    def __init__(self, *, uuid: str, connection_id: str) -> None:
        self.info = _FakeCatalogInfo(uuid=uuid)
        self.connection = _FakeConnectionForLabel(connection_id=connection_id)


class TestCatalogLabel:
    def test_normal_mode_returns_the_name_unchanged(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid", connection_id="fake-conn-id"))
        assert _catalog_label(catalog, "Source 1", verbose=False) == "Source 1"

    def test_verbose_mode_appends_uuid_and_connection_id(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid-0000", connection_id="fake-conn-id"))
        label = _catalog_label(catalog, "Source 1", verbose=True)
        assert label.startswith("Source 1 (")
        assert "uuid: fake-uuid-0000" in label
        assert "id: fake-conn-id" in label


class TestHumanizeType:
    @pytest.mark.parametrize(
        ("type_hint", "expected"),
        [
            # Device — unchanged, already correct acronyms.
            ("VM", "VM"),
            ("FS", "FS"),
            ("PC", "PC"),
            ("PS", "PS"),
            # GWS (Google Workspace).
            ("MAIL", "Mail"),
            ("CALENDAR", "Calendars"),
            ("CONTACT", "Contacts"),
            ("DRIVE", "Drives"),
            ("TEAM_DRIVE", "Shared Drives"),
            # M365 (Microsoft 365).
            ("USER_EXCHANGE", "Exchange"),
            ("USER_DRIVE", "OneDrive"),
            ("USER_CHAT", "Chat"),
            ("GROUP_EXCHANGE", "Groups"),
            ("SITE", "SharePoint"),
            ("TEAMS", "Teams"),
        ],
    )
    def test_every_real_dispatched_sub_type_gets_its_proper_vendor_name(self, type_hint: str, expected: str) -> None:
        # This list is exactly units/dispatch.py's own
        # _SAAS_SUB_TYPE_CANDIDATES key set (11 tokens) plus the 4
        # Device workload_type values — see
        # test_type_labels_covers_every_real_saas_sub_type below for the
        # actual, enforced cross-check against that source of truth (this
        # parametrize list is a hand-maintained mirror of it, not a
        # substitute).
        assert _humanize_type(type_hint) == expected

    def test_an_unrecognized_future_token_falls_back_to_the_raw_string(self) -> None:
        assert _humanize_type("SOME_FUTURE_TYPE") == "SOME_FUTURE_TYPE"

    def test_type_labels_covers_every_real_saas_sub_type(self) -> None:
        """``_TYPE_LABELS``' own comment already claims to mirror
        ``units/dispatch.py``'s ``_SAAS_SUB_TYPE_CANDIDATES`` ("the
        source of truth for which 11 tokens are real") — but nothing
        previously checked that claim against the SDK's own real
        dispatch table; a 12th real sub_type token could have been added
        there and this TUI-layer table would silently keep falling back
        to the raw wire token (``_humanize_type``'s own documented
        behavior for an unrecognized token) instead of failing loudly.
        Checked against ``SUPPORTED_SAAS_SUB_TYPES`` directly — the real
        source of truth, not a second hand-maintained copy of it."""
        assert _TYPE_LABELS.keys() >= SUPPORTED_SAAS_SUB_TYPES


__all__: list[str] = []
