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
from synology_apm_repo.sdk.api import Catalog, KeyStatus
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_SAAS_SUB_TYPES


def _layout(repo_root: str) -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.VAULT, repo_root=repo_root)


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
        label = _repo_path_component(_layout("@ActiveProtectVault"), "/Users/someone/samples/apv-sample-1")
        assert label == "apv-sample-1"
        assert "@ActiveProtectVault" not in label

    def test_object_store_appends_the_repo_id_after_the_directory_name(self) -> None:
        label = _repo_path_component(_layout("@ActiveProtectData/gqDuTMuityBf"), "/Users/someone/samples/sample-1")
        assert label == "sample-1/gqDuTMuityBf"
        assert "@ActiveProtectData" not in label

    def test_trailing_slash_in_scan_path_does_not_leave_an_empty_name(self) -> None:
        label = _repo_path_component(_layout("@ActiveProtectVault"), "/Users/someone/samples/apv-sample-1/")
        assert label == "apv-sample-1"

    def test_scanning_a_directory_of_multiple_repos_still_hides_the_marker(self) -> None:
        """Scanning the parent of several sample repositories nests a real
        subdirectory name in front of the marker in ``repo_root``; the
        marker is stripped by segment, not just leading-prefix, so the
        real neighboring segment survives."""
        label = _repo_path_component(_layout("apv-sample-1/@ActiveProtectVault"), "/Users/someone/samples")
        assert label == "samples/apv-sample-1"
        assert "@ActiveProtectVault" not in label


class TestRepoLabel:
    def test_no_key_provided_shows_key_needed(self) -> None:
        """NO_KEY_PROVIDED is only reachable once discover() has resolved
        it, so the hint is honest here."""
        label = _repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.NO_KEY_PROVIDED, "/samples/apv-sample-2-encrypted", verbose=False
        )
        assert label == "apv-sample-2-encrypted · key needed"

    def test_not_encrypted_shows_that_fact_once_known(self) -> None:
        label = _repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.NOT_ENCRYPTED, "/samples/apv-sample-1", verbose=False
        )
        assert label == "apv-sample-1 · not encrypted"

    def test_verified_key_shows_that_fact(self) -> None:
        label = _repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.VERIFIED, "/samples/apv-sample-2-encrypted", verbose=False
        )
        assert label == "apv-sample-2-encrypted · key verified"

    def test_invalid_key_shows_that_fact(self) -> None:
        label = _repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.INVALID, "/samples/apv-sample-2-encrypted", verbose=False
        )
        assert label == "apv-sample-2-encrypted · invalid key"

    def test_verbose_mode_appends_layout_regardless_of_key_status(self) -> None:
        # A repository's own uuid lives on Catalog.info, not here -- only
        # the layout-kind suffix is added.
        label = _repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.NO_KEY_PROVIDED, "/samples/apv-sample-1", verbose=True
        )
        assert "layout: vault" in label
        assert "apv-sample-1" in label

    def test_a_scanned_directory_name_shaped_like_rich_markup_is_escaped(self) -> None:
        """The scanned directory's own real name reaches this label via
        ``_repo_path_component`` -- both ``Tree`` and ``Static`` re-parse
        a plain ``str`` as Rich markup, so a directory literally named
        ``"a[x]b"`` must not reach either unescaped: an unmatched closing
        tag or an unresolvable tag body crashes the widget outright with a
        markup error. No embedded
        ``/`` here -- that would itself be a real path separator
        (``Path.name`` only keeps the last segment), not a markup test."""
        label = _repo_label(_layout("@ActiveProtectVault"), KeyStatus.NOT_ENCRYPTED, "/samples/a[x]b", verbose=False)
        assert label == r"a\[x]b · not encrypted"


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

    def test_a_catalog_display_name_shaped_like_rich_markup_is_escaped(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid", connection_id="fake-conn-id"))
        assert _catalog_label(catalog, "a[/]b", verbose=False) == r"a\[/]b"


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
        """Cross-checks ``_TYPE_LABELS`` against ``SUPPORTED_SAAS_SUB_TYPES``
        (``units/dispatch.py``'s real source of truth), not the
        parametrize list above (a hand-maintained mirror) — an
        unrecognized token would otherwise silently fall back to the raw
        wire string instead of failing this test loudly."""
        assert _TYPE_LABELS.keys() >= SUPPORTED_SAAS_SUB_TYPES


__all__: list[str] = []
