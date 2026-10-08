"""Unit tests for the label-formatting helpers: ``RepositoryLayout.display_root``,
``browser.core.browse.repo_labels`` and ``workload_grouping.humanize_type``."""

from __future__ import annotations

from typing import cast

import pytest

from synology_apm_repo.browser.core.browse.repo_labels import (
    catalog_label,
    repo_label,
    repo_path_component,
)
from synology_apm_repo.browser.core.browse.workload_grouping import _TYPE_LABELS, humanize_type
from synology_apm_repo.sdk.api import Catalog, KeyStatus
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_SAAS_SUB_TYPES


def _layout(repo_root: str) -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.VAULT, repo_root=repo_root)


class TestDisplayRoot:
    @pytest.mark.parametrize(
        ("repo_root", "expected"),
        [
            pytest.param("@ActiveProtectVault", "", id="bare_vault_marker_strips_to_empty"),
            pytest.param("@ActiveProtectData/gqDuTMuityBf", "gqDuTMuityBf", id="object_store_marker_keeps_the_repo_id"),
            pytest.param("some/other/path", "some/other/path", id="unrelated_path_passes_through_unchanged"),
            # Stripped at any depth; a directory in front of it survives.
            pytest.param(
                "alice-backup/@ActiveProtectVault", "alice-backup", id="marker_nested_one_level_down_still_strips"
            ),
            pytest.param(
                "alice-bucket/@ActiveProtectData/BikXpRbFNGI1",
                "alice-bucket/BikXpRbFNGI1",
                id="object_store_marker_nested_one_level_down_keeps_both_real_segments",
            ),
        ],
    )
    def test_display_root(self, repo_root: str, expected: str) -> None:
        assert _layout(repo_root).display_root == expected


class TestRepoPathComponent:
    @pytest.mark.parametrize(
        ("repo_root", "scan_path", "expected", "marker"),
        [
            pytest.param(
                "@ActiveProtectVault",
                "/Users/someone/samples/alice-backup",
                "alice-backup",
                "@ActiveProtectVault",
                id="single_vault_shows_just_the_scanned_directory_name",
            ),
            pytest.param(
                "@ActiveProtectData/gqDuTMuityBf",
                "/Users/someone/samples/bob-bucket",
                "bob-bucket/gqDuTMuityBf",
                "@ActiveProtectData",
                id="object_store_appends_the_repo_id_after_the_directory_name",
            ),
            # A subdirectory name in front of the marker survives.
            pytest.param(
                "alice-backup/@ActiveProtectVault",
                "/Users/someone/samples",
                "samples/alice-backup",
                "@ActiveProtectVault",
                id="scanning_a_directory_of_multiple_repos_still_hides_the_marker",
            ),
        ],
    )
    def test_marker_is_hidden(self, repo_root: str, scan_path: str, expected: str, marker: str) -> None:
        label = repo_path_component(_layout(repo_root), scan_path)
        assert label == expected
        assert marker not in label

    def test_trailing_slash_in_scan_path_does_not_leave_an_empty_name(self) -> None:
        label = repo_path_component(_layout("@ActiveProtectVault"), "/Users/someone/samples/alice-backup/")
        assert label == "alice-backup"


class TestRepoLabel:
    @pytest.mark.parametrize(
        ("key_status", "scan_path", "expected"),
        [
            pytest.param(
                KeyStatus.NO_KEY_PROVIDED,
                "/samples/alice-encrypted",
                "alice-encrypted · key needed",
                id="no_key_provided_shows_key_needed",
            ),
            pytest.param(
                KeyStatus.NOT_ENCRYPTED,
                "/samples/alice-backup",
                "alice-backup · not encrypted",
                id="not_encrypted_shows_that_fact_once_known",
            ),
            pytest.param(
                KeyStatus.VERIFIED,
                "/samples/alice-encrypted",
                "alice-encrypted · key verified",
                id="verified_key_shows_that_fact",
            ),
            pytest.param(
                KeyStatus.INVALID,
                "/samples/alice-encrypted",
                "alice-encrypted · invalid key",
                id="invalid_key_shows_that_fact",
            ),
            pytest.param(
                KeyStatus.NOT_ENCRYPTED,
                "/samples/a[x]b",
                r"a\[x]b · not encrypted",
                id="a_scanned_directory_name_shaped_like_rich_markup_is_escaped",
            ),
        ],
    )
    def test_normal_mode_label(self, key_status: KeyStatus, scan_path: str, expected: str) -> None:
        assert repo_label(_layout("@ActiveProtectVault"), key_status, scan_path, verbose=False) == expected

    def test_verbose_mode_appends_layout_regardless_of_key_status(self) -> None:
        label = repo_label(
            _layout("@ActiveProtectVault"), KeyStatus.NO_KEY_PROVIDED, "/samples/alice-backup", verbose=True
        )
        assert "layout: vault" in label
        assert "alice-backup" in label


class _FakeCatalogInfo:
    def __init__(self, *, uuid: str) -> None:
        self.uuid = uuid


class _FakeConnectionForLabel:
    def __init__(self, *, connection_id: str) -> None:
        self.connection_id = connection_id


class _FakeCatalogForLabel:
    """Duck-typed ``api.Catalog``; ``catalog_label`` reads only
    ``.info.uuid`` and ``.connection.connection_id``."""

    def __init__(self, *, uuid: str, connection_id: str) -> None:
        self.info = _FakeCatalogInfo(uuid=uuid)
        self.connection = _FakeConnectionForLabel(connection_id=connection_id)


class TestCatalogLabel:
    def test_normal_mode_returns_the_name_unchanged(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid", connection_id="fake-conn-id"))
        assert catalog_label(catalog, "Source 1", verbose=False) == "Source 1"

    def test_verbose_mode_appends_uuid_and_connection_id(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid-0000", connection_id="fake-conn-id"))
        label = catalog_label(catalog, "Source 1", verbose=True)
        assert label.startswith("Source 1 (")
        assert "uuid: fake-uuid-0000" in label
        assert "id: fake-conn-id" in label

    def test_a_catalog_display_name_shaped_like_rich_markup_is_escaped(self) -> None:
        catalog = cast(Catalog, _FakeCatalogForLabel(uuid="fake-uuid", connection_id="fake-conn-id"))
        assert catalog_label(catalog, "a[/]b", verbose=False) == r"a\[/]b"


class TestHumanizeType:
    @pytest.mark.parametrize(
        ("type_hint", "expected"),
        [
            # Device types keep their acronyms.
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
        # Hand-maintained; the enforced check against the SDK's sub-type
        # set is test_type_labels_covers_every_real_saas_sub_type.
        assert humanize_type(type_hint) == expected

    def test_an_unrecognized_future_token_falls_back_to_the_raw_string(self) -> None:
        assert humanize_type("SOME_FUTURE_TYPE") == "SOME_FUTURE_TYPE"

    def test_type_labels_covers_every_real_saas_sub_type(self) -> None:
        assert _TYPE_LABELS.keys() >= SUPPORTED_SAAS_SUB_TYPES
