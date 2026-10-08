"""Unit tests for ``synology_apm_repo.sdk.units.saas.contact`` against a
synthetic repository root. The integration test covers only dispatch and
the top-level bucket against real data, so listing, grouping, and CSV/JSON
content are proven only here."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import tempfile
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import pytest
import zstandard

from support.model_factories import make_version
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotRestorableError, UnsupportedDataFormatError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.table import Table
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.saas_contact import build_contact_csv
from synology_apm_repo.sdk.units.saas.contact import open_contact_provider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    gws_contact_db,
    write_indexed_saas_obj,
    write_saas_obj,
    write_saas_stream_dbs,
)

_STREAM_UUID = "contact-stream-uuid"
_IDS = SaasStreamIds(stream_id=14, stream_uuid=_STREAM_UUID)


def _build_m365_contact_db(
    contacts: list[tuple[str, str, str, str, str]], *, emails: dict[str, str] | None = None
) -> bytes:
    """``contacts``: (contact_id, first_name, last_name, parent_folder_id, meta_object_id).
    ``emails`` (contact_id -> primary_email), when given, populates the
    real ``primary_email`` column; omitted, that column stays empty."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE contact_table(contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, "
            "parent_folder_id TEXT, meta_object_id TEXT, primary_email TEXT)"
        )
        conn.executemany(
            "INSERT INTO contact_table VALUES (?, ?, ?, ?, ?, ?)",
            [(*row, (emails or {}).get(row[0], "")) for row in contacts],
        )
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_group_db(groups: list[tuple[str, str]]) -> bytes:
    """``group_table``: (group_id, group_name) -- GWS group definitions."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "group.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE group_table(group_id TEXT, group_name TEXT)")
        conn.executemany("INSERT INTO group_table VALUES (?, ?)", groups)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_contact_folder_db(folders: list[tuple[str, str]]) -> bytes:
    """``contact_folder_table``: (folder_id, folder_name) -- M365 folder
    definitions."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact_folder.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE contact_folder_table(folder_id TEXT, folder_name TEXT)")
        conn.executemany("INSERT INTO contact_folder_table VALUES (?, ?)", folders)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


_M365_META = json.dumps(
    {
        "version": "1.0",
        "client_metadata": {
            "givenName": "Ada",
            "middleName": "",
            "surname": "Lovelace",
            "emailAddresses": [{"address": "ada@example.com"}],
            "businessPhones": ["555-0100"],
            "jobTitle": "Mathematician",
            "companyName": "Analytical Engines Ltd",
            "businessAddress": {"street": "1 Engine St", "city": "London", "countryOrRegion": "UK"},
        },
        "contact_type": "Contact",
    }
).encode()

_GWS_META = json.dumps(
    {
        "version": "2.0",
        "client_metadata": {"names": [{"givenName": "Grace", "familyName": "Hopper"}]},
        "photo_size": 100,
        "photo_hash": "abc",
        "photo_object_id": "v1_object_photo",
    }
).encode()


def _build_contact_repo(
    tmp_path: Path,
    *,
    session_id: int = 9,
    is_m365: bool = True,
    include_folder_names: bool = False,
    include_groups: bool = False,
    emails: dict[str, str] | None = None,
) -> None:
    write_saas_stream_dbs(tmp_path, _IDS, target_type="M365" if is_m365 else "GW")

    if is_m365:
        contact_db_bytes = _build_m365_contact_db(
            [("contact-1", "Ada", "Lovelace", "folder-1", "meta_1")], emails=emails
        )
    else:
        group_memberships = (("contact-1", "group-1"),) if include_groups else ()
        contact_db_bytes = gws_contact_db(
            [("contact-1", "Grace", "Hopper", "meta_1")], group_memberships=group_memberships
        )

    meta_bytes = _M365_META if is_m365 else _GWS_META
    payloads = [("contact_svc", contact_db_bytes), ("meta_1", meta_bytes)]
    if is_m365 and include_folder_names:
        payloads.append(("folder_svc", _build_contact_folder_db([("folder-1", "My Contacts")])))
    if not is_m365 and include_groups:
        payloads.append(("group_svc", _build_group_db([("group-1", "Friends")])))
    db_objects = [("contact_db", "contact_svc")]
    if is_m365 and include_folder_names:
        db_objects.append(("contact_folder_db", "folder_svc"))
    if not is_m365 and include_groups:
        db_objects.append(("contact_group_db", "group_svc"))
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid="vuid-contact",
        payloads=payloads,
        db_objects=db_objects,
    )


def _install_call_counting_select(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Sequence[object]]]:
    """Records every ``Table.select`` ``(where, params)`` call (still
    delegating) into the returned list; callers clear it after setup, before
    the call under test."""
    calls: list[tuple[str, Sequence[object]]] = []
    original_select = Table.select

    def counting_select(
        self: Table,
        where: str = "",
        params: Sequence[object] = (),
        *,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> AsyncIterator[dict[str, object | None]]:
        calls.append((where, params))
        return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

    monkeypatch.setattr(Table, "select", counting_select)
    return calls


def _version(target_type: str) -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-contact",
        target_type=target_type,
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


class TestM365Tree:
    @pytest.fixture
    async def provider(self, tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
        _build_contact_repo(tmp_path, is_m365=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await open_contact_provider(repo, _version("M365"), saas_streams)
            try:
                yield p
            finally:
                await p.close()

    async def test_root_lists_the_folder(self, provider: SaasWorkloadProvider[Any]) -> None:
        folders = await provider.children(provider.root())
        assert len(folders) == 1
        assert folders[0].name == "folder-1"

    async def test_folder_lists_the_contact(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        contacts = await provider.children(folder)
        assert len(contacts) == 1
        assert contacts[0].name == "Ada Lovelace"
        assert contacts[0].kind is UnitKind.CONTACT

    async def test_contact_with_a_real_email_exposes_it_as_a_column(self, tmp_path: Path) -> None:
        _build_contact_repo(tmp_path, is_m365=True, emails={"contact-1": "ada@example.com"})
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_contact_provider(repo, _version("M365"), saas_streams) as provider,
        ):
            [folder] = await provider.children(provider.root())
            [contact] = await provider.children(folder)
            assert contact.columns.email == "ada@example.com"

    async def test_unit_builds_a_csv_with_bom(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        [contact] = await provider.children(folder)
        content = (await provider.unit(contact)).content
        data = await content.read()
        assert data.startswith(b"\xef\xbb\xbf")
        text = data.decode("utf-8-sig")
        assert "Ada" in text
        assert "ada@example.com" in text
        assert "Analytical Engines Ltd" in text

    async def test_unit_name_has_csv_extension(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        [contact] = await provider.children(folder)
        unit = await provider.unit(contact)
        assert unit.name == "Ada Lovelace.csv"

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_folder_lists_the_contact_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _install_call_counting_select(monkeypatch)

        [folder] = await provider.children(provider.root())
        calls.clear()
        await provider.children(folder)
        assert calls == [("parent_folder_id = ?", ("folder-1",))]

    async def test_root_resolves_the_real_folder_name_when_contact_folder_db_is_present(self, tmp_path: Path) -> None:
        # The shared ``provider`` fixture has no contact_folder_db entry.
        _build_contact_repo(tmp_path, is_m365=True, include_folder_names=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_contact_provider(repo, _version("M365"), saas_streams)
            try:
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                assert folders[0].name == "My Contacts"
            finally:
                await provider.close()


class TestGwsTree:
    @pytest.fixture
    async def provider(self, tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
        _build_contact_repo(tmp_path, is_m365=False)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await open_contact_provider(repo, _version("GW"), saas_streams)
            try:
                yield p
            finally:
                await p.close()

    async def test_root_lists_a_single_synthetic_group(self, provider: SaasWorkloadProvider[Any]) -> None:
        groups = await provider.children(provider.root())
        assert len(groups) == 1
        assert groups[0].name == "Contacts"

    async def test_group_lists_the_contact(self, provider: SaasWorkloadProvider[Any]) -> None:
        [group] = await provider.children(provider.root())
        contacts = await provider.children(group)
        assert len(contacts) == 1
        assert contacts[0].name == "Grace Hopper"

    async def test_group_lists_the_contact_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GWS has no group column (``group_column=None``; its groups are
        M:N), so the single synthetic group's members are one query with no
        ``WHERE``."""
        calls = _install_call_counting_select(monkeypatch)

        [group] = await provider.children(provider.root())
        calls.clear()
        await provider.children(group)
        assert calls == [("", ())]

    async def test_contact_gets_its_real_gws_group_names_as_a_detail(self, tmp_path: Path) -> None:
        # The shared ``provider`` fixture has no group membership rows.
        _build_contact_repo(tmp_path, is_m365=False, include_groups=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_contact_provider(repo, _version("GW"), saas_streams)
            try:
                [group] = await provider.children(provider.root())
                [contact] = await provider.children(group)
                assert contact.details.get("groups") == ["Friends"]
            finally:
                await provider.close()

    async def test_unit_returns_raw_meta_json_not_csv(self, provider: SaasWorkloadProvider[Any]) -> None:
        [group] = await provider.children(provider.root())
        [contact] = await provider.children(group)
        content = (await provider.unit(contact)).content
        data = await content.read()
        parsed = json.loads(data)
        assert parsed["version"] == "2.0"
        assert parsed["photo_object_id"] == "v1_object_photo"

    async def test_unit_name_has_json_extension(self, provider: SaasWorkloadProvider[Any]) -> None:
        [group] = await provider.children(provider.root())
        [contact] = await provider.children(group)
        unit = await provider.unit(contact)
        assert unit.name == "Grace Hopper.json"


def _parsed_csv_row(csv_bytes: bytes) -> dict[str, str]:
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    row = next(reader)
    return dict(zip(header, row, strict=True))


class TestBuildContactCsv:
    def test_meta_bytes_not_valid_json_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError, match="contact META did not parse as JSON"):
            build_contact_csv(b"not json at all")

    def test_missing_optional_fields_become_empty_columns(self) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "Bare"}}).encode()
        csv_bytes = build_contact_csv(meta)
        parsed = _parsed_csv_row(csv_bytes)
        assert parsed["First Name"] == "Bare"
        for column, value in parsed.items():
            if column != "First Name":
                assert value == "", f"{column!r} expected to be empty, got {value!r}"

    @pytest.mark.parametrize(
        "client_metadata",
        [{}, {"jobTitle": None}, {"jobTitle": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_job_title_or_default_catches_null_empty_and_missing(self, client_metadata: dict[str, object]) -> None:
        meta = json.dumps({"client_metadata": {**client_metadata, "givenName": "X"}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Job Title"] == ""

    @pytest.mark.parametrize(
        "business_address",
        [{}, {"street": None}, {"street": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_business_street_or_default_catches_null_empty_and_missing(
        self, business_address: dict[str, object]
    ) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "X", "businessAddress": business_address}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Business Street"] == ""

    @pytest.mark.parametrize(
        "client_metadata",
        [{"givenName": "X"}, {"givenName": "X", "businessAddress": None}, {"givenName": "X", "businessAddress": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_business_address_itself_or_default_catches_null_empty_and_missing(
        self, client_metadata: dict[str, object]
    ) -> None:
        """``businessAddress`` itself, not just its fields, falls back to
        ``{}`` (the ``.get(key) or default`` pattern)."""
        meta = json.dumps({"client_metadata": client_metadata}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Business Street"] == ""

    def test_malformed_non_dict_first_email_is_treated_as_absent(self) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "X", "emailAddresses": ["not-a-dict"]}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["E-mail Address"] == ""


class TestDisplayName:
    def test_is_blank_when_both_names_are_blank(self) -> None:
        # Never the raw contact_id: for GWS that's an opaque resource name.
        from synology_apm_repo.sdk.units.saas.contact import _display_name

        row: dict[str, object | None] = {"first_name": "", "last_name": None, "contact_id": "contact-123"}
        assert _display_name(row) == ""


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_contact_table_exists(self, tmp_path: Path) -> None:
        write_saas_stream_dbs(tmp_path, _IDS, target_type="M365")

        write_saas_obj(tmp_path, _IDS, session_id=9, content=b"\x00" * 4096)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index for table"):
                await open_contact_provider(repo, _version("M365"), saas_streams)
