"""Unit tests for ``synology_apm_repo.sdk.units.saas.mail`` over a
synthetic repository root (``unit.sdk.saas_fakes``), plus
``units.content.saas_mail``'s ``build_eml`` (``X-ABL-ID`` fragment
reassembly), which ``tests/integration/sdk/test_units_saas_mail.py`` leaves
to this file."""

from __future__ import annotations

import asyncio
import dataclasses
import email
import email.policy
import json
import sqlite3
import tempfile
from collections.abc import AsyncIterator, Sequence
from email.message import EmailMessage, Message
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
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.saas_mail import build_eml
from synology_apm_repo.sdk.units.saas.mail import (
    MAIL_CONFIG,
    MailState,
    _mail_display_name,
    open_archive_mail_provider,
    open_mail_provider,
)
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.tree_strategy import TreeStrategy
from unit.sdk.saas_fakes import SaasStreamIds, mail_db, write_indexed_saas_obj, write_saas_obj, write_saas_stream_dbs

_STREAM_UUID = "mail-stream-uuid"
_IDS = SaasStreamIds(stream_id=16, stream_uuid=_STREAM_UUID)


# -- build_eml() pure-function tests -------------------------------------


def _mark_part(msg: Message, content_type: str, abl_id: str) -> None:
    for part in msg.walk():
        if part.get_content_type() == content_type:
            part["X-ABL-ID"] = abl_id
            return
    raise AssertionError(f"no part with content type {content_type!r}")


class TestBuildEmlPureFunction:
    @pytest.mark.parametrize(
        ("subject", "maintype", "subtype", "filename", "encoding", "real_bytes"),
        [
            pytest.param(
                "Test", "application", "pdf", "doc.pdf", None, b"%PDF-1.4 fake pdf content 1234567890", id="base64"
            ),
            pytest.param(
                "QP",
                "text",
                "plain",
                "note.txt",
                "quoted-printable",
                b"line1\nline2 with =signs and \xe2\x82\xac euro",
                id="quoted_printable",
            ),
            pytest.param(
                "Plain",
                "text",
                "plain",
                "ascii.txt",
                "7bit",
                b"plain ascii content, no special encoding needed",
                id="7bit",
            ),
        ],
    )
    def test_attachment_round_trips_byte_for_byte(
        self, subject: str, maintype: str, subtype: str, filename: str, encoding: str | None, real_bytes: bytes
    ) -> None:
        # encoding=None keeps add_attachment's default (base64 for a binary
        # maintype).
        msg = EmailMessage()
        msg["Subject"] = subject
        msg.set_content("body")
        msg.add_attachment(b"", maintype=maintype, subtype=subtype, filename=filename)
        for part in msg.walk():
            if part.get_filename() == filename:
                if encoding is not None:
                    part.replace_header("Content-Transfer-Encoding", encoding)
                part["X-ABL-ID"] = "ID-file"
        skel = msg.as_bytes()

        result = build_eml(skel, {"ID-file": real_bytes})

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        [found] = [p for p in reparsed.walk() if p.get_filename() == filename]
        assert found.get_payload(decode=True) == real_bytes
        assert found.get("X-ABL-ID") is None

    def test_parts_without_x_abl_id_are_left_untouched(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "Kept"
        msg.set_content("this stays exactly as-is")
        skel = msg.as_bytes()

        result = build_eml(skel, {})  # no fragments at all
        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        body = reparsed.get_payload(decode=True)
        assert isinstance(body, bytes)
        assert body.strip() == b"this stays exactly as-is"

    def test_matching_is_by_header_value_not_array_order(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "Order"
        msg.set_content("body")
        msg.add_attachment(b"", maintype="application", subtype="a", filename="a.bin")
        msg.add_attachment(b"", maintype="application", subtype="b", filename="b.bin")
        for part in msg.walk():
            if part.get_filename() == "a.bin":
                part["X-ABL-ID"] = "ID-file-2"  # deliberately "swapped" ids
            elif part.get_filename() == "b.bin":
                part["X-ABL-ID"] = "ID-file"

        skel = msg.as_bytes()
        # dict insertion order is the OPPOSITE of which part references which id
        fragments = {"ID-file": b"content-for-b", "ID-file-2": b"content-for-a"}
        result = build_eml(skel, fragments)

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        for found in reparsed.walk():
            if found.get_filename() == "a.bin":
                assert found.get_payload(decode=True) == b"content-for-a"
            elif found.get_filename() == "b.bin":
                assert found.get_payload(decode=True) == b"content-for-b"

    def test_nested_message_rfc822_fragment_is_not_recursively_expanded(self) -> None:
        inner = EmailMessage()
        inner["Subject"] = "Inner"
        inner.set_content("inner body")
        inner_bytes = inner.as_bytes()

        outer = EmailMessage()
        outer["Subject"] = "Outer"
        outer.set_content("outer body")
        outer.add_attachment(b"", maintype="message", subtype="rfc822")
        for part in outer.walk():
            if part.get_content_type() == "message/rfc822":
                part.replace_header("Content-Transfer-Encoding", "7bit")
                part["X-ABL-ID"] = "ID-file"
        skel = outer.as_bytes()

        result = build_eml(skel, {"ID-file": inner_bytes})
        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        [nested] = [p for p in reparsed.walk() if p.get_content_type() == "message/rfc822"]
        # The parser re-nests a message/rfc822 body into a sub-Message
        # (get_payload(decode=True) is None for it), so compare that
        # sub-Message's serialization instead.
        nested_payload = nested.get_payload()
        assert isinstance(nested_payload, list)
        [inner_message] = nested_payload
        assert isinstance(inner_message, email.message.Message)
        assert inner_message.as_bytes() == inner_bytes

    def test_unmatched_fragment_ids_in_the_dict_are_simply_unused(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "NoRefs"
        msg.set_content("body")
        skel = msg.as_bytes()
        result = build_eml(skel, {"ID-file-orphan": b"never used"})

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        assert reparsed["Subject"] == "NoRefs"
        body = reparsed.get_payload(decode=True)
        assert isinstance(body, bytes)
        assert body.strip() == b"body"
        assert b"never used" not in result


class TestDeclaredSize:
    def test_non_int_size_is_narrowed_to_none(self) -> None:
        from synology_apm_repo.sdk.units.saas.mail import _declared_size

        assert _declared_size({"size": "12"}) is None
        assert _declared_size({}) is None
        assert _declared_size({"size": 12}) == 12


# -- open_mail_provider over a synthetic repository ---------------------------


def _build_mail_db_with_timestamps(mails: list[tuple[str, str, str, str, int]]) -> bytes:
    """Like ``saas_fakes.mail_db``, plus a ``remote_timestamp`` column --
    ``mails``: (mail_id, subject, parent_folder_id, meta_object_id,
    remote_timestamp)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE mail_table("
            "mail_id TEXT, subject TEXT, parent_folder_id TEXT, meta_object_id TEXT, remote_timestamp INTEGER)"
        )
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?, ?, ?)", mails)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_folder_db(folders: list[tuple[str, str]]) -> bytes:
    """``mail_folder_table``: (folder_id, folder_name) -- see
    ``mail.py``'s own ``_m365_folder_names``/``_folder_names``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_folder.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE mail_folder_table(folder_id TEXT, folder_name TEXT)")
        conn.executemany("INSERT INTO mail_folder_table VALUES (?, ?)", folders)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_folder_hierarchy_db(folders: list[tuple[str, str, str, int]]) -> bytes:
    """The full M365 ``mail_folder_table`` schema, unlike
    ``_build_mail_folder_db``'s 2-column one -- ``folders``: (folder_id,
    folder_name, parent_folder_id, is_root)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_folder.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE mail_folder_table(folder_id TEXT, folder_name TEXT, parent_folder_id TEXT, is_root INTEGER)"
        )
        conn.executemany("INSERT INTO mail_folder_table VALUES (?, ?, ?, ?)", folders)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_gws_mail_db(mails: list[tuple[str, str, str]], memberships: list[tuple[int, str, str]]) -> bytes:
    """GWS's ``mail_table`` (no ``parent_folder_id``: labels, not folders)
    plus the label *membership* half of ``mail_label_table``, which lives in
    this same ``mail_db`` object. ``mails``: (mail_id, subject,
    meta_object_id); ``memberships``: (row_id, mail_id, label_id)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("CREATE TABLE mail_table(mail_id TEXT, subject TEXT, meta_object_id TEXT)")
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?)", mails)
        conn.execute("CREATE TABLE mail_label_table(row_id INTEGER, mail_id TEXT, label_id TEXT)")
        conn.executemany("INSERT INTO mail_label_table VALUES (?, ?, ?)", memberships)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_gws_mail_label_db(labels: list[tuple[int, str, str, int]]) -> bytes:
    """GWS's label *definitions* half of ``mail_label_table``, living in
    its own, separate object — ``labels``: (row_id, label_id, label_name,
    label_type)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_label.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE mail_label_table(row_id INTEGER, label_id TEXT, label_name TEXT, label_type INTEGER)"
        )
        conn.executemany("INSERT INTO mail_label_table VALUES (?, ?, ?, ?)", labels)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _make_skel_and_attachment() -> tuple[bytes, bytes]:
    msg = EmailMessage()
    msg["Subject"] = "Hello"
    msg["From"] = "sender@example.com"
    msg.set_content("Mail body text")
    msg.add_attachment(b"", maintype="application", subtype="octet-stream", filename="file.bin")
    _mark_part(msg, "application/octet-stream", "ID-file")
    real_attachment = b"the real attachment bytes"
    return msg.as_bytes(), real_attachment


def _build_mail_repo(
    tmp_path: Path,
    *,
    session_id: int = 11,
    include_folder_names: bool = False,
    index_db_name: str = "mail_db",
    subject: str = "Hello",
) -> None:
    write_saas_stream_dbs(tmp_path, _IDS)

    mail_db_bytes = mail_db([("mail-1", subject, "folder-1", "meta_1")])
    skel_bytes, attachment_bytes = _make_skel_and_attachment()
    meta_1 = json.dumps(
        {
            "version": "3.0",
            "content_list": [
                {
                    "fragment_id": "ID-skel",
                    "type": 0,
                    "file_name": "",
                    "content_id": "",
                    "object_id": "skel_obj",
                    "size": len(skel_bytes),
                },
                {
                    "fragment_id": "ID-file",
                    "type": 4,
                    "file_name": "file.bin",
                    "content_id": "",
                    "object_id": "att_obj",
                    "size": len(attachment_bytes),
                },
            ],
        }
    ).encode()

    payloads = [
        ("mail_svc", mail_db_bytes),
        ("meta_1", meta_1),
        ("skel_obj", skel_bytes),
        ("att_obj", attachment_bytes),
    ]
    if include_folder_names:
        payloads.append(("folder_svc", _build_mail_folder_db([("folder-1", "Inbox")])))
    db_objects = [(index_db_name, "mail_svc")]
    if include_folder_names:
        db_objects.append(("mail_folder_db", "folder_svc"))
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=db_objects,
    )


def _build_mail_repo_with_timestamps(
    tmp_path: Path, mails: list[tuple[str, str, str, str, int]], *, session_id: int = 21
) -> None:
    """A listing-only fixture (no skeleton/attachment payloads): several
    mails in one folder with distinct ``remote_timestamp`` values."""
    write_saas_stream_dbs(tmp_path, _IDS)

    mail_db_bytes = _build_mail_db_with_timestamps(mails)
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=[("mail_svc", mail_db_bytes)],
        db_objects=[("mail_db", "mail_svc")],
    )


def _build_mail_repo_with_folder_hierarchy(tmp_path: Path, *, session_id: int = 25) -> None:
    """A listing-only fixture (no skeleton/attachment payloads) for M365
    Mail's nested folder hierarchy: "root" (the ``is_root`` anchor row) ->
    "inbox" (zero direct mail) -> "haha" (nested, has mail), and "sent"
    (top-level, has direct mail) alongside "inbox"."""
    write_saas_stream_dbs(tmp_path, _IDS)

    folders = [
        ("root", "RootLabel", "anchor", 1),
        ("inbox", "Inbox", "root", 0),
        ("sent", "Sent", "root", 0),
        ("haha", "haha", "inbox", 0),
    ]
    mails = [
        ("mail-1", "Under Sent", "sent", "meta_1"),
        ("mail-2", "Under Haha", "haha", "meta_2"),
    ]
    mail_db_bytes = mail_db(mails)
    folder_db_bytes = _build_mail_folder_hierarchy_db(folders)
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=[("mail_svc", mail_db_bytes), ("folder_svc", folder_db_bytes)],
        db_objects=[("mail_db", "mail_svc"), ("mail_folder_db", "folder_svc")],
    )


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-mail",
        target_type="M365",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
    _build_mail_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await open_mail_provider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


def _gws_version() -> Version:
    return make_version(
        version_id=62,
        version_uid="vuid-mail-gws",
        target_type="GW",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


def _build_gws_mail_repo(tmp_path: Path, *, session_id: int = 15) -> None:
    """A GWS mailbox with two messages: ``mail-1`` carries one real
    label, ``mail-2`` carries none at all — the "no membership row for
    this mail_id" case ``membership_detail`` degrades on: it returns ``{}``,
    so the node has no ``"labels"`` key at all, not an empty list."""
    write_saas_stream_dbs(tmp_path, _IDS, target_type="GW")

    mails = [("mail-1", "Hello", "meta_1"), ("mail-2", "No Labels", "meta_2")]
    memberships = [(1, "mail-1", "label-1")]
    mail_db_bytes = _build_gws_mail_db(mails, memberships)
    mail_label_db_bytes = _build_gws_mail_label_db([(1, "label-1", "Important", 0)])

    payloads = [("mail_svc", mail_db_bytes), ("mail_label_svc", mail_label_db_bytes)]
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_gws_version().version_uid,
        payloads=payloads,
        db_objects=[("mail_db", "mail_svc"), ("mail_label_db", "mail_label_svc")],
    )


@pytest.fixture
async def gws_provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
    _build_gws_mail_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await open_mail_provider(repo, _gws_version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


async def test_gws_labels_reuse_the_open_mail_db_instead_of_materializing_it_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """mail_db is opened once for mail_table; the label membership join reads
    that same open DB. A second open would decompress and copy the whole
    mailbox DB again."""
    from synology_apm_repo.sdk.units.saas import object_name_index

    _build_gws_mail_repo(tmp_path)
    one_shot_reads: list[tuple[str, ...]] = []
    real_read_indexed_table = object_name_index.read_indexed_table

    async def recording_read_indexed_table(
        dedup_file: Any, index: Any, object_names: tuple[str, ...], *a: Any, **k: Any
    ) -> Any:
        one_shot_reads.append(object_names)
        return await real_read_indexed_table(dedup_file, index, object_names, *a, **k)

    monkeypatch.setattr(object_name_index, "read_indexed_table", recording_read_indexed_table)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await open_mail_provider(repo, _gws_version(), saas_streams)
        try:
            assert provider.state.gws_labels  # labels were still resolved
        finally:
            await provider.close()
    assert ("mail_db",) not in one_shot_reads


class TestGwsTree:
    async def test_root_lists_a_single_synthetic_mail_group(self, gws_provider: SaasWorkloadProvider[Any]) -> None:
        # GWS has no folders: every message sits in the one root_name group.
        groups = await gws_provider.children(gws_provider.root())
        assert len(groups) == 1
        assert groups[0].name == "Mail"
        assert groups[0].is_leaf is False

    async def test_group_lists_both_mails_with_real_label_names_and_the_empty_boundary(
        self, gws_provider: SaasWorkloadProvider[Any]
    ) -> None:
        [group] = await gws_provider.children(gws_provider.root())
        mails = await gws_provider.children(group)
        by_name = {mail.name: mail for mail in mails}
        assert set(by_name) == {"Hello", "No Labels"}
        # mail-1's one real label membership resolves through the object-name
        # index to its real definitions-table name, not the raw label_id.
        assert by_name["Hello"].details.get("labels") == ["Important"]
        assert "labels" not in by_name["No Labels"].details


class TestMailDisplayName:
    @pytest.mark.parametrize(
        ("subject", "expected"),
        [
            pytest.param("", "(no subject)", id="empty_string_subject_gets_the_no_subject_label"),
            pytest.param(None, "(no subject)", id="null_subject_gets_the_no_subject_label"),
            pytest.param("Hello", "Hello", id="real_subject_is_used_as_is"),
        ],
    )
    def test_mail_display_name(self, subject: str | None, expected: str) -> None:
        assert _mail_display_name({"subject": subject}) == expected


class TestTree:
    async def test_root_lists_the_folder(self, provider: SaasWorkloadProvider[Any]) -> None:
        folders = await provider.children(provider.root())
        assert len(folders) == 1
        assert folders[0].name == "folder-1"

    async def test_root_resolves_the_real_folder_name_when_mail_folder_db_is_present(self, tmp_path: Path) -> None:
        # The default fixture has no mail_folder_db entry, so groups fall
        # back to the raw folder id; this one resolves the real name.
        _build_mail_repo(tmp_path, include_folder_names=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                assert folders[0].name == "Inbox"
            finally:
                await provider.close()

    async def test_folder_lists_the_mail(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        mails = await provider.children(folder)
        assert len(mails) == 1
        assert mails[0].name == "Hello"
        assert mails[0].kind is UnitKind.MAIL

    async def test_folder_lists_an_empty_subject_mail_as_no_subject_not_the_raw_id(self, tmp_path: Path) -> None:
        # An empty subject must show "(no subject)", never the raw mail_id,
        # since the exported .eml filename also falls back to it.
        _build_mail_repo(tmp_path, subject="")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                [mail] = await provider.children(folder)
                assert mail.name == "(no subject)"
                assert "mail-1" not in mail.name
            finally:
                await provider.close()

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_folder_lists_mail_newest_first(self, tmp_path: Path) -> None:
        _build_mail_repo_with_timestamps(
            tmp_path,
            [
                ("mail-1", "Oldest", "folder-1", "meta_1", 100),
                ("mail-2", "Newest", "folder-1", "meta_2", 300),
                ("mail-3", "Middle", "folder-1", "meta_3", 200),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                mails = await provider.children(folder)
                assert [mail.name for mail in mails] == ["Newest", "Middle", "Oldest"]
            finally:
                await provider.close()

    async def test_folder_lists_the_mail_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``children_of()`` for one folder is exactly one ``WHERE
        parent_folder_id = ?`` query."""
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[object] = []
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
            calls.append((where, tuple(params)))
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        [folder] = await provider.children(provider.root())
        calls.clear()
        await provider.children(folder)
        assert calls == [("parent_folder_id = ?", ("folder-1",))]


class TestM365RealFolderHierarchy:
    """The real, nested M365 mail_folder_table hierarchy
    (RecursiveGroupFlatTree) -- Inbox has zero direct mail but a real
    nested subfolder (haha) that does; Sent has direct mail of its own."""

    async def test_root_lists_real_folder_names_including_a_zero_mail_folder(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                folders = await provider.children(provider.root())
                # Inbox has zero direct mail -- it must still appear.
                assert {folder.name for folder in folders} == {"Inbox", "Sent"}
                assert all(folder.is_leaf is False for folder in folders)
            finally:
                await provider.close()

    async def test_the_zero_mail_folder_still_shows_its_real_nested_subfolder(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                children = await provider.children(inbox)
                assert [child.name for child in children] == ["haha"]
                assert children[0].is_leaf is False
            finally:
                await provider.close()

    async def test_the_nested_subfolder_lists_its_own_mail(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                [haha] = await provider.children(inbox)
                mails = await provider.children(haha)
                assert [mail.name for mail in mails] == ["Under Haha"]
                assert mails[0].kind is UnitKind.MAIL
            finally:
                await provider.close()

    async def test_a_folder_with_direct_mail_lists_it(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [sent] = [f for f in await provider.children(provider.root()) if f.name == "Sent"]
                mails = await provider.children(sent)
                assert [mail.name for mail in mails] == ["Under Sent"]
            finally:
                await provider.close()

    async def test_unit_on_a_folder_raises(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                with pytest.raises(NotRestorableError, match="not a restorable unit"):
                    await provider.unit(inbox)
            finally:
                await provider.close()


class TestArchiveMailFolderHierarchy:
    """Archive Mail shares regular Mail's hierarchy wiring
    (``_make_build_tree``/``_open_m365_folder_tree``); one representative
    test proves Archive reaches it too."""

    async def test_root_lists_real_folder_names_including_a_zero_mail_folder(self, tmp_path: Path) -> None:
        write_saas_stream_dbs(tmp_path, _IDS)

        folders = [
            ("root", "RootLabel", "anchor", 1),
            ("inbox", "Inbox", "root", 0),
            ("haha", "haha", "inbox", 0),
        ]
        mails = [("mail-1", "Under Haha", "haha", "meta_1")]
        mail_db_bytes = mail_db(mails)
        folder_db_bytes = _build_mail_folder_hierarchy_db(folders)
        write_indexed_saas_obj(
            tmp_path,
            _IDS,
            session_id=27,
            version_uid=_version().version_uid,
            payloads=[("mail_svc", mail_db_bytes), ("folder_svc", folder_db_bytes)],
            db_objects=[("archive_mail_db", "mail_svc"), ("archive_mail_folder_db", "folder_svc")],
        )

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_archive_mail_provider(repo, _version(), saas_streams)
            try:
                [inbox] = await provider.children(provider.root())
                assert inbox.name == "Inbox"
                [haha] = await provider.children(inbox)
                assert haha.name == "haha"
                mails_list = await provider.children(haha)
                assert [mail.name for mail in mails_list] == ["Under Haha"]
            finally:
                await provider.close()


class TestUnit:
    async def test_builds_a_valid_eml_with_the_real_attachment_bytes(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        [mail] = await provider.children(folder)
        content = (await provider.unit(mail)).content
        data = await content.read()

        reparsed = email.message_from_bytes(data, policy=email.policy.compat32)
        assert reparsed["Subject"] == "Hello"
        [attachment] = [p for p in reparsed.walk() if p.get_filename() == "file.bin"]
        assert attachment.get_payload(decode=True) == b"the real attachment bytes"
        assert attachment.get("X-ABL-ID") is None

    async def test_unit_name_has_eml_extension(self, provider: SaasWorkloadProvider[Any]) -> None:
        [folder] = await provider.children(provider.root())
        [mail] = await provider.children(folder)
        unit = await provider.unit(mail)
        assert unit.name == "Hello.eml"

    async def test_the_listed_node_exports_under_the_units_own_name(self, provider: SaasWorkloadProvider[Any]) -> None:
        """A folder export names files from the listing alone, without assembling each mail."""
        [folder] = await provider.children(provider.root())
        [mail] = await provider.children(folder)
        assert mail.export_name == (await provider.unit(mail)).name == "Hello.eml"

    async def test_unit_name_for_an_empty_subject_mail_is_no_subject_not_the_raw_id(self, tmp_path: Path) -> None:
        _build_mail_repo(tmp_path, subject="")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                [mail] = await provider.children(folder)
                unit = await provider.unit(mail)
                assert unit.name == "(no subject).eml"
            finally:
                await provider.close()


class TestAssembleEmlErrors:
    async def test_missing_skeleton_raises_data_corrupt_on_first_access(self, tmp_path: Path) -> None:
        _build_mail_repo_without_skeleton(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [folder] = await p.children(p.root())
                [mail] = await p.children(folder)
                unit = await p.unit(mail)  # must not raise here — lazy assembly
                with pytest.raises(DataCorruptError, match="no skeleton"):
                    await unit.content.read()
            finally:
                await p.close()

    async def test_meta_bytes_not_valid_json_raises_data_corrupt_on_first_access(self, tmp_path: Path) -> None:
        _build_mail_repo_with_malformed_meta(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await open_mail_provider(repo, _version(), saas_streams)
            try:
                [folder] = await p.children(p.root())
                [mail] = await p.children(folder)
                unit = await p.unit(mail)  # must not raise here — lazy assembly
                with pytest.raises(DataCorruptError, match="did not parse as JSON"):
                    await unit.content.read()
            finally:
                await p.close()


def _build_mail_repo_with_malformed_meta(tmp_path: Path, *, session_id: int = 13) -> None:
    """Same shape as ``_build_mail_repo`` but the META object's bytes
    aren't valid JSON at all (a genuinely corrupted repository, as
    opposed to ``_build_mail_repo_without_skeleton``'s well-formed-but-
    incomplete META)."""
    write_saas_stream_dbs(tmp_path, _IDS)

    mail_db_bytes = mail_db([("mail-1", "Hello", "folder-1", "meta_1")])
    meta_1 = b"not json at all"

    payloads = [("mail_svc", mail_db_bytes), ("meta_1", meta_1)]
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=[("mail_db", "mail_svc")],
    )


def _build_mail_repo_without_skeleton(tmp_path: Path, *, session_id: int = 12) -> None:
    """Same shape as ``_build_mail_repo`` but the META's
    ``content_list`` omits the type=0 skeleton entry entirely."""
    write_saas_stream_dbs(tmp_path, _IDS)

    mail_db_bytes = mail_db([("mail-1", "Hello", "folder-1", "meta_1")])
    meta_1 = json.dumps({"version": "3.0", "content_list": []}).encode()

    payloads = [("mail_svc", mail_db_bytes), ("meta_1", meta_1)]
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=[("mail_db", "mail_svc")],
    )


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_mail_table_exists(self, tmp_path: Path) -> None:
        write_saas_stream_dbs(tmp_path, _IDS)

        write_saas_obj(tmp_path, _IDS, session_id=11, content=b"\x00" * 4096)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index for table"):
                await open_mail_provider(repo, _version(), saas_streams)

    async def test_tree_factory_failure_closes_resources_instead_of_leaking_them(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``tree_factory`` failure happens after every table is already
        open (``self._sources`` and the index ObjectDB) -- ``create()``
        must close them."""
        _build_mail_repo(tmp_path)

        async def failing_tree_factory(provider: SaasWorkloadProvider[Any]) -> tuple[TreeStrategy, MailState]:
            raise RuntimeError("synthetic tree_factory failure")

        failing_config = dataclasses.replace(MAIL_CONFIG, tree_factory=failing_tree_factory)

        closed_instances: list[SaasWorkloadProvider[Any]] = []
        original_close = SaasWorkloadProvider.close

        async def spy_close(self: SaasWorkloadProvider[Any]) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(SaasWorkloadProvider, "close", spy_close)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(RuntimeError, match="synthetic tree_factory failure"):
                await SaasWorkloadProvider.create(repo, _version(), failing_config, saas_streams)
        assert len(closed_instances) == 1

    async def test_cancelled_create_still_closes_its_resources(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancellation is not an ``Exception``: ``create()``'s cleanup must
        still close every connection already opened, or the leaked aiosqlite
        thread keeps the interpreter alive."""
        _build_mail_repo(tmp_path)

        async def cancelled_tree_factory(provider: SaasWorkloadProvider[Any]) -> tuple[TreeStrategy, MailState]:
            raise asyncio.CancelledError

        cancelled_config = dataclasses.replace(MAIL_CONFIG, tree_factory=cancelled_tree_factory)

        closed_instances: list[SaasWorkloadProvider[Any]] = []
        original_close = SaasWorkloadProvider.close

        async def spy_close(self: SaasWorkloadProvider[Any]) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(SaasWorkloadProvider, "close", spy_close)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(asyncio.CancelledError):
                await SaasWorkloadProvider.create(repo, _version(), cancelled_config, saas_streams)
        assert len(closed_instances) == 1

    async def test_tree_factory_schema_drift_degrades_to_unsupported_data_format(self, tmp_path: Path) -> None:
        """A ``tree_factory``'s ``DataCorruptError``/``sqlite3.DatabaseError``
        (what a connector-version schema mismatch produces) converts to
        ``UnsupportedDataFormatError``, so the candidate degrades like any
        other non-match instead of crashing ``saas_provider_for``'s
        dispatch loop."""
        _build_mail_repo(tmp_path)

        async def failing_tree_factory(provider: SaasWorkloadProvider[Any]) -> tuple[TreeStrategy, MailState]:
            raise DataCorruptError("synthetic schema drift")

        failing_config = dataclasses.replace(MAIL_CONFIG, tree_factory=failing_tree_factory)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="tree construction failed for version"):
                await SaasWorkloadProvider.create(repo, _version(), failing_config, saas_streams)


class TestOpenArchiveMailProvider:
    """``open_archive_mail_provider``/``ARCHIVE_MAIL_CONFIG``: a separate M365-only
    mailbox with regular Mail's schema, resolved under the index's
    ``archive_mail_db`` name instead of ``mail_db`` (``_build_mail_repo``
    with ``index_db_name="archive_mail_db"``)."""

    async def test_root_group_is_named_archive_not_mail(self, tmp_path: Path) -> None:
        _build_mail_repo(tmp_path, index_db_name="archive_mail_db")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_archive_mail_provider(repo, _version(), saas_streams)
            try:
                assert provider.root().name == "Archive"
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                folder = folders[0]
                mails = await provider.children(folder)
                assert len(mails) == 1
                assert mails[0].name == "Hello"
                assert mails[0].kind is UnitKind.MAIL
            finally:
                await provider.close()

    async def test_raises_unsupported_data_format_when_the_catalog_has_no_archive_mail_db(self, tmp_path: Path) -> None:
        # The default fixture registers only "mail_db"; with no
        # "archive_mail_db" index entry there is nothing to open.
        _build_mail_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index entry for table"):
                await open_archive_mail_provider(repo, _version(), saas_streams)
