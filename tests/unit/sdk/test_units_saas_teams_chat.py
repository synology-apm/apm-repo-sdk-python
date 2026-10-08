"""Unit tests for ``synology_apm_repo.sdk.units.saas.teams_chat`` and
``teams_discovery`` — a full synthetic repository root (``saas_fakes``)
with a hand-built index object plus one channel-list DB and per-channel
message DBs embedded in its ``saas_obj`` content. Comments citing "module docstring" mean ``saas/teams_chat.py``'s or
``saas/teams_discovery.py``'s."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard

from support.fakes import faithful_to
from support.model_factories import make_version
from support.repo_builders import (
    write_repo_info,
    write_workload_config,
)
from synology_apm_repo.sdk.api.export import run_export
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import (
    DataCorruptError,
    NotFoundError,
    NotRestorableError,
    ResourceLimitExceededError,
    UnsupportedDataFormatError,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Table
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.saas import teams_discovery as teams_discovery_module
from synology_apm_repo.sdk.units.saas.context import SharedIndexObjectDb, SharedSaasContext
from synology_apm_repo.sdk.units.saas.object_name_index import ObjectNameIndex
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.services import IndexEntry, ServiceKind, SniffResult
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.teams_chat import TeamsChatProvider, _TeamsEntityFlatTree
from synology_apm_repo.sdk.units.saas.teams_discovery import (
    _chat_display_name_from_members,
    _is_container,
    channel_info,
    chat_labels,
    owning_account_email,
    resolve_message_index,
)
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    channel_list_db,
    chat_list_db,
    index_json,
    write_empty_saas_repo,
    write_saas_object_repo,
)

_STREAM_UUID = "teams-stream-uuid"
_IDS = SaasStreamIds(stream_id=29, stream_uuid=_STREAM_UUID)


def _build_message_db(messages: list[tuple[str, str]], *, stickers: dict[str, dict[str, str]] | None = None) -> bytes:
    """``messages``: (sender display name, content_preview). ``author`` and
    ``metadata`` are JSON strings, as in real ``msg_info_table`` rows.
    ``stickers`` (``msg_id -> {url: base64_content}``) also creates a
    ``sticker_info_table``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "msg.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE msg_info_table(row_id INTEGER PRIMARY KEY, msg_id TEXT, author TEXT, "
            "create_time INTEGER, content_preview TEXT, metadata TEXT, is_sys_message INTEGER, reply_to_id TEXT)"
        )
        base_time = 1700000000
        rows = []
        for i, (sender, preview) in enumerate(messages):
            author = json.dumps({"email": "", "id": "", "name": sender, "tenant_id": ""})
            metadata = json.dumps(
                {
                    "attachments": [],
                    "body": {"content": preview, "contentType": "text"},
                    "createdDateTime": f"2023-11-14T22:{13 + i:02d}:20.{i:03d}Z",
                    "from": {"user": {"displayName": sender}},
                }
            )
            rows.append((str(i), author, base_time + i, preview, metadata))
        conn.executemany(
            "INSERT INTO msg_info_table(msg_id, author, create_time, content_preview, metadata, "
            "is_sys_message) VALUES (?, ?, ?, ?, ?, 0)",
            rows,
        )
        if stickers:
            conn.execute("CREATE TABLE sticker_info_table(msg_id TEXT, url TEXT, base64_content TEXT)")
            conn.executemany(
                "INSERT INTO sticker_info_table VALUES (?, ?, ?)",
                [(msg_id, url, content) for msg_id, by_url in stickers.items() for url, content in by_url.items()],
            )
        conn.commit()
        conn.close()
        return path.read_bytes()


def _build_message_db_compressed(messages: list[tuple[str, str]]) -> bytes:
    return zstandard.ZstdCompressor().compress(_build_message_db(messages))


def _build_teams_repo(
    tmp_path: Path,
    *,
    list_db_bytes: bytes,
    list_db_name: str,
    entries: list[tuple[str, bytes]],
    session_id: int = 31,
) -> None:
    """``entries``: (channel_or_chat_id, message_db_compressed_bytes).
    The ``saas_obj`` holds ``list_db``, the message DBs, then the index; the
    provider locates each by object_id, so order doesn't matter. The one
    ``db_objects`` entry, ``"db_infos_in_snapshot"``, points at the index
    object itself (module docstring)."""
    index_bytes = index_json([(list_db_name, "list_db"), *[(cid, f"msg_{cid}") for cid, _ in entries]])
    write_saas_object_repo(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=[("list_db", list_db_bytes), *[(f"msg_{cid}", data) for cid, data in entries], ("index", index_bytes)],
        db_objects=[("db_infos_in_snapshot", "index")],
    )


def _version() -> Version:
    return make_version(
        version_id=71,
        version_uid="vuid-teams",
        target_type="M365",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def channel_provider(tmp_path: Path) -> AsyncIterator[TeamsChatProvider]:
    _build_teams_repo(
        tmp_path,
        list_db_bytes=channel_list_db([("chan-a", "Alpha"), ("chan-b", "Beta")]),
        list_db_name="teams_channel_db",
        entries=[
            ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
            ("chan-b", _build_message_db_compressed([("Bob", "hi from beta")])),
        ],
    )
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
        try:
            yield provider
        finally:
            await provider.close()


async def _channels_of(provider: TeamsChatProvider) -> list[Node]:
    """Every channel across the category level ``children(root())``
    inserts (fixtures here default to Standard, so normally one category)."""
    channels: list[Node] = []
    for category in await provider.children(provider.root()):
        channels.extend(await provider.children(category))
    return channels


class TestChannelTree:
    def test_root_is_named_channels(self, channel_provider: TeamsChatProvider) -> None:
        assert channel_provider.root().name == "Channels"

    async def test_root_children_are_a_single_standard_channels_category(
        self, channel_provider: TeamsChatProvider
    ) -> None:
        # No fixture sets channel_type, so only Standard appears, never an
        # empty Private/Shared.
        categories = await channel_provider.children(channel_provider.root())
        assert [c.name for c in categories] == ["Standard Channels"]
        assert all(not c.is_leaf for c in categories)

    async def test_children_are_named_from_channel_info_table(self, channel_provider: TeamsChatProvider) -> None:
        names = {n.name for n in await _channels_of(channel_provider)}
        assert names == {"Alpha", "Beta"}

    async def test_children_are_leaves_with_teams_chat_message_kind(self, channel_provider: TeamsChatProvider) -> None:
        # The browser routes these leaves to its chat-transcript preview by kind alone.
        for node in await _channels_of(channel_provider):
            assert node.is_leaf
            assert node.kind is UnitKind.TEAMS_CHAT_MESSAGE
            assert node.degraded is None

    async def test_pagination(self, channel_provider: TeamsChatProvider) -> None:
        [category] = await channel_provider.children(channel_provider.root())
        one = await channel_provider.children(category, offset=0, limit=1)
        assert len(one) == 1

    async def test_channels_are_listed_alphabetically_not_index_order(self, tmp_path: Path) -> None:
        # Inserted in reverse alphabetical order; index order isn't meaningful.
        _build_teams_repo(
            tmp_path,
            list_db_bytes=channel_list_db([("chan-z", "Zeta"), ("chan-a", "Alpha")]),
            list_db_name="teams_channel_db",
            entries=[
                ("chan-z", _build_message_db_compressed([("Zoe", "hi from zeta")])),
                ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                names = [n.name for n in await _channels_of(provider)]
                assert names == ["Alpha", "Zeta"]
            finally:
                await provider.close()

    async def test_a_channel_missing_from_channel_info_table_still_lists_under_standard(self, tmp_path: Path) -> None:
        """A channel the index names but ``channel_info_table`` has no row
        for is still listed, defaulting to Standard."""
        _build_teams_repo(
            tmp_path,
            list_db_bytes=channel_list_db([("chan-a", "Alpha")]),  # "chan-b" has no row here
            list_db_name="teams_channel_db",
            entries=[
                ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
                ("chan-b", _build_message_db_compressed([("Bob", "hi from beta")])),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await TeamsChatProvider.create(repo, _version(), saas_streams) as provider,
        ):
            categories = await provider.children(provider.root())
            assert [c.name for c in categories] == ["Standard Channels"]
            names = {n.name for n in await provider.children(categories[0])}
            assert names == {"Alpha", "chan-b"}  # chan-b falls back to its raw id -- no name row either


class TestChannelUnit:
    async def test_unit_content_is_an_html_page_with_the_real_message(
        self, channel_provider: TeamsChatProvider
    ) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        unit = await channel_provider.unit(node)
        page = (await unit.content.read()).decode("utf-8")
        assert page.startswith("<!doctype html>")
        assert "Alice" in page
        assert "hi from alpha" in page
        assert "Alpha" in page  # the channel name, in the page title/heading

    async def test_unit_name_has_html_suffix(self, channel_provider: TeamsChatProvider) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        assert (await channel_provider.unit(node)).name == "Alpha.html"

    async def test_exported_content_is_a_well_formed_self_contained_html_file(
        self, channel_provider: TeamsChatProvider, tmp_path: Path
    ) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Beta")
        unit = await channel_provider.unit(node)
        dst = tmp_path / "export" / "beta.html"
        dst.parent.mkdir(parents=True, exist_ok=True)
        await run_export(unit.content, LocalFileSink(dst, staged=False))
        page = dst.read_text(encoding="utf-8")
        assert "Bob" in page
        assert "hi from beta" in page
        # Self-contained: no tag makes a browser fetch an external resource
        # (a bare "http://" inside escaped text is fine).
        assert "<script" not in page
        assert "<img" not in page
        assert "<link" not in page
        assert "<iframe" not in page
        assert 'src="http' not in page
        assert 'href="http' not in page

    async def test_a_real_sticker_table_gets_read_and_embedded_in_the_exported_page(self, tmp_path: Path) -> None:
        """``_read_stickers`` reads a ``sticker_info_table`` through a live
        connection (``test_units_content_saas_teams_chat.py``'s ``TestRenderChannelHtml``
        passes the dict in directly)."""
        _build_teams_repo(
            tmp_path,
            list_db_bytes=channel_list_db([("chan-a", "Alpha")]),
            list_db_name="teams_channel_db",
            entries=[
                (
                    "chan-a",
                    zstandard.ZstdCompressor().compress(
                        _build_message_db(
                            [("Alice", '<img src="https://graph.microsoft.com/sticker1">caption')],
                            stickers={"0": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}},
                        )
                    ),
                ),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                [node] = await _channels_of(provider)
                page = (await (await provider.unit(node)).content.read()).decode("utf-8")
                assert '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,BASE64DATA">' in page
            finally:
                await provider.close()

    async def test_unit_on_a_node_with_no_object_id_raises(self, channel_provider: TeamsChatProvider) -> None:
        real_node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        phantom = Node(ref=real_node.ref, name="phantom", is_leaf=True)
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await channel_provider.unit(phantom)


class TestDegradedReason:
    async def test_chat_schema_not_found_at_all_reports_that_specifically(self, tmp_path: Path) -> None:
        # Unreachable while _is_container() requires one of the two tables;
        # the flag is set directly to exercise the branch.
        list_db = chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        _build_teams_repo(
            tmp_path,
            list_db_bytes=list_db,
            list_db_name="chat_db",
            entries=[("chat-1", _build_message_db_compressed([("Carol", "hi from chat")]))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                provider._chat_schema_found = False
                provider._labels = {}  # ensure this entity_id isn't already labeled
                entity_id = next(iter(provider._entity_object_ids))
                assert provider._degraded_reason(entity_id) == (
                    "this backup has no chat names for this version — showing raw chat ids"
                )
            finally:
                await provider.close()


class TestChatFallback:
    """Chat support is unverified against a real chat backup (module
    docstring); these cover ``chat_labels``'s by-name column lookup and its
    failure mode."""

    @asynccontextmanager
    async def _open(self, tmp_path: Path, list_db_bytes: bytes) -> AsyncIterator[TeamsChatProvider]:
        _build_teams_repo(
            tmp_path,
            list_db_bytes=list_db_bytes,
            list_db_name="chat_db",  # must be in _CONTAINER_DB_NAMES
            entries=[("chat-1", _build_message_db_compressed([("Carol", "hi from chat")]))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                yield provider
            finally:
                await provider.close()

    async def test_recognizable_columns_resolve_a_label(self, tmp_path: Path) -> None:
        list_db = chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        async with self._open(tmp_path, list_db) as provider:
            assert provider.root().name == "Chats"
            node = (await provider.children(provider.root()))[0]
            assert node.name == "Carol & Dave"
            assert node.degraded is None

    async def test_unrecognizable_columns_fall_back_to_raw_id_and_flag_degraded(self, tmp_path: Path) -> None:
        list_db = chat_list_db([("chat-1", "Carol & Dave")], id_col="weird_key", label_col="weird_value")
        async with self._open(tmp_path, list_db) as provider:
            node = (await provider.children(provider.root()))[0]
            assert node.name == "chat-1"  # falls back to the raw chat id
            assert node.degraded is not None
            # Only the listing is partial: the exported content is whole.
            assert (await provider.unit(node)).degraded is None

    async def test_unit_on_a_chat_node_renders_the_real_message_html(self, tmp_path: Path) -> None:
        list_db = chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        async with self._open(tmp_path, list_db) as provider:
            node = (await provider.children(provider.root()))[0]
            unit = await provider.unit(node)
            page = (await unit.content.read()).decode("utf-8")
            assert page.startswith("<!doctype html>")
            assert "Carol" in page
            assert "hi from chat" in page


@faithful_to(ObjectDb)
class _FakeObjectDb:
    """The ``ObjectDb`` surface (``get``/``close``) that ``_is_container``
    and ``resolve_message_index`` use."""

    def __init__(
        self, locations: dict[str, tuple[int, int]], *, get_raises: dict[str, Exception] | None = None
    ) -> None:
        self._locations = locations
        self._get_raises = get_raises or {}
        self.closed = False

    async def get(self, object_id: str) -> tuple[int, int]:
        if object_id in self._get_raises:
            raise self._get_raises[object_id]
        return self._locations[object_id]

    async def close(self) -> None:
        self.closed = True


class TestIsContainer:
    """``_is_container``'s defensive branches."""

    async def test_zero_length_returns_none(self) -> None:
        db = _FakeObjectDb({"obj-a": (0, 0)})
        result = await _is_container(cast(DedupFile, object()), cast(ObjectDb, db), "obj-a")
        assert result is None

    @pytest.mark.parametrize(
        "sniff",
        [
            pytest.param(SniffResult(kind=ServiceKind.META_JSON), id="non_service_db_kind"),
            pytest.param(
                SniffResult(kind=ServiceKind.SERVICE_DB, tables=frozenset({"some_unrelated_table"})),
                id="service_db_without_a_container_table",
            ),
        ],
    )
    async def test_not_a_container_db_returns_none(self, monkeypatch: pytest.MonkeyPatch, sniff: SniffResult) -> None:
        db = _FakeObjectDb({"obj-a": (0, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return sniff

        monkeypatch.setattr(teams_discovery_module, "inspect_object", fake_inspect)
        result = await _is_container(cast(DedupFile, object()), cast(ObjectDb, db), "obj-a")
        assert result is None


class TestResolveMessageIndex:
    """``resolve_message_index`` returns ``None`` on every degraded input;
    a failed load of the index ObjectDB it reads degrades ``create()``."""

    async def test_missing_db_infos_in_snapshot_entry_returns_none(self) -> None:
        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={})
        fake_db = _FakeObjectDb({})
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )

    @pytest.mark.parametrize(
        "error", [DataCorruptError("synthetic corruption"), ResourceLimitExceededError("synthetic: no disk space")]
    )
    async def test_an_index_object_db_that_fails_to_load_degrades_create(
        self, monkeypatch: pytest.MonkeyPatch, error: Exception
    ) -> None:
        """A degradable failure loading the shared index ObjectDB makes
        create() unsupported rather than escaping."""

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise error

        monkeypatch.setattr(ObjectDb, "load", failing_load)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        dedup_file = cast(DedupFile, object())
        shared = SharedSaasContext(dedup_file, object_name_index, SharedIndexObjectDb(dedup_file, object_name_index))
        with pytest.raises(UnsupportedDataFormatError, match="no Teams/Chat channel-or-chat index found for version"):
            await TeamsChatProvider.create(cast(Any, object()), _version(), cast(Any, object()), shared=shared)

    async def test_a_failed_acquire_leaves_the_shared_hold_count_untouched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-degradable load failure escapes create(); its cleanup must
        not release a hold this provider never took."""

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise RuntimeError("synthetic unexpected failure")

        monkeypatch.setattr(ObjectDb, "load", failing_load)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        dedup_file = cast(DedupFile, object())
        hold = SharedIndexObjectDb(dedup_file, object_name_index)
        shared = SharedSaasContext(dedup_file, object_name_index, hold)
        with pytest.raises(RuntimeError, match="synthetic unexpected failure"):
            await TeamsChatProvider.create(cast(Any, object()), _version(), cast(Any, object()), shared=shared)
        assert hold._holders == 0

    async def test_index_object_id_not_in_the_object_db_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_db = _FakeObjectDb({}, get_raises={"idx-obj": NotFoundError("no such object", ref="idx-obj")})

        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )
        assert fake_db.closed is False  # borrowed, never closed here

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(DataCorruptError("bad index bytes"), id="data_corrupt"),
            # Also in DEGRADABLE_OPEN_ERRORS: degrade to None, don't abort
            # the saas_provider_for dispatch.
            pytest.param(ResourceLimitExceededError("synthetic: not enough disk space"), id="insufficient_disk_space"),
        ],
    )
    async def test_inspect_object_failure_on_the_index_itself_returns_none(
        self, monkeypatch: pytest.MonkeyPatch, error: Exception
    ) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def raising_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            raise error

        monkeypatch.setattr(teams_discovery_module, "inspect_object", raising_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )
        assert fake_db.closed is False  # borrowed, never closed here

    async def test_index_object_that_does_not_actually_look_like_an_index_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(kind=ServiceKind.META_JSON)

        monkeypatch.setattr(teams_discovery_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )
        assert fake_db.closed is False  # borrowed, never closed here

    async def test_index_with_no_recognized_container_entry_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(
                kind=ServiceKind.INDEX, index_entries=(IndexEntry(name="some_other_db", object_id="other-obj"),)
            )

        monkeypatch.setattr(teams_discovery_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )
        assert fake_db.closed is False  # borrowed, never closed here

    async def test_a_recognized_container_entry_that_does_not_validate_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The index names a container entry whose own object fails
        # _is_container's inspect_object check.
        fake_db = _FakeObjectDb({"idx-obj": (0, 10), "container-obj": (100, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            if offset == 0:
                return SniffResult(
                    kind=ServiceKind.INDEX,
                    index_entries=(IndexEntry(name="teams_channel_db", object_id="container-obj"),),
                )
            return SniffResult(kind=ServiceKind.META_JSON)  # the container object itself doesn't validate

        monkeypatch.setattr(teams_discovery_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )
        assert fake_db.closed is False  # borrowed, never closed here

    @pytest.mark.parametrize(
        "error",
        [
            DataCorruptError("synthetic corrupt container"),
            ResourceLimitExceededError("synthetic: no disk space"),
            NotFoundError("container object missing", ref="container-obj"),
        ],
    )
    async def test_a_container_object_that_fails_to_open_returns_none(
        self, monkeypatch: pytest.MonkeyPatch, error: Exception
    ) -> None:
        """The container gets the same degradation as the INDEX object."""
        fake_db = _FakeObjectDb({"idx-obj": (0, 10), "container-obj": (100, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            if offset == 0:
                return SniffResult(
                    kind=ServiceKind.INDEX,
                    index_entries=(IndexEntry(name="teams_channel_db", object_id="container-obj"),),
                )
            raise error

        monkeypatch.setattr(teams_discovery_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert (
            await resolve_message_index(cast(DedupFile, object()), object_name_index, cast(ObjectDb, fake_db)) is None
        )


class TestOwningAccountEmail:
    async def test_no_matching_workload_row_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_workload_config(tmp_path / "db" / "workload_config", [(999, "wl-999", "M365", {})])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await owning_account_email(repo, _version()) is None

    async def test_matching_row_with_no_email_returns_none(self, tmp_path: Path) -> None:
        spec: dict[str, object] = {"status": {"entity_meta": {"spec": {"user_info": {}}}}}
        write_repo_info(tmp_path / "repo_info")
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "wl-1", "M365", spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await owning_account_email(repo, _version()) is None

    async def test_matching_row_with_a_real_email_returns_it(self, tmp_path: Path) -> None:
        spec = {"status": {"entity_meta": {"spec": {"user_info": {"email": "alice@example.com"}}}}}
        write_repo_info(tmp_path / "repo_info")
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "wl-1", "M365", spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await owning_account_email(repo, _version()) == "alice@example.com"

    async def test_schema_drifted_workload_config_returns_none_instead_of_raising(self, tmp_path: Path) -> None:
        """A ``workload_config`` without a ``workload_spec`` column
        (``Table.create`` raises ``DataCorruptError``) returns ``None``: the
        lookup only enriches labels."""
        write_repo_info(tmp_path / "repo_info")
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "workload_config")
        conn.execute("CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO workload_config VALUES (1)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await owning_account_email(repo, _version()) is None


def _build_plain_sqlite_bytes(build: Callable[[sqlite3.Connection], object]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "container.db"
        conn = sqlite3.connect(path)
        build(conn)
        conn.commit()
        conn.close()
        return path.read_bytes()


async def _open_plain_sqlite(build: Callable[[sqlite3.Connection], object]) -> SqliteSource:
    """``_build_plain_sqlite_bytes`` opened as the ``SqliteSource`` that
    ``channel_info``/``chat_labels`` take (as ``TeamsChatProvider.create``
    passes after ``open_service_db()``)."""
    return await SqliteSource.from_bytes(_build_plain_sqlite_bytes(build))


class TestChatLabels:
    """``chat_labels`` over already-decompressed plain SQLite."""

    async def test_no_chat_info_table_at_all_returns_empty(self) -> None:
        async with await _open_plain_sqlite(lambda conn: conn.execute("CREATE TABLE unrelated(x INTEGER)")) as source:
            assert await chat_labels(source, None) == ({}, {})

    async def test_a_sqlite_error_while_reading_labels_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic')")

        async def raising_select(
            self: Table, where: str = "", params: object = (), **kwargs: object
        ) -> AsyncIterator[dict[str, object]]:
            raise sqlite3.OperationalError("synthetic corruption for this test")
            yield {}  # pragma: no cover - unreachable, makes this a real async generator

        monkeypatch.setattr(Table, "select", raising_select)
        async with await _open_plain_sqlite(build) as source:
            assert await chat_labels(source, None) == ({}, {})

    async def test_bot_label_for_an_unnamed_one_on_one_chat_with_no_derivable_member_name(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, chat_type INTEGER)")
            # No topic and chat_type=0 (ONE_ON_ONE, not MEETING): the first
            # pass leaves this chat unlabeled.
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', '', 0)")
            conn.execute("CREATE TABLE chat_members_table(chat_id TEXT, members TEXT)")
            # An empty member list, as in a Teams bot/app chat: no name to derive.
            conn.execute("INSERT INTO chat_members_table VALUES ('chat-1', '[]')")

        async with await _open_plain_sqlite(build) as source:
            labels, _create_times = await chat_labels(source, "me@example.com")
        assert labels == {"chat-1": "Bot"}

    async def test_no_title_meeting_gets_the_literal_meeting_label(self) -> None:
        # chat_type 2 (MEETING) with no topic.
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, chat_type INTEGER)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', '', 2)")

        async with await _open_plain_sqlite(build) as source:
            labels, _create_times = await chat_labels(source, None)
        assert labels == {"chat-1": "(no title)"}

    async def test_create_time_is_read_when_the_column_is_present(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, create_time INTEGER)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic', 1700000000)")

        async with await _open_plain_sqlite(build) as source:
            _labels, create_times = await chat_labels(source, None)
        assert create_times == {"chat-1": 1700000000}

    async def test_create_time_is_absent_when_the_column_is_missing(self) -> None:
        # chat_info_table's columns are looked up by name; a missing one must not crash.
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic')")

        async with await _open_plain_sqlite(build) as source:
            _labels, create_times = await chat_labels(source, None)
        assert create_times == {}


class TestChannelInfo:
    async def test_channels_with_no_name_are_omitted(self) -> None:
        # An empty/null name yields no label. With no channel_type column,
        # Table backfills None (required=False), treated as Standard.
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', '')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-3', NULL)")

        async with await _open_plain_sqlite(build) as source:
            labels, categories, create_times = await channel_info(source)
        assert labels == {"ch-1": "General"}
        assert categories == {"ch-1": "standard", "ch-2": "standard", "ch-3": "standard"}
        assert create_times == {}

    async def test_channel_type_drives_category(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT, channel_type TEXT)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General', 'standard')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', 'private 2', 'private')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-3', 'shared 1', 'shared')")
            # An unrecognized channel_type degrades to Standard.
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-4', 'weird', 'unknown-type')")

        async with await _open_plain_sqlite(build) as source:
            _labels, categories, _create_times = await channel_info(source)
        assert categories == {"ch-1": "standard", "ch-2": "private", "ch-3": "shared", "ch-4": "standard"}

    async def test_create_time_is_read_when_present(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT, create_time INTEGER)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General', 1700000000)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', 'No Time', NULL)")

        async with await _open_plain_sqlite(build) as source:
            _labels, _categories, create_times = await channel_info(source)
        assert create_times == {"ch-1": 1700000000}


class TestTeamsEntityFlatTree:
    """``_TeamsEntityFlatTree``, the ``TreeStrategy`` behind the
    channel/chat listing (bare for Chat, inside ``CategorizedGroupTree`` for
    Channel); needs no repository."""

    async def test_children_of_a_leafs_own_key_is_empty(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1"}, {})
        assert await tree.children_of(("chat-1",)) == []

    async def test_each_listed_entity_carries_its_message_db_object_id(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1", "chat-2": "obj-2"}, {"chat-2": "Alpha"})
        entries = await tree.children_of(())
        assert [(entry.key, entry.name, entry.is_leaf, entry.row) for entry in entries] == [
            (("chat-2",), "Alpha", True, {"entity_id": "chat-2", "object_id": "obj-2"}),
            (("chat-1",), "chat-1", True, {"entity_id": "chat-1", "object_id": "obj-1"}),
        ]


class TestDegradation:
    async def test_no_index_raises_unsupported_data_format(self, tmp_path: Path) -> None:
        write_empty_saas_repo(tmp_path, _IDS, session_id=30)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(
                UnsupportedDataFormatError, match="no Teams/Chat channel-or-chat index found for version"
            ):
                await TeamsChatProvider.create(repo, _version(), saas_streams)

    async def test_no_index_found_closes_the_half_built_provider(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``create()`` closes the half-built provider, releasing any index
        hold it took, on the routine no-index degrade path too."""
        write_empty_saas_repo(tmp_path, _IDS, session_id=30)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        closed_instances = []
        original_close = TeamsChatProvider.close

        async def spy_close(self: TeamsChatProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(TeamsChatProvider, "close", spy_close)
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(
                UnsupportedDataFormatError, match="no Teams/Chat channel-or-chat index found for version"
            ):
                await TeamsChatProvider.create(repo, _version(), saas_streams)
            assert len(closed_instances) == 1


class TestChatDisplayNameFromMembers:
    """``_chat_display_name_from_members``: pure formatting."""

    @pytest.mark.parametrize(
        "members",
        [
            pytest.param("not json", id="malformed_json_string"),
            pytest.param(json.dumps({"not": "a list"}), id="non_list_json"),
            pytest.param(None, id="non_string_input"),
        ],
    )
    def test_unusable_members_returns_none(self, members: str | None) -> None:
        assert _chat_display_name_from_members(members, "me@example.com") is None

    def test_derives_a_comma_joined_name_from_other_members_excluding_self(self) -> None:
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"display_name": "Bob", "userEmail": "bob@example.com"},
                {"display_name": "Carol", "userEmail": "carol@example.com"},
            ]
        )
        assert _chat_display_name_from_members(members, "me@example.com") == "Bob, Carol"

    def test_every_member_being_self_or_nameless_returns_none(self) -> None:
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"userEmail": "no-name@example.com"},  # no display_name at all
            ]
        )
        assert _chat_display_name_from_members(members, "me@example.com") is None

    def test_self_stays_included_when_self_email_matches_no_member(self) -> None:
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"display_name": "Bob", "userEmail": "bob@example.com"},
            ]
        )
        assert _chat_display_name_from_members(members, "someone-else@example.com") == "Me, Bob"
