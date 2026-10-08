"""Unit tests for ``synology_apm_repo.sdk.units.saas.stream`` —
synthetic repository roots written to real files
(``tests/integration/sdk/test_units_saas_stream.py`` is the real-data
counterpart)."""

from __future__ import annotations

import asyncio
import dataclasses
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from support.fakes import faithful_to
from support.model_factories import make_version
from support.repo_builders import (
    write_bare_connection_config,
    write_bucket,
    write_composition,
    write_file_map,
    write_repo_info,
    write_saas_snapshot_db,
    write_saas_version_db,
    write_vault_encryption_key_db,
)
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    SnapshotUuid,
    StreamUuid,
    VersionUid,
)
from synology_apm_repo.sdk.storage.base import Entry
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column
from synology_apm_repo.sdk.units.saas.stream import SaasStream, SaasStreamCache, _nearest_live_generation

_STREAM_ID = 9
_CCID = ConnectionConfigId(1)
_CONNECTION_ID = "conn-1"
_STREAM_UUID = StreamUuid("stream-uuid-1")
_SAAS_OBJ = b"saas-obj-content" * 272  # not chunk-aligned on purpose — read() must still slice correctly
assert len(_SAAS_OBJ) == 4352


def _build_saas_repo(
    tmp_path: Path,
    *,
    session_id: int = 5,
    middle_segment: str = _CONNECTION_ID,
    stream_version: int = 1,
    saas_snapshot_suffix: str = "",
) -> None:
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    saas_obj_path = f"{_STREAM_UUID}/{middle_segment}/{stream_version}/saas_obj"
    write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, 2, 2)])
    write_bare_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    write_saas_snapshot_db(
        stream_db_dir / f"saas_snapshot{saas_snapshot_suffix}",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, len(_SAAS_OBJ), 1, 3)],
    )
    write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, 3, stream_version, 0)],
        target_type="M365",
    )

    write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    write_bucket(
        tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
        [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
    )


def _version(*, saas_version_id: int = 3, connection_config_id: int = _CCID) -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-saas",
        connection_config_id=connection_config_id,
        target_type="M365",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid-1",
        saas_version_id=saas_version_id,
    )


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    _build_saas_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestDbPathResolution:
    async def test_prefers_bare_file_over_suffixed(self, tmp_path: Path) -> None:
        """The bare saas_version/saas_snapshot file wins even over a
        higher-numbered suffixed one."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        # a decoy suffixed snapshot db with a *different* snapshot_uuid —
        # if the decoy were read instead of the bare file, stream_version_for
        # (which looks up snapshot_uuid="snap-uuid-1") would raise NotFoundError.
        write_saas_snapshot_db(
            stream_db_dir / "saas_snapshot.5",
            snapshots=[(99, "decoy-uuid", 1, 1)],
            distribution=[],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_falls_back_to_largest_suffix_when_no_bare_file(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_snapshot"
        suffixed = stream_db_dir / "saas_snapshot.3"
        bare.rename(suffixed)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    @pytest.mark.parametrize(
        ("logical_name", "suffixed_name"),
        [
            pytest.param("saas_snapshot", "saas_snapshot.3", id="snapshot"),
            # Each logical name has its own candidate files.
            pytest.param("saas_version", "saas_version.7", id="version"),
        ],
    )
    async def test_falls_back_to_suffix_when_bare_file_is_empty(
        self, tmp_path: Path, logical_name: str, suffixed_name: str
    ) -> None:
        """A bare file that exists but is empty (an earlier rotation's
        placeholder) is not live; resolution falls back to the largest
        suffixed generation, the same as when the bare file is absent."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / logical_name
        suffixed = stream_db_dir / suffixed_name
        suffixed.write_bytes(bare.read_bytes())  # the real content, at a numbered generation
        bare.write_bytes(b"")  # an empty placeholder left behind, still present
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1


class TestVersionChain:
    async def test_stream_version_for_resolves_the_real_chain(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_stream_version_for_unknown_snapshot_uuid_raises_not_found(self, repo: DedupRepo) -> None:
        bad_version = dataclasses.replace(_version(), saas_snapshot_uuid=SnapshotUuid("no-such-uuid"))
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no snapshot_info row for snapshot_uuid"):
                await stream.stream_version_for(bad_version)

    async def test_stream_version_for_unknown_version_id_raises_not_found(self, repo: DedupRepo) -> None:
        bad_version = _version(saas_version_id=999)
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no version_info row for snapshot_id"):
                await stream.stream_version_for(bad_version)

    async def test_stream_version_for_raises_not_found_when_snapshot_info_table_is_missing(
        self, tmp_path: Path
    ) -> None:
        """A valid SQLite database with zero tables degrades to
        ``NotFoundError`` (the "no matching row" signal), not
        ``DataCorruptError``."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_snapshot"
        bare.unlink()
        conn = sqlite3.connect(bare)
        conn.execute("CREATE TABLE _placeholder(x)")
        conn.execute("DROP TABLE _placeholder")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="snapshot_info unreadable"):
                await stream.stream_version_for(_version())


class TestOpenSaasObj:
    async def test_opens_via_connection_id_when_that_is_the_file_map_hit(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert f.stream_id == _STREAM_ID
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_opens_via_connection_config_id_when_that_is_the_file_map_hit(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path, middle_segment=str(_CCID))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_raises_not_found_when_neither_candidate_hits(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path, middle_segment="some-other-connection")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no live saas_obj for stream_version>"):
                await stream.open_saas_obj(_version())

    async def test_missing_connection_config_row_still_tries_the_ccid_candidate(self, tmp_path: Path) -> None:
        # middle segment is the numeric ccid, and connection_config has no
        # matching row at all (no connection_id candidate to try first).
        _build_saas_repo(tmp_path, middle_segment=str(_CCID))
        (tmp_path / "db" / "connection_config").unlink()
        conn = sqlite3.connect(tmp_path / "db" / "connection_config")
        conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ


def _build_saas_repo_multi_gen(
    tmp_path: Path,
    *,
    live_stream_versions: list[int],
    latest_complete_version: int | None,
    session_id: int = 5,
    middle_segment: str = _CONNECTION_ID,
    non_complete_stream_versions: frozenset[int] = frozenset(),
) -> None:
    """Like ``_build_saas_repo`` but ``version_info`` records
    ``stream_version=1`` for ``_version()``'s default row (the
    "requested" generation) while ``file_map`` only has rows for
    ``live_stream_versions`` — the real shape left behind once older
    generations are server-side GC'd (FORMAT-SPEC.md: Generic SaaS object addressing). Every live
    generation shares one physical composition/bucket (same
    stream_id/session_id) — these tests only need to prove *which*
    generation resolution picks, not that content differs across
    generations. ``non_complete_stream_versions`` gives those entries
    ``status=1`` (Written) instead of ``2`` (Complete)."""
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    write_file_map(
        tmp_path / "db" / "file_map",
        [
            (
                f"{_STREAM_UUID}/{middle_segment}/{v}/saas_obj",
                _STREAM_ID,
                session_id,
                64,
                2,
                1 if v in non_complete_stream_versions else 2,
            )
            for v in live_stream_versions
        ],
    )
    write_bare_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, len(_SAAS_OBJ), 1, 3)],
    )
    write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, 3, 1, 0)],
        target_type="M365",
        latest_complete_version=latest_complete_version,
    )

    write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    write_bucket(
        tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
        [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
    )


class TestNearestLiveGeneration:
    @pytest.mark.parametrize(
        ("candidates", "requested", "cap", "expected"),
        [
            pytest.param([], 1, None, None, id="empty_list_returns_none"),
            pytest.param([(1, "m"), (3, "m")], 1, None, (1, "m"), id="exact_match_wins"),
            pytest.param([(3, "m"), (5, "m")], 1, None, (3, "m"), id="picks_nearest_above_requested_not_the_furthest"),
            pytest.param([(1, "m")], 5, None, None, id="nothing_at_or_above_requested_returns_none"),
            pytest.param([(5, "m")], 1, 3, None, id="candidate_beyond_cap_returns_none"),
            pytest.param([(3, "m")], 1, 3, (3, "m"), id="candidate_at_cap_is_accepted"),
            pytest.param(
                [(3, "copy"), (3, "tiering")],
                1,
                None,
                (3, "copy"),
                id="tie_between_two_middles_picks_first_in_sort_order",
            ),
        ],
    )
    def test_nearest_live_generation(
        self,
        candidates: list[tuple[int, str]],
        requested: int,
        cap: int | None,
        expected: tuple[int, str] | None,
    ) -> None:
        assert _nearest_live_generation(candidates, requested=requested, cap=cap) == expected


class TestForwardResolution:
    async def test_resolves_forward_when_requested_generation_is_gone(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_picks_the_nearest_not_the_furthest_live_generation(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3, 5], latest_complete_version=5)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            resolved = await stream._resolve_forward(1)
            assert resolved is not None and resolved[0] == 3

    async def test_a_non_complete_generation_is_skipped_for_a_later_complete_one(self, tmp_path: Path) -> None:
        """Only a Complete ``file_map`` row makes a generation live
        (FORMAT-SPEC.md: db/file_map); stream_version=3's row is Written."""
        _build_saas_repo_multi_gen(
            tmp_path,
            live_stream_versions=[3, 5],
            latest_complete_version=5,
            non_complete_stream_versions=frozenset({3}),
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            resolved = await stream._resolve_forward(1)
            assert resolved is not None and resolved[0] == 5

    @pytest.mark.parametrize(
        ("live_stream_versions", "latest_complete_version", "non_complete_stream_versions"),
        [
            pytest.param([3], 3, frozenset({3}), id="only_a_non_complete_generation_within_cap_is_a_genuine_gap"),
            # A live file_map row at 5 beyond latest_complete_version=2 is
            # crash garbage from a rolled-back write, not a substitute.
            pytest.param([5], 2, frozenset(), id="does_not_cross_latest_complete_version"),
        ],
    )
    async def test_no_usable_forward_generation_raises_not_found(
        self,
        tmp_path: Path,
        live_stream_versions: list[int],
        latest_complete_version: int,
        non_complete_stream_versions: frozenset[int],
    ) -> None:
        _build_saas_repo_multi_gen(
            tmp_path,
            live_stream_versions=live_stream_versions,
            latest_complete_version=latest_complete_version,
            non_complete_stream_versions=non_complete_stream_versions,
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no live saas_obj for stream_version>"):
                await stream.open_saas_obj(_version())

    async def test_genuine_gap_raises_not_found_with_originally_requested_path(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[], latest_complete_version=5)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no live saas_obj for stream_version>") as excinfo:
                await stream.open_saas_obj(_version())
        assert f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj" in str(excinfo.value)

    async def test_generation_scan_runs_once_per_middle_across_repeated_calls(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            calls = 0
            real_fn = r.file_map_paths_with_prefix

            async def _counted(prefix: str, *, status: int | None = None) -> list[str]:
                nonlocal calls
                calls += 1
                return await real_fn(prefix, status=status)

            monkeypatch.setattr(r, "file_map_paths_with_prefix", _counted)
            for _ in range(3):
                f = await stream.open_saas_obj(_version())
                assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ
            # 2 candidate middles (connection_id, numeric ccid), scanned
            # once each across all three calls -- not once per call.
            assert calls == 2

    async def test_only_the_middle_with_a_live_hit_is_used(self, tmp_path: Path) -> None:
        """Forward resolution merges candidate middles: the
        Copy/connectionId middle has no live generation, the
        Tiering/connectionConfigId one does."""
        write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        write_file_map(tmp_path / "db" / "file_map", [(f"{_STREAM_UUID}/{_CCID}/3/saas_obj", _STREAM_ID, 5, 64, 2, 2)])
        write_bare_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        write_saas_snapshot_db(
            stream_db_dir / "saas_snapshot",
            snapshots=[(1, "snap-uuid-1", 3, 1)],
            distribution=[(0, len(_SAAS_OBJ), 1, 3)],
        )
        write_saas_version_db(
            stream_db_dir / "saas_version", versions=[(1, 3, 1, 0)], target_type="M365", latest_complete_version=3
        )
        write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=5, num_chunks=2)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
            [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ


class TestResolveSaasObj:
    """``resolve_saas_obj`` reports which generation it read, for
    ``verify_reachable``'s label enrichment."""

    async def test_requested_equals_resolved_when_no_substitution_happened(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            resolved = await stream.resolve_saas_obj(_version())
            assert (resolved.requested_stream_version, resolved.stream_version) == (1, 1)
            assert await resolved.dedup_file.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_requested_and_resolved_differ_after_a_substitution(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            resolved = await stream.resolve_saas_obj(_version())
            assert (resolved.requested_stream_version, resolved.stream_version) == (1, 3)

    async def test_saas_stream_cache_mirrors_it(self, repo: DedupRepo) -> None:
        async with SaasStreamCache(repo) as cache:
            resolved = await cache.resolve_saas_obj(_version())
            assert (resolved.requested_stream_version, resolved.stream_version) == (1, 1)


class TestResourceManagement:
    async def test_close_is_idempotent_and_releases_connections(self, repo: DedupRepo) -> None:
        stream = SaasStream(repo, _CCID, _STREAM_UUID)
        await stream.stream_version_for(_version())  # opens both the snapshot and version files
        assert len(stream._sources) == 2
        await stream.close()
        assert len(stream._sources) == 0
        await stream.close()  # idempotent

    async def test_close_attempts_every_source_when_one_fails(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stream = SaasStream(repo, _CCID, _STREAM_UUID)
        await stream.stream_version_for(_version())
        sources, _ = await stream._sources.settle_all()
        first, second = sources.values()
        closed: list[object] = []
        first_close, second_close = first.close, second.close

        async def failing_close() -> None:
            await first_close()
            raise RuntimeError("synthetic close failure")

        async def spying_close() -> None:
            closed.append(second)
            await second_close()

        monkeypatch.setattr(first, "close", failing_close)
        monkeypatch.setattr(second, "close", spying_close)

        with pytest.raises(ExceptionGroup, match="closing a SaaS stream's sources failed"):
            await stream.close()
        assert closed == [second]
        assert len(stream._sources) == 0

    async def test_context_manager_closes_on_exit(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            await stream.stream_version_for(_version())
        assert len(stream._sources) == 0


class TestBoundedEviction:
    """SaasStreamCache's bounded LRU: eviction closes the evicted stream,
    not just drops the reference. Uses a faked ``SaasStream``."""

    @staticmethod
    def _install_fake_stream(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, str]]:
        closed: list[tuple[int, str]] = []

        @faithful_to(SaasStream)
        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def close(self) -> None:
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        return closed

    async def test_maxsize_evicts_the_least_recently_used_and_closes_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=2)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        assert closed == []
        await cache._stream_for((3, "c"))  # over the cap -- evicts (1, "a")
        assert closed == [(1, "a")]
        assert set(cache._streams) == {(2, "b"), (3, "c")}

    async def test_resolving_an_existing_key_refreshes_its_recency(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=2)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        await cache._stream_for((1, "a"))  # touches (1, "a") again
        await cache._stream_for((3, "c"))  # must evict (2, "b"), not (1, "a")
        assert closed == [(2, "b")]

    async def test_an_evicted_key_is_reconstructed_as_a_genuinely_new_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        first = await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))  # evicts (1, "a")
        assert closed == [(1, "a")]
        second = await cache._stream_for((1, "a"))  # rebuilt, not silently missing
        assert second is not first

    async def test_close_closes_every_remaining_stream(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=8)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        await cache.close()
        assert sorted(closed) == [(1, "a"), (2, "b")]

    async def test_cache_stats_count_hits_misses_and_evictions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=2)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))  # miss
        await cache._stream_for((1, "a"))  # hit
        await cache._stream_for((2, "b"))  # miss
        await cache._stream_for((3, "c"))  # miss, evicts (1, "a")

        stats = cache.cache_stats()["saas_streams"]

        assert (stats.size, stats.maxsize, stats.hits, stats.misses, stats.evictions) == (2, 2, 1, 3, 1)

    async def test_the_default_bound_is_the_cache_limit(self, repo: DedupRepo) -> None:
        assert SaasStreamCache(repo).cache_stats()["saas_streams"].maxsize == DEFAULT_LIMITS.saas_streams

    async def test_eviction_close_failure_does_not_propagate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Closing an evicted stream is best-effort: its failure doesn't
        reach the caller getting its newly-built stream."""

        @faithful_to(SaasStream)
        class _RaisingCloseStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                pass

            async def close(self) -> None:
                raise OSError("simulated close failure")

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _RaisingCloseStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        new_stream = await cache._stream_for((2, "b"))  # evicts (1, "a"); its close() raises
        assert new_stream is not None
        assert set(cache._streams) == {(2, "b")}

    async def test_maxsize_below_one_is_rejected_at_construction(self) -> None:
        # A cache of size 0 would evict (and close) every stream the
        # instant it's inserted, then hand the now-closed instance back
        # to the caller as if live -- rejected up front instead.
        with pytest.raises(ValueError, match="maxsize"):
            SaasStreamCache(repo=None, maxsize=0)  # type: ignore[arg-type]


class TestStreamReuseAcrossVersions:
    """Two catalog Versions sharing one ``(connection_config_id,
    saas_stream_uuid)`` pair resolve through one shared ``SaasStream``."""

    async def test_two_versions_of_the_same_stream_share_one_saas_stream_instance(self, repo: DedupRepo) -> None:
        cache = SaasStreamCache(repo)
        try:
            await cache.open_saas_obj(_version())
            first = cache._streams[(int(_CCID), str(_STREAM_UUID))]
            other_version = dataclasses.replace(_version(), version_uid=VersionUid("some-other-vuid"))
            await cache.open_saas_obj(other_version)
            second = cache._streams[(int(_CCID), str(_STREAM_UUID))]
            assert first is second
        finally:
            await cache.close()


class TestConcurrentConnectionResolution:
    """Concurrent callers resolving the same not-yet-opened connection on
    one ``SaasStream`` share one open (``AsyncKeyedCache``'s in-flight
    sharing)."""

    async def test_concurrent_table_opens_resolve_to_one_table_and_one_connection(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            columns = [Column("snapshot_id")]
            first, second = await asyncio.gather(
                stream._open_table("saas_snapshot", "snapshot_info", columns),
                stream._open_table("saas_snapshot", "snapshot_info", columns),
            )
            assert first is second
            assert len(stream._sources) == 1


class TestEvictionDefersForAnInUseStream:
    """``SaasStreamCache.open_saas_obj`` marks its stream in-use for the
    call's duration, so a concurrent call for a different key can't
    evict-and-close it out from under the first caller."""

    async def test_a_stream_still_mid_open_is_not_evicted_by_a_different_keys_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[tuple[int, str]] = []
        entered = asyncio.Event()
        release = asyncio.Event()

        @faithful_to(SaasStream)
        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def resolve_saas_obj(self, version: Version) -> object:
                if self._key == (1, "a"):
                    entered.set()
                    await release.wait()
                return SimpleNamespace(dedup_file=object())

            async def close(self) -> None:
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        version_a = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(1), saas_stream_uuid=StreamUuid("a")
        )
        version_b = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(2), saas_stream_uuid=StreamUuid("b")
        )

        task = asyncio.create_task(cache.open_saas_obj(version_a))
        try:
            await entered.wait()
            # Cache is at capacity (1) and (1, "a") is still mid-open --
            # a concurrent call for a different key must not evict it.
            await cache.open_saas_obj(version_b)
            assert closed == []
            assert (1, "a") in cache._streams
        finally:
            release.set()
            await task

    async def test_a_just_inserted_key_is_protected_even_while_its_own_eviction_is_still_closing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_in_use`` is set before ``_stream_for`` runs, so a concurrent
        eviction can't evict a still-mid-insert key."""
        closed: list[tuple[int, str]] = []
        c_close_started = asyncio.Event()
        release_c_close = asyncio.Event()

        @faithful_to(SaasStream)
        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def resolve_saas_obj(self, version: Version) -> object:
                return SimpleNamespace(dedup_file=object())

            async def close(self) -> None:
                if self._key == (3, "c"):
                    c_close_started.set()
                    await release_c_close.wait()
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        # Seed the cache with (3, "c") as the sole, already-resident entry.
        await cache._stream_for((3, "c"))

        version_a = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(1), saas_stream_uuid=StreamUuid("a")
        )
        version_b = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(2), saas_stream_uuid=StreamUuid("b")
        )

        # Task A: opens A -- cache is at capacity (1), so this evicts
        # (3, "c"), whose close() blocks with (1, "a") already inserted
        # but not yet returned.
        task_a = asyncio.create_task(cache.open_saas_obj(version_a))
        try:
            await c_close_started.wait()
            assert (1, "a") in cache._streams  # inserted before the blocking close

            # Task B: opens a different key concurrently while task A is
            # still blocked in eviction. (1, "a") must already be
            # protected, or this eviction loop would treat it as free.
            await cache.open_saas_obj(version_b)
            assert (1, "a") in cache._streams  # never evicted out from under task A
            assert (3, "c") not in closed  # task A's own close() hasn't finished yet
        finally:
            release_c_close.set()
            await task_a
        assert closed == [(3, "c")]


class TestCloseRacingAnInFlightOpen:
    """``SaasStreamCache.close()`` clears ``_in_use`` unconditionally, so a
    still in-flight ``open_saas_obj()`` must tolerate its own key being gone
    by the time its ``finally`` block runs."""

    async def test_close_running_concurrently_does_not_mask_the_callers_own_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        completed: list[str] = []

        @faithful_to(SaasStream)
        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                pass

            async def resolve_saas_obj(self, version: Version) -> object:
                entered.set()
                await release.wait()
                completed.append("real result")
                return SimpleNamespace(dedup_file=object())

            async def close(self) -> None:
                pass

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]

        task = asyncio.create_task(cache.open_saas_obj(_version()))
        await entered.wait()
        await cache.close()  # races the in-flight call above, clearing _in_use
        release.set()

        await task  # must not raise KeyError -- would mask this real completion
        assert completed == ["real result"]


class TestOneSourcePerFile:
    async def test_version_info_and_stream_info_share_one_connection(self, repo: DedupRepo) -> None:
        """Both tables live in ``saas_version``, so the file is opened once,
        not once per table."""
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            await stream.open_saas_obj(_version())  # needs snapshot_info, version_info and stream_info

            assert len(stream._sources) == 2  # saas_snapshot + saas_version, not three

    async def test_a_healthy_bare_file_costs_no_directory_listing(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        listed: list[str] = []
        real_listdir = LocalFsStore.listdir

        async def spying_listdir(self: LocalFsStore, path: str) -> list[Entry]:
            listed.append(path)
            return await real_listdir(self, path)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(LocalFsStore, "listdir", spying_listdir)
                await stream.stream_version_for(_version())

        assert not any(path.endswith("/db") and "saas" in path for path in listed)

    async def test_the_stream_lists_its_db_directory_through_the_repositorys_shared_cache(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        suffixed = stream_db_dir / "saas_snapshot.3"
        suffixed.write_bytes((stream_db_dir / "saas_snapshot").read_bytes())
        (stream_db_dir / "saas_snapshot").write_bytes(b"")  # forces the fallback listing
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            await stream.stream_version_for(_version())

            assert any(name.endswith("/db") and "saas" in name for name in r.dir_cache._listings)

    async def test_when_no_candidate_file_exists_it_raises_not_found(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        (stream_db_dir / "saas_snapshot").unlink()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="no file matches logical name"):
                await stream.stream_version_for(_version())

    async def test_when_every_candidate_is_unreadable_the_error_says_so(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        (stream_db_dir / "saas_snapshot").write_bytes(b"")
        (stream_db_dir / "saas_snapshot.3").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="unreadable"):
                await stream.stream_version_for(_version())

    async def test_an_unreadable_bare_file_is_tried_once_per_table_not_reopened(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        (stream_db_dir / "saas_version.7").write_bytes((stream_db_dir / "saas_version").read_bytes())
        (stream_db_dir / "saas_version").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            await stream.open_saas_obj(_version())  # version_info and stream_info both fall back

            # snapshot (bare) + version (empty bare, then generation) = three files, each opened once.
            assert len(stream._sources) == 3


class TestBareFileProbedOncePerFile:
    async def test_version_info_and_stream_info_ask_whether_the_bare_file_exists_once(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        probed: list[str] = []
        real_exists = LocalFsStore.exists

        async def spying_exists(self: LocalFsStore, path: str) -> bool:
            probed.append(path)
            return await real_exists(self, path)

        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(LocalFsStore, "exists", spying_exists)
                await stream.open_saas_obj(_version())  # snapshot_info, version_info and stream_info

        bare_probes = [path for path in probed if path.endswith(("/saas_snapshot", "/saas_version"))]
        assert sorted(bare_probes) == sorted(set(bare_probes))  # each bare file asked about exactly once
        assert len(bare_probes) == 2


class TestUnopenableBareFile:
    async def test_a_bare_file_that_fails_to_open_falls_back_to_the_generation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        (stream_db_dir / "saas_snapshot.3").write_bytes((stream_db_dir / "saas_snapshot").read_bytes())
        real_open = SqliteSource.from_raw_store.__func__  # type: ignore[attr-defined]

        async def failing_for_bare(cls: type[SqliteSource], store: object, path: str) -> SqliteSource:
            if path.endswith("/saas_snapshot"):
                raise DataCorruptError("simulated: not a database", ref=path)
            return cast(SqliteSource, await real_open(cls, store, path))

        monkeypatch.setattr(SqliteSource, "from_raw_store", classmethod(failing_for_bare))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1


class TestUnreadableOnlyCandidate:
    async def test_an_unreadable_bare_file_with_no_generation_reports_why(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        (stream_db_dir / "saas_snapshot").write_bytes(b"")  # the only candidate, and it has no tables
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError, match="unreadable"):
                await stream.stream_version_for(_version())
