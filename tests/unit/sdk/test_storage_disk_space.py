"""Unit tests for ``storage.disk_space``, with ``shutil.disk_usage``
replaced by a synthetic filesystem of a chosen size."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

import synology_apm_repo.sdk.storage.disk_space as disk_space_module
from synology_apm_repo.sdk.errors import ResourceLimitExceededError
from synology_apm_repo.sdk.storage.disk_space import reserve_disk_space
from unit.sdk.storage_fakes import fake_disk_usage

GiB = 1 << 30


def _in_flight() -> int:
    return sum(disk_space_module._in_flight.values())


@pytest.fixture(autouse=True)
def _no_hold_outlives_its_test() -> Iterator[None]:
    """Fails the test that leaks a hold, rather than every later test that
    the process-wide counter would then mislead."""
    yield
    leaked = dict(disk_space_module._in_flight)
    disk_space_module._in_flight.clear()
    assert leaked == {}


class TestReserve:
    @pytest.mark.parametrize(
        ("total", "reserve"),
        [
            pytest.param(512 << 20, (512 << 20) // 10, id="512MiB_tmpfs_10pct_floor"),
            pytest.param(8 * GiB, 8 * GiB // 10, id="8GiB_10pct_floor"),
            pytest.param(10 * GiB, GiB, id="10GiB_10pct_floor_meets_1GiB"),
            pytest.param(16 * GiB, GiB, id="16GiB_1GiB_floor"),
            pytest.param(20 * GiB, GiB, id="20GiB_5pct_meets_floor"),
            pytest.param(64 * GiB, 64 * GiB // 20, id="64GiB_5pct"),
            pytest.param(200 * GiB, 10 * GiB, id="200GiB_5pct_meets_ceiling"),
            pytest.param(4 << 40, 10 * GiB, id="4TiB_ceiling"),
        ],
    )
    def test_five_percent_between_a_size_scaled_floor_and_ten_gib(self, total: int, reserve: int) -> None:
        assert disk_space_module._free_space_reserve(total) == reserve


class TestBoundary:
    def test_free_exactly_needed_plus_reserve_passes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        needed = 3 * GiB
        fake_disk_usage(monkeypatch, total=100 * GiB, free=needed + 5 * GiB)
        with reserve_disk_space(tmp_path, needed) as reservation:
            assert reservation.needed == needed
            assert reservation.reserve == 5 * GiB

    def test_one_byte_less_fails_naming_needed_free_reserve_and_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        needed = 3 * GiB
        fake_disk_usage(monkeypatch, total=100 * GiB, free=needed + 5 * GiB - 1)
        with (
            pytest.raises(ResourceLimitExceededError, match="not enough free space under") as excinfo,
            reserve_disk_space(tmp_path, needed),
        ):
            pass  # pragma: no cover - the check raises first
        message = excinfo.value.safe_message
        assert str(tmp_path) in message
        assert "needs 3.0 GiB plus 5.0 GiB kept free" in message
        assert "only 8.0 GiB is free (1 B short)" in message
        assert _in_flight() == 0

    def test_a_small_disk_keeps_the_floor_free(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # 16 GiB total: 5% is 819 MiB, so the 1 GiB floor applies.
        fake_disk_usage(monkeypatch, total=16 * GiB, free=GiB + 100)
        with reserve_disk_space(tmp_path, 100):
            pass
        with (
            pytest.raises(ResourceLimitExceededError, match=r"plus 1\.0 GiB kept free"),
            reserve_disk_space(tmp_path, 101),
        ):
            pass  # pragma: no cover - the check raises first

    def test_a_small_tmpfs_takes_a_small_copy_keeping_a_tenth_of_it_free(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        total = 512 << 20
        reserve = total // 10
        fake_disk_usage(monkeypatch, total=total, free=reserve + 4096)
        with reserve_disk_space(tmp_path, 4096):
            pass
        with (
            pytest.raises(ResourceLimitExceededError, match=r"plus 51\.2 MiB kept free"),
            reserve_disk_space(tmp_path, 4097),
        ):
            pass  # pragma: no cover - the check raises first

    def test_a_large_disk_keeps_only_the_ceiling_free(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # 4 TiB total: 5% would be 205 GiB, so the 10 GiB ceiling applies.
        fake_disk_usage(monkeypatch, total=4 << 40, free=10 * GiB + 100)
        with reserve_disk_space(tmp_path, 100):
            pass
        with (
            pytest.raises(ResourceLimitExceededError, match=r"plus 10\.0 GiB kept free"),
            reserve_disk_space(tmp_path, 101),
        ):
            pass  # pragma: no cover - the check raises first


class TestConcurrentReservations:
    def test_two_sixty_percent_writes_do_not_both_pass_against_the_same_free_space(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # free never drops here: the first write hasn't landed yet.
        fake_disk_usage(monkeypatch, total=100 * GiB, free=50 * GiB)
        needed = 27 * GiB  # 60% of the 45 GiB above the reserve
        with reserve_disk_space(tmp_path, needed):
            assert _in_flight() == needed
            with (
                pytest.raises(ResourceLimitExceededError, match=r"27\.0 GiB of it held by other"),
                reserve_disk_space(tmp_path, needed),
            ):
                pass  # pragma: no cover - the check raises first
        assert _in_flight() == 0
        with reserve_disk_space(tmp_path, needed):
            pass

    def test_directories_on_one_filesystem_share_the_holds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=50 * GiB)
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        with (
            reserve_disk_space(tmp_path / "a", 27 * GiB),
            pytest.raises(ResourceLimitExceededError, match="held by other temporary copies"),
            reserve_disk_space(tmp_path / "b", 27 * GiB),
        ):
            pass  # pragma: no cover - the check raises first

    def test_threads_reserving_at_once_never_overcommit(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Room above the 1 GiB reserve for exactly three of the ten.
        fake_disk_usage(monkeypatch, total=16 * GiB, free=GiB + 300)
        start = threading.Barrier(10)
        release = threading.Event()
        decided = threading.Semaphore(0)
        granted: list[int] = []
        refused: list[int] = []

        def attempt(i: int) -> None:
            start.wait()
            try:
                with reserve_disk_space(tmp_path, 100):
                    granted.append(i)
                    decided.release()
                    release.wait()
            except ResourceLimitExceededError:
                refused.append(i)
                decided.release()

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        try:
            for _ in threads:
                assert decided.acquire(timeout=10)
            assert (len(granted), len(refused)) == (3, 7)
            assert _in_flight() == 300
        finally:  # a failed assertion must not leave the granted threads holding
            release.set()
            for thread in threads:
                thread.join()
        assert _in_flight() == 0


class TestRelease:
    @pytest.mark.parametrize("error", [OSError("disk full"), asyncio.CancelledError(), KeyboardInterrupt()])
    def test_a_failed_or_interrupted_write_releases_its_hold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: BaseException
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=50 * GiB)
        with pytest.raises(type(error)), reserve_disk_space(tmp_path, GiB):
            assert _in_flight() == GiB
            raise error
        assert _in_flight() == 0


class TestUnknownSize:
    def test_holds_what_it_has_written_up_to_the_space_above_the_reserve_and_other_holds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=50 * GiB)
        room = 50 * GiB - 20 * GiB - 5 * GiB
        with reserve_disk_space(tmp_path, 20 * GiB), reserve_disk_space(tmp_path, None) as reservation:
            assert _in_flight() == 20 * GiB
            reservation.account(room - 1)
            reservation.account(1)
            assert _in_flight() == 20 * GiB + room
            with pytest.raises(ResourceLimitExceededError, match=r"needs more than 25\.0 GiB"):
                reservation.account(1)
        assert _in_flight() == 0

    def test_concurrent_unknown_size_writes_share_the_room_instead_of_each_getting_all_of_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=15 * GiB)  # 10 GiB above the 5 GiB reserve
        with reserve_disk_space(tmp_path, None) as first, reserve_disk_space(tmp_path, None) as second:
            first.account(6 * GiB)
            with pytest.raises(ResourceLimitExceededError, match=r"needs more than 4\.0 GiB"):
                second.account(6 * GiB)
            second.account(4 * GiB)
            assert _in_flight() == 10 * GiB
        assert _in_flight() == 0

    def test_a_sized_write_sees_an_unknown_size_writes_bytes_so_far(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=15 * GiB)
        with reserve_disk_space(tmp_path, None) as unknown:
            unknown.account(8 * GiB)
            with pytest.raises(ResourceLimitExceededError, match="held by other temporary copies"):
                reserve_disk_space(tmp_path, 3 * GiB).__enter__()

    def test_a_known_size_write_may_not_outgrow_its_declared_size(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=50 * GiB)
        with reserve_disk_space(tmp_path, 10) as reservation:
            reservation.account(10)
            with pytest.raises(ResourceLimitExceededError, match="needs more than 10 B"):
                reservation.account(1)

    def test_fails_when_nothing_is_free_above_the_reserve(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_disk_usage(monkeypatch, total=100 * GiB, free=5 * GiB)
        with (
            pytest.raises(ResourceLimitExceededError, match="needs an undeclared size"),
            reserve_disk_space(tmp_path, None),
        ):
            pass  # pragma: no cover - the check raises first
