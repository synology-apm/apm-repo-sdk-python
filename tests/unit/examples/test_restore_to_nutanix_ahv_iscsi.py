"""Tests for the iSCSI half of examples/restore_to_nutanix_ahv.py: the CDB builders, ``LibiscsiWriter`` and
``BlockSink``.

``LibiscsiWriter`` runs against ``_FakeIscsi`` through the module's ``_load_iscsi`` seam.
"""

from __future__ import annotations

import asyncio
import pickle
import threading
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.fakes import faithful_to, unchecked_fake
from synology_apm_repo.sdk.dedup.export_sink import WorkerTarget
from synology_apm_repo.sdk.export import ExportResult, run_export
from synology_apm_repo.sdk.units.base import ContentSource

_MIB = 1 << 20


# -- CDB builders ----------------------------------------------------------


def test_read_capacity16_cdb_layout(ex: ModuleType) -> None:
    cdb = ex.read_capacity16_cdb()
    assert len(cdb) == 16
    assert cdb[0] == 0x9E and cdb[1] == 0x10
    assert int.from_bytes(cdb[10:14], "big") == 32  # allocation length


def test_write16_cdb_layout(ex: ModuleType) -> None:
    cdb = ex.write16_cdb(0x1234, 8)
    assert len(cdb) == 16
    assert cdb[0] == 0x8A and cdb[1] == 0x00
    assert int.from_bytes(cdb[2:10], "big") == 0x1234
    assert int.from_bytes(cdb[10:14], "big") == 8


def test_write16_cdb_fua_goes_in_byte_one(ex: ModuleType) -> None:
    assert ex.write16_cdb(0, 1, fua=0x08)[1] == 0x08


def test_sync_cache16_cdb_covers_whole_lun(ex: ModuleType) -> None:
    cdb = ex.sync_cache16_cdb()
    assert len(cdb) == 16
    assert cdb[0] == 0x91
    assert cdb[2:14] == bytes(12)  # LBA 0, 0 blocks


# -- LibiscsiWriter over a fake iscsi module -----------------------------------


@unchecked_fake("the iscsi module")
class _FakeIscsi:
    """A stand-in for the ``iscsi`` module: records every command and answers READ CAPACITY(16)."""

    def __init__(self, *, block_size: int = 512, block_count: int = 4096, fail_opcodes: tuple[int, ...] = ()) -> None:
        self.block_size = block_size
        self.block_count = block_count
        self.fail_opcodes = fail_opcodes
        self.contexts: list[Any] = []
        self.commands: list[SimpleNamespace] = []
        outer = self

        class Task:
            def __init__(self, cdb: bytes, direction: object, xferlen: int) -> None:
                self.cdb, self.direction, self.xferlen = cdb, direction, xferlen
                self.status = 0

        class URL:
            def __init__(self, ctx: Any, url: str) -> None:
                rest = url.removeprefix("iscsi://")
                self.portal, remainder = rest.split("/", 1)
                self.target, lun = remainder.rsplit("/", 1)
                self.lun = int(lun)

        class Context:
            def __init__(self, initiator: str) -> None:
                self.initiator = initiator
                self.calls: list[tuple[str, Any]] = []
                self.disconnected = 0
                outer.contexts.append(self)

            def set_targetname(self, name: str) -> None:
                self.calls.append(("set_targetname", name))

            def set_session_type(self, kind: object) -> None:
                self.calls.append(("set_session_type", kind))

            def set_header_digest(self, kind: object) -> None:
                self.calls.append(("set_header_digest", kind))

            def connect(self, portal: str, lun: int) -> None:
                self.calls.append(("connect", (portal, lun)))

            def disconnect(self) -> None:
                self.disconnected += 1

            def command(self, lun: int, task: Task, dataout: bytearray, datain: bytearray) -> None:
                opcode = task.cdb[0]
                outer.commands.append(
                    SimpleNamespace(lun=lun, cdb=task.cdb, direction=task.direction, dataout=bytes(dataout))
                )
                if opcode == 0x9E:
                    datain[0:8] = (outer.block_count - 1).to_bytes(8, "big")
                    datain[8:12] = outer.block_size.to_bytes(4, "big")
                task.status = 2 if opcode in outer.fail_opcodes else 0

        self.Task, self.URL, self.Context = Task, URL, Context
        self.iscsi_session_type = SimpleNamespace(ISCSI_SESSION_NORMAL="normal")
        self.iscsi_header_digest = SimpleNamespace(ISCSI_HEADER_DIGEST_NONE_CRC32C="digest")
        self.scsi_xfer_dir = SimpleNamespace(SCSI_XFER_NONE="none", SCSI_XFER_READ="read", SCSI_XFER_WRITE="write")

    def writes(self) -> list[SimpleNamespace]:
        return [c for c in self.commands if c.cdb[0] == 0x8A]


@pytest.fixture
def fake_iscsi(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> _FakeIscsi:
    fake = _FakeIscsi()
    monkeypatch.setattr(ex, "_load_iscsi", lambda: fake)
    return fake


_URL = "iscsi://10.0.0.5:3260/iqn.2010-06.com.nutanix:vg-1/2"


def test_writer_connects_then_reads_geometry(ex: ModuleType, fake_iscsi: _FakeIscsi) -> None:
    writer = ex.LibiscsiWriter(_URL, "iqn.host:me")

    (ctx,) = fake_iscsi.contexts
    assert ctx.initiator == "iqn.host:me"
    assert [name for name, _ in ctx.calls] == ["set_targetname", "set_session_type", "set_header_digest", "connect"]
    assert ctx.calls[0][1] == "iqn.2010-06.com.nutanix:vg-1"
    assert ctx.calls[-1][1] == ("10.0.0.5:3260", 2)
    assert writer.geometry() == (512, 4096)
    (capacity,) = fake_iscsi.commands
    assert capacity.cdb[0] == 0x9E and capacity.lun == 2 and capacity.direction == "read"


def test_writer_splits_a_write_at_the_max_transfer(ex: ModuleType, fake_iscsi: _FakeIscsi) -> None:
    fake_iscsi.block_count = 16384
    writer = ex.LibiscsiWriter(_URL, "iqn")

    writer.write(1024, b"\xaa" * (3 * _MIB))

    writes = fake_iscsi.writes()
    assert [int.from_bytes(w.cdb[2:10], "big") for w in writes] == [2, 2050, 4098]
    assert [int.from_bytes(w.cdb[10:14], "big") for w in writes] == [2048, 2048, 2048]
    assert all(w.direction == "write" and w.cdb[1] == 0x00 for w in writes)
    assert b"".join(w.dataout for w in writes) == b"\xaa" * (3 * _MIB)


def test_writer_rounds_the_chunk_down_to_whole_blocks(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeIscsi(block_size=1536, block_count=4096)
    monkeypatch.setattr(ex, "_load_iscsi", lambda: fake)
    writer = ex.LibiscsiWriter(_URL, "iqn")

    writer.write(0, bytes(1536 * 1000))

    # 1 MiB is not a multiple of 1536, so each command carries 682 whole blocks.
    assert [int.from_bytes(w.cdb[10:14], "big") for w in fake.writes()] == [682, 318]


@pytest.mark.parametrize(("offset", "length"), [(1, 512), (512, 100), (0, 513)])
def test_writer_rejects_unaligned_writes_without_sending(
    ex: ModuleType, fake_iscsi: _FakeIscsi, offset: int, length: int
) -> None:
    writer = ex.LibiscsiWriter(_URL, "iqn")

    with pytest.raises(ValueError, match="unaligned write"):
        writer.write(offset, bytes(length))

    assert fake_iscsi.writes() == []


def test_writer_raises_when_the_target_rejects_a_command(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeIscsi(fail_opcodes=(0x8A,))
    monkeypatch.setattr(ex, "_load_iscsi", lambda: fake)
    writer = ex.LibiscsiWriter(_URL, "iqn")

    with pytest.raises(OSError, match="0x8a failed with status 2"):
        writer.write(0, bytes(512))


def test_writer_write_at_accepts_a_memoryview(ex: ModuleType, fake_iscsi: _FakeIscsi) -> None:
    writer = ex.LibiscsiWriter(_URL, "iqn")

    writer.write_at(512, memoryview(b"\x07" * 512))

    (write,) = fake_iscsi.writes()
    assert int.from_bytes(write.cdb[2:10], "big") == 1 and write.dataout == b"\x07" * 512


def test_writer_close_syncs_the_cache_then_disconnects(ex: ModuleType, fake_iscsi: _FakeIscsi) -> None:
    writer = ex.LibiscsiWriter(_URL, "iqn")

    writer.close()

    assert fake_iscsi.commands[-1].cdb[0] == 0x91
    assert fake_iscsi.contexts[0].disconnected == 1


def test_writer_close_disconnects_even_when_the_sync_fails(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeIscsi(fail_opcodes=(0x91,))
    monkeypatch.setattr(ex, "_load_iscsi", lambda: fake)
    writer = ex.LibiscsiWriter(_URL, "iqn")

    with pytest.raises(OSError, match="0x91"):
        writer.close()

    assert fake.contexts[0].disconnected == 1


def test_descriptor_pickles_and_opens_a_writer(ex: ModuleType, fake_iscsi: _FakeIscsi) -> None:
    descriptor = ex.LibiscsiDescriptor(_URL, "iqn.host:me")

    assert pickle.loads(pickle.dumps(descriptor)) == descriptor
    writer = descriptor.open_writer()
    assert isinstance(writer, ex.LibiscsiWriter)
    assert fake_iscsi.contexts[0].initiator == "iqn.host:me"


# -- BlockSink ---------------------------------------------------------------


@unchecked_fake("examples/restore_to_nutanix_ahv.py's BlockWriter, loaded at test time")
class _FakeWriter:
    """A ``BlockWriter`` over memory that records its writes."""

    def __init__(self, block_size: int = 512, block_count: int = 8192) -> None:
        self._geometry = (block_size, block_count)
        self.writes: list[tuple[int, bytes]] = []
        self.closed = 0

    def geometry(self) -> tuple[int, int]:
        return self._geometry

    def write(self, offset: int, data: bytes) -> None:
        self.writes.append((offset, data))

    def close(self) -> None:
        self.closed += 1


async def _open_sink(ex: ModuleType, writer: _FakeWriter, size: int = 2 * _MIB, descriptor: Any = None) -> Any:
    sink = ex.BlockSink(lambda: writer, descriptor)
    await sink.open(size, sparse=True)
    return sink


def test_sink_declares_sparse_and_not_preallocated(ex: ModuleType) -> None:
    sink = ex.BlockSink(_FakeWriter)

    assert sink.caps.supports_sparse is True
    assert sink.preallocated is False


async def test_sink_open_rejects_a_lun_that_is_too_small_and_closes_it(ex: ModuleType) -> None:
    writer = _FakeWriter(block_size=512, block_count=100)
    sink = ex.BlockSink(lambda: writer)

    with pytest.raises(ValueError, match="cannot hold"):
        await sink.open(10 * _MIB, sparse=True)

    assert writer.closed == 1


@pytest.mark.parametrize("block_size", [512, 4096])
async def test_sink_open_accepts_the_two_block_sizes_real_luns_have(ex: ModuleType, block_size: int) -> None:
    writer = _FakeWriter(block_size=block_size, block_count=2 * _MIB // block_size)
    sink = await _open_sink(ex, writer)

    await sink.write_at(0, memoryview(b"abc"))

    assert writer.closed == 0  # it stays open for the export
    assert writer.writes == [(0, b"abc")]


@pytest.mark.parametrize("block_size", [520, 1536, 8192])
async def test_sink_open_rejects_any_other_block_size_and_closes_the_lun(ex: ModuleType, block_size: int) -> None:
    writer = _FakeWriter(block_size=block_size, block_count=100000)
    sink = ex.BlockSink(lambda: writer)

    with pytest.raises(ValueError, match="block size is"):
        await sink.open(2 * _MIB, sparse=True)

    assert writer.closed == 1


async def test_sink_open_rejects_a_size_that_is_not_a_block_multiple(ex: ModuleType) -> None:
    writer = _FakeWriter(block_size=4096, block_count=1024)
    sink = ex.BlockSink(lambda: writer)

    with pytest.raises(ValueError, match="multiple of the block size"):
        await sink.open(4096 + 512, sparse=True)

    assert writer.closed == 1


async def test_sink_open_accepts_a_lun_whose_capacity_is_exactly_the_expected_one(ex: ModuleType) -> None:
    writer = _FakeWriter(block_size=512, block_count=4096)  # 2 MiB
    sink = ex.BlockSink(lambda: writer, capacity=2 * _MIB)

    await sink.open(2 * _MIB, sparse=True)

    assert writer.closed == 0  # it stays open for the export


@pytest.mark.parametrize("block_count", [4095, 4097, 8192])
async def test_sink_open_rejects_a_lun_whose_capacity_differs_from_its_volume_group_disk(
    ex: ModuleType, block_count: int
) -> None:
    writer = _FakeWriter(block_size=512, block_count=block_count)
    sink = ex.BlockSink(lambda: writer, capacity=2 * _MIB)  # a bigger LUN is wrong too: it is another disk's

    with pytest.raises(ValueError, match="not the LUN of that disk"):
        await sink.open(2 * _MIB, sparse=True)

    assert writer.closed == 1


async def test_sink_without_an_expected_capacity_accepts_a_larger_lun(ex: ModuleType) -> None:
    writer = _FakeWriter(block_size=512, block_count=8192)  # 4 MiB
    sink = ex.BlockSink(lambda: writer)

    await sink.open(2 * _MIB, sparse=True)

    assert writer.closed == 0  # it stays open for the export


async def test_sink_rejects_writes_before_open(ex: ModuleType) -> None:
    sink = ex.BlockSink(_FakeWriter)

    with pytest.raises(RuntimeError, match="not open"):
        await sink.write_at(0, b"x")


async def test_sink_write_at_passes_bytes_through(ex: ModuleType) -> None:
    writer = _FakeWriter()
    sink = await _open_sink(ex, writer)

    await sink.write_at(4096, memoryview(b"abc"))

    assert writer.writes == [(4096, b"abc")]


async def test_sink_write_zero_writes_real_zeros_in_chunks(ex: ModuleType) -> None:
    writer = _FakeWriter(block_count=16384)
    sink = await _open_sink(ex, writer, size=4 * _MIB)

    await sink.write_zero(_MIB, 2 * _MIB + 4096)

    assert [(offset, len(data)) for offset, data in writer.writes] == [
        (_MIB, _MIB),
        (2 * _MIB, _MIB),
        (3 * _MIB, 4096),
    ]
    assert all(data == bytes(len(data)) for _, data in writer.writes)


async def test_sink_rejects_write_zero_before_open(ex: ModuleType) -> None:
    sink = ex.BlockSink(_FakeWriter)

    with pytest.raises(RuntimeError, match="not open"):
        await sink.write_zero(0, 512)


async def test_sink_commit_closes_the_writer_and_blocks_further_writes(ex: ModuleType) -> None:
    writer = _FakeWriter()
    sink = await _open_sink(ex, writer)

    await sink.commit()

    assert writer.closed == 1
    with pytest.raises(RuntimeError, match="not open"):
        await sink.write_at(0, b"x")


async def test_sink_abort_before_open_is_a_no_op(ex: ModuleType) -> None:
    outcome = await ex.BlockSink(_FakeWriter).abort()

    assert (outcome.kept, outcome.ever_written) == (False, False)


async def test_sink_abort_reports_what_was_written_and_is_idempotent(ex: ModuleType) -> None:
    writer = _FakeWriter()
    sink = await _open_sink(ex, writer)
    await sink.write_at(0, b"x")

    first = await sink.abort()
    second = await sink.abort()

    assert (first.kept, first.ever_written) == (True, True)
    assert (second.kept, second.ever_written) == (True, True)
    assert writer.closed == 1


async def test_sink_abort_without_writes_keeps_nothing(ex: ModuleType) -> None:
    sink = await _open_sink(ex, _FakeWriter())

    outcome = await sink.abort()

    assert (outcome.kept, outcome.ever_written) == (False, False)


def test_sink_without_a_descriptor_takes_no_worker_writes(ex: ModuleType) -> None:
    assert ex.BlockSink(_FakeWriter).worker_target() is None


def test_sink_with_a_descriptor_takes_worker_writes(ex: ModuleType) -> None:
    descriptor = ex.LibiscsiDescriptor(_URL, "iqn")
    sink = ex.BlockSink(_FakeWriter, descriptor)

    target = sink.worker_target()

    assert isinstance(target, WorkerTarget)
    assert target.descriptor is descriptor and target.base_offset == 0


async def test_sink_notes_worker_writes_so_abort_reports_them(ex: ModuleType) -> None:
    sink = await _open_sink(ex, _FakeWriter(), descriptor=ex.LibiscsiDescriptor(_URL, "iqn"))

    sink.note_worker_write()
    outcome = await sink.abort()

    assert (outcome.kept, outcome.ever_written) == (True, True)


@faithful_to(ContentSource)
class _FakeContent:
    """A ``ContentSource`` that exports two runs through whatever sink it is given."""

    size = 2 * _MIB

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def stream(self, block: int = _MIB) -> AsyncIterator[tuple[int, bytes]]:
        return
        yield  # unreachable: makes this an (empty) async generator

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        await sink.write_at(0, b"A" * 4096)
        await sink.write_at(_MIB, b"B" * 4096)
        return ExportResult(bytes_written=8192, logical_size=self.size, holes=0, zeros=0)


async def test_run_export_drives_the_sink_lifecycle(ex: ModuleType) -> None:
    writer = _FakeWriter()
    sink = ex.BlockSink(lambda: writer)

    result = await run_export(_FakeContent(), sink)

    assert result.bytes_written == 8192
    assert writer.writes == [(0, b"A" * 4096), (_MIB, b"B" * 4096)]
    assert writer.closed == 1  # commit closed it


async def test_run_export_aborts_the_sink_when_the_export_fails(ex: ModuleType) -> None:
    class Failing(_FakeContent):
        async def planned_bytes(self, start: int, end: int) -> int:
            return end - start

        async def export_range(
            self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
        ) -> ExportResult:
            await sink.write_at(0, b"A" * 4096)
            raise RuntimeError("source went away")

    writer = _FakeWriter()
    sink = ex.BlockSink(lambda: writer)

    with pytest.raises(RuntimeError, match="source went away"):
        await run_export(Failing(), sink)

    assert writer.closed == 1
    outcome = await sink.abort()
    assert outcome.ever_written is True


class _ContendedLock:
    """A ``threading.Lock`` that sets ``contended`` when an acquire finds it
    already held, so a test can tell another thread is waiting on it."""

    def __init__(self, contended: threading.Event) -> None:
        self._lock = threading.Lock()
        self._contended = contended

    def __enter__(self) -> None:
        if not self._lock.acquire(blocking=False):
            self._contended.set()
            self._lock.acquire()

    def __exit__(self, *exc: object) -> None:
        self._lock.release()


async def test_sink_serializes_concurrent_writes_to_its_writer(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """The first write to arrive holds the writer until a second one is
    waiting for it, so writes that weren't serialized would overlap inside
    ``write``."""
    active = 0
    peak = 0
    held = False
    # Set by a second write either waiting on the sink's lock or (unserialized) entering write().
    contended = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(ex, "threading", SimpleNamespace(Lock=lambda: _ContendedLock(contended)))

    class Slow(_FakeWriter):
        def write(self, offset: int, data: bytes) -> None:
            nonlocal active, peak, held
            active += 1
            peak = max(peak, active)
            try:
                if active > 1:
                    contended.set()
                    release.set()
                if not held:
                    held = True
                    release.wait()
                super().write(offset, data)
            finally:
                active -= 1

    writer = Slow()
    sink = await _open_sink(ex, writer)

    writes = asyncio.gather(*(sink.write_at(i * 512, b"x" * 512) for i in range(20)))
    assert await asyncio.to_thread(contended.wait, 10.0), "no second write ever reached the writer"
    release.set()
    await writes

    assert len(writer.writes) == 20
    assert peak == 1
