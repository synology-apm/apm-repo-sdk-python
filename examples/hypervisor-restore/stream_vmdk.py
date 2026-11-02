"""A disk exported as a streamOptimized VMDK, written front to back into a ``VmdkOutput`` (an HTTP upload, say)
as the export runs, so no copy of the disk is staged locally.

A streamOptimized VMDK holds only the disk's non-zero 64 KiB grains, each deflated behind a grain marker; its
grain tables and directory come after the last grain, so it can be produced in one pass.
"""

from __future__ import annotations

import array
import asyncio
import concurrent.futures
import http.client
import os
import secrets
import ssl
import struct
import urllib.parse
import zlib
from collections.abc import Callable, Mapping
from typing import Protocol

from apm_restore_common import RestoreError, SourceDisk, export_disk
from synology_apm_repo.sdk.export import BufferedExportSink, FlushableSegment

SECTOR = 512
GRAIN_SECTORS = 128
GRAIN = GRAIN_SECTORS * SECTOR
GTES_PER_GT = 512
_GT_SECTORS = GTES_PER_GT * 4 // SECTOR
_OVERHEAD_SECTORS = GRAIN_SECTORS
"""Sectors before the first grain: the header and the embedded descriptor."""
_MAGIC = 0x564D444B  # "KDMV"
_FLAGS = 0x30001  # valid newline test, compressed grains, markers
_GD_AT_END = 0xFFFFFFFFFFFFFFFF
_COMPRESSION_DEFLATE = 1
_HEADER = struct.Struct("<IIIQQQQIQQQB4sH433x")
_MARKER_EOS, _MARKER_GT, _MARKER_GD, _MARKER_FOOTER = 0, 1, 2, 3
_ZERO_GRAIN = bytes(GRAIN)
_LEVEL = 1
"""zlib level: deflate dominates the export's CPU, and level 1 is several times faster than the default for a
few percent more bytes."""
_SEGMENT_SIZE = 32 << 20
_COMPRESSORS = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 4, thread_name_prefix="vmdk-zlib")


class VmdkOutput(Protocol):
    """Where the VMDK's bytes go, in order."""

    async def write(self, data: bytes) -> None: ...

    async def finish(self) -> None:
        """Every byte was written: make the result final."""
        ...

    async def abort(self) -> None:
        """The export failed: drop what was written. May be called after a failed ``finish``."""
        ...


def header(capacity_sectors: int, descriptor_sectors: int, gd_offset: int) -> bytes:
    """The sparse extent header (also the footer, with the real ``gd_offset``)."""
    return _HEADER.pack(
        _MAGIC,
        3,
        _FLAGS,
        capacity_sectors,
        GRAIN_SECTORS,
        1,
        descriptor_sectors,
        GTES_PER_GT,
        0,
        gd_offset,
        _OVERHEAD_SECTORS,
        0,
        b"\n \r\n",
        _COMPRESSION_DEFLATE,
    )


def descriptor(capacity_sectors: int, *, cid: int, file_name: str) -> bytes:
    """The embedded text descriptor of a monolithic streamOptimized disk."""
    cylinders = min(capacity_sectors // (255 * 63), 65535)
    return (
        "# Disk DescriptorFile\nversion=1\n"
        f'CID={cid:08x}\nparentCID=ffffffff\ncreateType="streamOptimized"\n\n'
        f'# Extent description\nRW {capacity_sectors} SPARSE "{file_name}"\n\n'
        "# The Disk Data Base\n#DDB\n\n"
        'ddb.virtualHWVersion = "4"\n'
        f'ddb.geometry.cylinders = "{cylinders}"\nddb.geometry.heads = "255"\nddb.geometry.sectors = "63"\n'
        'ddb.adapterType = "lsilogic"\n'
    ).encode()


def metadata_marker(sectors: int, kind: int) -> bytes:
    return struct.pack("<QII", sectors, 0, kind).ljust(SECTOR, b"\0")


def grain_record(lba: int, compressed: bytes) -> bytes:
    """A grain marker and its deflated data, padded to whole sectors."""
    record = struct.pack("<QI", lba, len(compressed)) + compressed
    return record + bytes(-len(record) % SECTOR)


def _compress(grain: bytes) -> bytes | None:
    """The deflated grain, or ``None`` for an all-zero one (left out of the VMDK)."""
    return None if grain == _ZERO_GRAIN else zlib.compress(grain, _LEVEL)


class StreamOptimizedVmdkSink(BufferedExportSink):
    """``ExportSink`` writing one disk as a streamOptimized VMDK into ``output``.

    ``cid`` is the descriptor's content id: give two runs over the same disk the same one and they write the same
    bytes. The capacity is the disk's size rounded up to a whole sector; its last grain is padded with zeros."""

    def __init__(self, output: VmdkOutput, *, cid: int, file_name: str = "disk.vmdk") -> None:
        super().__init__(_SEGMENT_SIZE)
        self._output = output
        self._cid = cid
        self._file_name = file_name
        self._capacity = 0
        self._descriptor_sectors = 0
        self._sector = 0
        self._tables: dict[int, array.array[int]] = {}

    async def _emit(self, data: bytes) -> None:
        await self._output.write(data)
        self._sector += len(data) // SECTOR

    async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
        self._capacity = -(-logical_size // SECTOR)
        text = descriptor(self._capacity, cid=self._cid, file_name=self._file_name)
        self._descriptor_sectors = -(-len(text) // SECTOR)
        if 1 + self._descriptor_sectors > _OVERHEAD_SECTORS:
            raise ValueError("the VMDK descriptor does not fit before the first grain")
        head = header(self._capacity, self._descriptor_sectors, _GD_AT_END) + text
        await self._emit(head.ljust(_OVERHEAD_SECTORS * SECTOR, b"\0"))

    async def flush_segment(self, segment: FlushableSegment) -> None:
        # Segments and their blocks are whole grains apart from the export's last block, whose short last grain
        # is padded with zeros.
        offset = segment.start
        async for block in segment.blocks():
            if offset % GRAIN:
                raise ValueError(f"block at {offset} is not grain-aligned")
            grains = [bytes(block[i : i + GRAIN]).ljust(GRAIN, b"\0") for i in range(0, len(block), GRAIN)]
            await self._write_grains(offset // SECTOR, grains)
            offset += len(block)

    async def _write_grains(self, first_lba: int, grains: list[bytes]) -> None:
        loop = asyncio.get_running_loop()
        compressed = await asyncio.gather(*(loop.run_in_executor(_COMPRESSORS, _compress, g) for g in grains))
        for index, data in enumerate(compressed):
            if data is None:
                continue
            lba = first_lba + index * GRAIN_SECTORS
            grain = lba // GRAIN_SECTORS
            table = self._tables.setdefault(grain // GTES_PER_GT, array.array("I", bytes(GTES_PER_GT * 4)))
            table[grain % GTES_PER_GT] = self._sector
            await self._emit(grain_record(lba, data))

    async def finalize_destination(self) -> None:
        tables = -(-self._capacity // (GRAIN_SECTORS * GTES_PER_GT))
        directory = array.array("I", bytes(tables * 4))
        for index in sorted(self._tables):
            await self._emit(metadata_marker(_GT_SECTORS, _MARKER_GT))
            directory[index] = self._sector
            await self._emit(self._tables[index].tobytes())
        gd_sectors = -(-len(directory) * 4 // SECTOR)
        await self._emit(metadata_marker(gd_sectors, _MARKER_GD))
        gd_offset = self._sector
        await self._emit(directory.tobytes().ljust(gd_sectors * SECTOR, b"\0"))
        await self._emit(metadata_marker(1, _MARKER_FOOTER))
        await self._emit(header(self._capacity, self._descriptor_sectors, gd_offset))
        await self._emit(metadata_marker(0, _MARKER_EOS))
        await self._output.finish()

    async def discard_destination(self) -> bool:
        await self._output.abort()
        return False


# --------------------------------------------------------------------------- #
# Outputs
# --------------------------------------------------------------------------- #


class CountingOutput:
    """``VmdkOutput`` that only counts the bytes: the first pass of a two-pass upload."""

    def __init__(self) -> None:
        self.size = 0

    async def write(self, data: bytes) -> None:
        self.size += len(data)

    async def finish(self) -> None:
        pass

    async def abort(self) -> None:
        pass


class HttpUploadOutput:
    """``VmdkOutput`` that streams the VMDK as the body of one HTTP request.

    With ``content_length`` (the VMDK's size) the body is sent with a ``Content-Length`` and must come out exactly
    that size; without, it is sent chunked. ``prefix`` and ``suffix`` go before and after the VMDK (a multipart
    form's framing) and count toward the length. ``finish`` raises ``RestoreError`` unless the server answers
    2xx; ``response`` then holds its body."""

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: Mapping[str, str] | None = None,
        content_length: int | None = None,
        prefix: bytes = b"",
        suffix: bytes = b"",
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._url = urllib.parse.urlsplit(url)
        self._method = method
        self._headers = dict(headers or {})
        self._content_length = content_length
        self._prefix = prefix
        self._suffix = suffix
        self._ssl_context = ssl_context
        self._connection: http.client.HTTPConnection | None = None
        self._sent = 0
        self.response = b""

    def _open(self) -> http.client.HTTPConnection:
        host, port = self._url.hostname or "", self._url.port
        if self._url.scheme == "https":
            connection: http.client.HTTPConnection = http.client.HTTPSConnection(host, port, context=self._ssl_context)
        else:
            connection = http.client.HTTPConnection(host, port)
        path = self._url.path + (f"?{self._url.query}" if self._url.query else "")
        connection.putrequest(self._method, path, skip_accept_encoding=True)
        for name, value in self._headers.items():
            connection.putheader(name, value)
        if self._content_length is None:
            connection.putheader("Transfer-Encoding", "chunked")
        else:
            connection.putheader("Content-Length", str(len(self._prefix) + self._content_length + len(self._suffix)))
        connection.endheaders()
        return connection

    def _send(self, data: bytes) -> None:
        if self._connection is None:
            self._connection = self._open()
            if self._prefix:
                self._send_raw(self._prefix)
        self._send_raw(data)

    def _send_raw(self, data: bytes) -> None:
        assert self._connection is not None
        if not data:
            return  # an empty chunk would end a chunked body
        try:
            if self._content_length is None:
                self._connection.send(b"%x\r\n%b\r\n" % (len(data), data))
            else:
                self._connection.send(data)
        except OSError as exc:
            raise RestoreError(f"upload to {self._url.hostname} failed: {self._refusal() or exc}") from exc

    def _refusal(self) -> str | None:
        """What the server said when it cut the upload short, if it said anything."""
        assert self._connection is not None
        try:
            reply = self._connection.getresponse()
            return f"HTTP {reply.status} {reply.reason}: {reply.read(4096).decode(errors='replace').strip()}"
        except (OSError, http.client.HTTPException):
            return None

    async def write(self, data: bytes) -> None:
        await asyncio.to_thread(self._send, data)
        self._sent += len(data)

    def _finish(self) -> None:
        if self._content_length is not None and self._sent != self._content_length:
            raise RestoreError(f"the VMDK came out {self._sent} B, not the {self._content_length} B announced")
        self._send(b"")  # opens the request even for an empty body
        assert self._connection is not None
        if self._suffix:
            self._send_raw(self._suffix)
        if self._content_length is None:
            self._connection.send(b"0\r\n\r\n")
        reply = self._connection.getresponse()
        body = reply.read()
        self._connection.close()
        self._connection = None
        if not 200 <= reply.status < 300:
            text = body[:4096].decode(errors="replace").strip()
            raise RestoreError(f"upload to {self._url.hostname} failed: HTTP {reply.status} {reply.reason}: {text}")
        self.response = body

    async def finish(self) -> None:
        await asyncio.to_thread(self._finish)

    async def abort(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            await asyncio.to_thread(connection.close)


async def export_vmdk(
    disk: SourceDisk,
    label: str,
    open_upload: Callable[..., HttpUploadOutput],
    *,
    two_pass: bool,
    file_name: str = "disk.vmdk",
) -> HttpUploadOutput:
    """Exports ``disk`` as a streamOptimized VMDK into the upload ``open_upload(content_length=...)`` returns, and
    returns that upload once finished. With ``two_pass`` a first export into a ``CountingOutput`` gives the
    length; without, it is ``None`` (chunked)."""
    cid = secrets.randbits(32)
    length = None
    if two_pass:
        counter = CountingOutput()
        sink = StreamOptimizedVmdkSink(counter, cid=cid, file_name=file_name)
        await export_disk(disk, sink, f"{label} (sizing)")
        length = counter.size
    upload = open_upload(content_length=length)
    sink = StreamOptimizedVmdkSink(upload, cid=cid, file_name=file_name)
    await export_disk(disk, sink, label)
    return upload
