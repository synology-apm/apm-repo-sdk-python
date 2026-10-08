"""An in-memory stand-in for aiobotocore's S3 client, shared by
``test_storage_s3.py`` and ``test_storage_object_store_contract.py``, and
a synthetic ``shutil.disk_usage`` for the ``storage.disk_space`` tests and
the temp-file writers it guards."""

from __future__ import annotations

import shutil
from collections.abc import AsyncIterator
from typing import Any, NamedTuple

import pytest
from botocore.exceptions import ClientError

from support.fakes import unchecked_fake


class _DiskUsage(NamedTuple):
    total: int
    used: int
    free: int


def fake_disk_usage(monkeypatch: pytest.MonkeyPatch, *, total: int, free: int) -> None:
    """Make every ``shutil.disk_usage`` call report a filesystem of
    ``total`` bytes with ``free`` of them free."""
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: _DiskUsage(total, total - free, free))


def s3_not_found(operation: str, code: str = "NoSuchKey") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "not found"}}, operation)


@unchecked_fake("aiobotocore's S3 client surface")
class FakeStreamingBody:
    """Stands in for ``get_object``'s ``Body``: an async-``read``-once
    stream with a synchronous ``close()``, which ``S3Store.read`` calls on
    a cancelled read so the connection doesn't return to the pool with
    unread bytes on the wire."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.closed = False

    async def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.closed = True


@unchecked_fake("aiobotocore's S3 client surface")
class FakeS3Paginator:
    """Stands in for ``client.get_paginator("list_objects_v2")``: as in
    aiobotocore, ``.paginate(...)`` is a plain call returning something
    ``async for``'d over, driving the ``ContinuationToken`` loop."""

    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    async def paginate(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        token = None
        while True:
            page = await self._client.list_objects_v2(ContinuationToken=token, **kwargs)
            yield page
            if not page.get("IsTruncated"):
                return
            token = page["NextContinuationToken"]


@unchecked_fake("aiobotocore's S3 client surface")
class FakeS3Client:
    """A minimal in-memory S3: ``Range``-header parsing, the
    ``NoSuchKey``/``404``/``InvalidRange`` error codes ``S3Store`` maps,
    and ``list_objects_v2`` paging at AWS's 1000-key page limit."""

    _PAGE_SIZE = 1000

    def __init__(self) -> None:
        self._buckets: set[str] = set()
        self._objects: dict[tuple[str, str], bytes] = {}
        self.closed = False

    async def create_bucket(self, *, Bucket: str) -> None:
        self._buckets.add(Bucket)

    async def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.seed(Bucket, Key, Body)

    def seed(self, bucket: str, key: str, data: bytes) -> None:
        """``put_object`` without an event loop, for a sync fixture."""
        self._objects[(bucket, key)] = data

    async def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, Any]:
        content = self._objects.get((Bucket, Key))
        if content is None:
            raise s3_not_found("GetObject", "NoSuchKey")
        start, end = _parse_range(Range, len(content))
        if start >= len(content) and len(content) > 0:
            raise s3_not_found("GetObject", "InvalidRange")
        return {"Body": FakeStreamingBody(content[start:end])}

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        content = self._objects.get((Bucket, Key))
        if content is None:
            raise s3_not_found("HeadObject", "404")
        return {"ContentLength": len(content)}

    async def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str = "",
        MaxKeys: int = _PAGE_SIZE,
        Delimiter: str | None = None,
        ContinuationToken: str | None = None,
    ) -> dict[str, Any]:
        keys = sorted(key for bucket, key in self._objects if bucket == Bucket and key.startswith(Prefix))
        start_index = keys.index(ContinuationToken) + 1 if ContinuationToken is not None else 0
        page_size = min(MaxKeys, self._PAGE_SIZE)
        page_keys = keys[start_index : start_index + page_size]

        contents: list[str] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()
        for key in page_keys:
            rest = key[len(Prefix) :]
            if Delimiter and Delimiter in rest:
                prefix = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
                    common_prefixes.append(prefix)
            else:
                contents.append(key)

        is_truncated = start_index + len(page_keys) < len(keys)
        result: dict[str, Any] = {
            "Contents": [{"Key": key, "Size": len(self._objects[(Bucket, key)])} for key in contents],
            "CommonPrefixes": [{"Prefix": prefix} for prefix in common_prefixes],
            "IsTruncated": is_truncated,
        }
        if is_truncated:
            result["NextContinuationToken"] = page_keys[-1]
        return result

    def get_paginator(self, operation_name: str) -> FakeS3Paginator:
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self)

    async def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": [{"Name": name} for name in sorted(self._buckets)]}

    async def close(self) -> None:
        self.closed = True


def _parse_range(range_header: str, size: int) -> tuple[int, int]:
    """``"bytes=X-Y"``/``"bytes=X-"`` -> ``(start, end)``, ``end`` clamped
    to ``size`` — S3's short read at EOF."""
    start_s, _, end_s = range_header.removeprefix("bytes=").partition("-")
    start = int(start_s)
    end = int(end_s) + 1 if end_s else size
    return start, min(end, size)
