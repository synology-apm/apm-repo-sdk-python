"""Conformance test: every ``ObjectStore`` wrapper forwards ``close()`` to
the store it wraps, so ``Session.close()`` releases a wrapped
``S3Store``/``AzureStore``/``SmbStore``'s clients. A new wrapper belongs in
``_WRAPPERS``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from support.recording.fixture_store import AliasedStore, RecordingStore, ReplayStore
from support.store_fakes import CloseCountingStore
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.recording import TracingStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore

#: Every ``ObjectStore`` wrapper the SDK (``TracingStore``) and the suite's
#: recording tooling define.
_WRAPPERS: list[tuple[str, Callable[[ObjectStore], ObjectStore]]] = [
    ("AliasedStore", lambda backing: AliasedStore(backing, {})),
    ("RecordingStore", lambda backing: RecordingStore(backing)),
    ("TracingStore", lambda backing: TracingStore(backing, lambda _event: None)),
]


@pytest.mark.parametrize(("name", "make_wrapper"), _WRAPPERS, ids=[name for name, _ in _WRAPPERS])
async def test_wrapper_forwards_close_to_its_backing_store(
    name: str, make_wrapper: Callable[[ObjectStore], ObjectStore]
) -> None:
    backing = CloseCountingStore()
    await make_wrapper(backing).close()
    assert backing.close_count == 1


@pytest.mark.parametrize("store_class", [LocalFsStore, S3Store, AzureStore, SmbStore, ReplayStore])
def test_every_built_in_store_implements_the_whole_protocol(store_class: type) -> None:
    assert all(callable(getattr(store_class, name, None)) for name in ("read", "size", "exists", "listdir", "close"))


async def test_local_and_replay_stores_close_as_a_no_op(tmp_path: Path) -> None:
    (tmp_path / "a.bin").write_bytes(b"abc")
    local = LocalFsStore(tmp_path)
    await local.close()
    assert await local.read("a.bin") == b"abc"  # still serves after close
    replay = ReplayStore(
        '{"format": 2, "reads": {}, "sizes": {"a.bin": 3}, "exists": {}, "listdirs": {}, "missing": {}}'
    )
    await replay.close()
    await replay.close()
    assert await replay.size("a.bin") == 3
