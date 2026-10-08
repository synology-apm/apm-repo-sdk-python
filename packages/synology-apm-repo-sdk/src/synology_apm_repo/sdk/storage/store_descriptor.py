"""Picklable stand-ins for a live ``ObjectStore``, so a worker process from
``concurrency.new_process_pool()`` can rebuild an equivalent store: an open
``S3Store``/``AzureStore``/``SmbStore`` holds a client bound to the parent's
event loop and can't cross a process boundary. Each backend describes
itself (``descriptor()``) and each descriptor rebuilds its store
(``build()``); this module is the dispatch over both.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .azure import AzureStoreDescriptor
from .base import ObjectStore
from .local import LocalFsStoreDescriptor
from .s3 import S3StoreDescriptor
from .smb import SmbStoreDescriptor

StoreDescriptor = LocalFsStoreDescriptor | S3StoreDescriptor | AzureStoreDescriptor | SmbStoreDescriptor


@runtime_checkable
class _Describable(Protocol):
    def descriptor(self) -> StoreDescriptor | None: ...


def describe_store(store: ObjectStore) -> StoreDescriptor | None:
    """``store`` as a picklable recipe for rebuilding an equivalent store, or
    ``None`` (never an error) when that is impossible; callers then stay
    single-process. ``None`` is returned for:

    - an ``InstrumentedStore`` wrapper such as ``TracingStore``, or any other
      ``ObjectStore`` without ``descriptor()``: a wrapper observes every
      call, and a worker rebuilt from the bare backend would bypass it.
    - an ``S3Store``/``AzureStore`` built from an injected ``client=``.
    """
    return store.descriptor() if isinstance(store, _Describable) else None


def rebuild_store(descriptor: StoreDescriptor) -> ObjectStore:
    """The worker-side counterpart to ``describe_store()``. Every backend
    constructor is synchronous and does no network I/O, so this is safe
    inside a ``ProcessPoolExecutor`` ``initializer=``.
    """
    return descriptor.build()
