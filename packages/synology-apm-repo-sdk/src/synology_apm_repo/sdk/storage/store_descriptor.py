"""Picklable stand-ins for a live ``ObjectStore``, so a worker process
spawned by ``concurrency.new_process_pool()`` can rebuild an equivalent
store instead of receiving one directly — an already-open ``S3Store``/
``AzureStore``/``SmbStore`` holds a live client bound to the parent's own
event loop and can't cross a process boundary at all.

``describe_store()`` is the one place this module reaches into another
``storage/`` class's private fields (``_bucket``/``_client_kwargs``/...) —
an accepted, deliberately narrow exception to those classes' own
encapsulation, the same way ``dedup/chunk_walk.py`` is the one module
allowed to reach into ``DedupFile._extents()`` for its own narrow,
single purpose rather than that field becoming generally public: every
field read here is read *only* to describe how to rebuild an equivalent
store, never touched for any other purpose,
and the reason a free function does this instead of each store class
exposing a public ``describe()`` method is that ``ObjectStore`` itself
stays a narrow four-method Protocol (``read``/``size``/``exists``/
``listdir``) — a test fake, or a future backend, should never be required
to also implement multiprocess-support machinery just to satisfy that
Protocol.
"""

from __future__ import annotations

import dataclasses

from .azure import AzureStore
from .base import ObjectStore
from .local import LocalFsStore
from .s3 import S3Store
from .smb import SmbStore


@dataclasses.dataclass(frozen=True)
class LocalFsStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``LocalFsStore``."""

    root: str


@dataclasses.dataclass(frozen=True)
class S3StoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``S3Store``."""

    bucket: str
    client_kwargs: dict[str, object]


@dataclasses.dataclass(frozen=True)
class AzureStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``AzureStore``."""

    container: str
    client_kwargs: dict[str, object]


@dataclasses.dataclass(frozen=True)
class SmbStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``SmbStore``."""

    share: str
    server: str
    port: int
    username: str | None
    password: str | None


StoreDescriptor = LocalFsStoreDescriptor | S3StoreDescriptor | AzureStoreDescriptor | SmbStoreDescriptor


def describe_store(store: ObjectStore) -> StoreDescriptor | None:
    """``store`` reduced to a picklable recipe for rebuilding an
    equivalent one elsewhere, or ``None`` when that's not possible —
    always a graceful "can't", never a raised error, since every caller
    treats ``None`` as "fall back to today's single-process path" rather
    than a failure:

    - A ``TracingStore``/``RecordingStore`` wrapper (``storage/recording.py``)
      or any other/unrecognized ``ObjectStore`` implementation — this
      module only knows the four real backends by name, deliberately, not
      an oversight.
    - An ``S3Store``/``AzureStore`` built from an already-live, injected
      ``client=`` (tests, or a caller-supplied pre-entered client) — there
      is no picklable recipe for a client that already exists.
    """
    if isinstance(store, LocalFsStore):
        return LocalFsStoreDescriptor(root=str(store.root))
    if isinstance(store, S3Store):
        if not store._owns_client:  # noqa: SLF001 - read only to build a picklable rebuild recipe
            return None
        return S3StoreDescriptor(bucket=store._bucket, client_kwargs=dict(store._client_kwargs))  # noqa: SLF001
    if isinstance(store, AzureStore):
        if not store._owns_client:  # noqa: SLF001
            return None
        return AzureStoreDescriptor(
            container=store._container.container_name,  # noqa: SLF001
            client_kwargs=dict(store._client_kwargs),  # noqa: SLF001
        )
    if isinstance(store, SmbStore):
        return SmbStoreDescriptor(
            share=store._share,  # noqa: SLF001
            server=store._server,  # noqa: SLF001
            port=store._port,  # noqa: SLF001
            username=store._username,  # noqa: SLF001
            password=store._password,  # noqa: SLF001
        )
    return None


def rebuild_store(descriptor: StoreDescriptor) -> ObjectStore:
    """The worker-process-side counterpart to ``describe_store()`` — every
    real backend's constructor is synchronous and does no I/O (confirmed
    for all four: ``LocalFsStore.__init__``'s ``os.open``/``Path.is_dir()``,
    ``S3Store``/``SmbStore``'s constructors likewise do no I/O, and
    ``AzureStore``'s ``BlobServiceClient(...)`` call is plain
    client-object configuration), so this is always safe to call from a
    ``ProcessPoolExecutor``'s synchronous ``initializer=``, not just from
    inside a worker's own event loop.
    """
    if isinstance(descriptor, LocalFsStoreDescriptor):
        return LocalFsStore(descriptor.root)
    if isinstance(descriptor, S3StoreDescriptor):
        return S3Store(descriptor.bucket, **descriptor.client_kwargs)
    if isinstance(descriptor, AzureStoreDescriptor):
        return AzureStore(descriptor.container, **descriptor.client_kwargs)
    return SmbStore(
        descriptor.share,
        server=descriptor.server,
        port=descriptor.port,
        username=descriptor.username,
        password=descriptor.password,
    )
