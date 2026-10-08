"""Storage access abstraction.

Owns the "logical name -> physical file" rules (sequence-id suffixes,
S3/Azure generation selection, repository layout detection), so every layer
above (Dedup and up) deals only in logical names.
"""

from __future__ import annotations

from .azure import AzureStore, list_containers
from .base import Entry, ObjectStore, list_names
from .dircache import DirCache, split_seq_suffix
from .generations import SUPPLEMENTAL_TABLES, latest_transaction_id, resolve_generation
from .layout import (
    RepoKind,
    RepoLayout,
    RepositoryLayout,
    catalog_repo_layouts,
    detect_repository_layout,
    iter_repository_layouts,
    key_probe_layout,
)
from .local import LocalFsStore
from .recording import TraceEvent, TracingStore
from .s3 import S3Store, list_buckets
from .seqid import resolve_seq_file
from .smb import SmbStore
from .sqlite import open_sqlite
from .sqlite_source import Envelope, SqliteSource, peel
from .table import Column, Table

__all__ = [
    "SUPPLEMENTAL_TABLES",
    "AzureStore",
    "Column",
    "DirCache",
    "Entry",
    "Envelope",
    "LocalFsStore",
    "ObjectStore",
    "RepoKind",
    "RepoLayout",
    "RepositoryLayout",
    "S3Store",
    "SmbStore",
    "SqliteSource",
    "Table",
    "TraceEvent",
    "TracingStore",
    "catalog_repo_layouts",
    "detect_repository_layout",
    "iter_repository_layouts",
    "key_probe_layout",
    "latest_transaction_id",
    "list_buckets",
    "list_containers",
    "list_names",
    "open_sqlite",
    "peel",
    "resolve_generation",
    "resolve_seq_file",
    "split_seq_suffix",
]
