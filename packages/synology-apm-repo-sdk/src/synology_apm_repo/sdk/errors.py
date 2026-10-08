"""Exception hierarchy for synology-apm-repo-sdk.

Every ``ApmRepoError`` carries two optional pieces of context:

- ``ref``: the node/file the error refers to, as a plain string (a
  store-relative path, a ``file_map`` path, ``str(node_ref)``, ...).
- ``spec``: the ``FORMAT-SPEC.md`` subsection explaining why this is an
  error.

The hierarchy covers repository data, backend and worker failures;
argument and programmer errors raise stdlib ``ValueError``/``IndexError``.
Names avoid shadowing builtins such as ``KeyError``.
"""

from __future__ import annotations


class ApmRepoError(Exception):
    """Base of all errors raised by synology-apm-repo-sdk."""

    def __init__(
        self,
        message: str,
        *,
        ref: str | None = None,
        spec: str | None = None,
    ) -> None:
        self.ref = ref
        self.spec = spec
        self._message = message
        parts = [message]
        if ref is not None:
            parts.append(f"[ref={ref}]")
        if spec is not None:
            parts.append(f"[spec={spec}]")
        super().__init__(" ".join(parts))

    @property
    def safe_message(self) -> str:
        """The message alone, without the ``[ref=...]``/``[spec=...]`` tags
        ``str(exc)`` appends — for a presentation layer's non-verbose
        rendering. ``str(exc)`` keeps the full detail."""
        return self._message


class FormatError(ApmRepoError):
    """The bytes at ``ref`` do not match the on-disk format they were
    expected to have (magic mismatch, CRC mismatch, unsupported header
    version, truncated file, ...)."""


class DataCorruptError(FormatError):
    """An integrity check failed: a magic, CRC32 or other self-consistency
    check did not match, so the bytes did not survive intact."""


class UnsupportedVersionError(FormatError):
    """The on-disk format version is one this SDK does not read: a newer
    major version, or an obsolete layout."""


class ChunkCompactedError(FormatError):
    """SizeStore reports ``CompressType.COMPACTED`` for this chunk — the
    data was reclaimed by the write-side compactor and can no longer be
    read (FORMAT-SPEC.md: SizeStore)."""


class KeyMaterialError(ApmRepoError):
    """The key string itself (``<userKeyID>@<base64(userKey)>``) is
    malformed, or does not match this repository's wrapped VaultKey."""


class KeyRequiredError(KeyMaterialError):
    """The repository, or the data being read, is encrypted but no key was
    supplied."""


class KeyMismatchError(KeyMaterialError):
    """The supplied key does not unwrap this repository's VaultKey, or was
    already rejected by ``Repository.set_key()`` (FORMAT-SPEC.md: Key hierarchy)."""


class NotFoundError(ApmRepoError):
    """The referenced path / node / version / object does not exist in this
    repository — as opposed to existing but being unreadable."""


class ContentUnavailableError(ApmRepoError):
    """This node's metadata was found but its content can't be read: the
    guest held only a placeholder at backup time (e.g. a cloud-only
    OneDrive/iCloud file), or the bytes need key material this SDK lacks
    (e.g. NTFS EFS). Says nothing about damage, and is deliberately not a
    ``NotFoundError``, so code skipping absent nodes doesn't swallow it."""


class NotRestorableError(ApmRepoError):
    """A resolved node/item that has no restorable content (a folder, a
    group, a node missing the metadata its content comes from)."""


class PermissionDeniedError(ApmRepoError):
    """The OS or backend denied reading or listing an existing path;
    deliberately not a ``NotFoundError``, so code skipping absent paths
    doesn't swallow it."""


class StorageBackendError(ApmRepoError):
    """The storage backend failed to serve a request — a network or
    transport failure, a timeout, a service-side error, or an OS-level I/O
    error. Not a statement about the repository's data, so code that turns
    unreadable data into a finding or a placeholder re-raises it instead."""


class UnsupportedDataFormatError(ApmRepoError):
    """The content exists but this version of the SDK deliberately refuses
    to interpret it (e.g. a CBT merge chain) rather than risk emitting
    silently-wrong bytes."""


class ResourceLimitExceededError(ApmRepoError):
    """A requested size exceeds a safety ceiling (a guard against a corrupt
    or hostile repository's implausible sizes, e.g. one ``DedupFile.read()``)
    or the free disk space a temporary copy needs with a reserve left
    over (``storage.disk_space``); not a claim that the bytes are damaged."""


class WorkerProcessError(ApmRepoError):
    """A worker process of an export or a FULL verify died (killed, out of
    memory, crashed), so the operation was aborted. Not a statement about
    the repository's data."""
