"""Exception hierarchy for synology-apm-repo-sdk.

Every exception carries two optional pieces of support/forensics context:

- ``ref``: which node/file the error refers to — a plain string (a
  store-relative path, a ``file_map`` path, or similar locator). Callers
  holding a ``NodeRef`` pass ``str(node_ref)``; this module doesn't import
  that type, to avoid a dependency from the lowest layer onto the highest.
- ``spec``: a pointer into ``FORMAT-SPEC.md`` (e.g. ``"FORMAT-SPEC.md:
  SizeStore"``) explaining why this is an error, not just that it is one.

No exception here is named ``KeyError`` — that would shadow the builtin
and be silently swallowed by unrelated ``except KeyError`` blocks.
Argument/programmer-error validation raises stdlib ``ValueError``/
``IndexError`` directly instead of this hierarchy, which is reserved for
on-disk data problems.
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
        """This error's message alone, without the ``[ref=...]``/
        ``[spec=...]`` tags ``str(exc)`` appends — for a presentation
        layer's default, non-verbose rendering. Strips only that
        structured suffix, not every occurrence of the same value a raise
        site's own message text may separately mention. ``str(exc)``
        itself is unchanged and keeps carrying the full detail (internal
        call sites that store ``str(exc)`` for later diagnostic display,
        e.g. ``units/device_disk_fs.py``'s per-disk failure reasons, rely
        on that)."""
        return self._message


class FormatError(ApmRepoError):
    """The bytes at ``ref`` do not match the on-disk format they were
    expected to have (magic mismatch, CRC mismatch, unsupported header
    version, truncated file, ...)."""


class DataCorruptError(FormatError):
    """A CRC32 (or similar) integrity check failed: the magic matched but
    the payload did not survive intact."""


class UnsupportedVersionError(FormatError):
    """The on-disk major/minor version is newer than this SDK understands."""


class ChunkCompactedError(FormatError):
    """SizeStore reports ``CompressType.COMPACTED`` for this chunk — the
    data was reclaimed by the write-side compactor and can no longer be
    read (FORMAT-SPEC.md: SizeStore)."""


class KeyMaterialError(ApmRepoError):
    """The key string itself (``<userKeyID>@<base64(userKey)>``) is
    malformed, or does not match this repository's wrapped VaultKey."""


class KeyRequiredError(KeyMaterialError):
    """The repository is encrypted (some ``.buk`` has mode bit ``0x80`` set,
    or a ``copy_meta_file`` entry starts with the ``aHlT`` magic) but no key
    was supplied."""


class KeyMismatchError(KeyMaterialError):
    """The AES-256-GCM tag check, or the chunk-fingerprint verification,
    failed for the supplied key (FORMAT-SPEC.md: chunk-pool-encryption/vaultkey-custody)."""


class NotFoundError(ApmRepoError):
    """The referenced path / node / version / object does not exist in this
    repository — as opposed to existing but being unreadable."""


class ContentUnavailableError(ApmRepoError):
    """This node's metadata was found, but its real content is not
    available to read — either the guest OS only held a placeholder at
    backup time (e.g. iCloud Drive/OneDrive Files-On-Demand eviction), or
    the bytes exist but this SDK has no key material for them (e.g. an
    NTFS EFS-encrypted file). Distinct from ``DataCorruptError``: nothing
    here asserts the bytes are damaged. Not a ``NotFoundError`` subclass,
    so callers treating that as absent-and-skippable don't swallow this
    too."""


class PermissionDeniedError(ApmRepoError):
    """The referenced path/node exists but the OS or backend denied the
    access needed to read or list it — as opposed to not existing at all
    (see ``NotFoundError``). Not a ``NotFoundError`` subclass, so callers
    treating that as absent-and-skippable don't swallow this too."""


class UnsupportedDataFormatError(ApmRepoError):
    """The content exists but this version of the SDK deliberately refuses
    to interpret it (e.g. a CBT merge chain) rather than risk emitting
    silently-wrong bytes."""


class ProfileNotFoundError(NotFoundError):
    """The named S3/Azure/SMB connection profile does not exist in
    ``profiles.json``."""


class ProfileConfigCorruptError(DataCorruptError):
    """``profiles.json`` failed to parse, or failed schema validation (bad
    ``schema_version``, unknown ``kind``, missing/malformed field)."""


class ProfileSecretBackendUnavailableError(ApmRepoError):
    """No usable OS keyring backend is available to store/retrieve a
    profile's secret fields — either ``keyring`` failed to import, or it
    resolved no real backend (e.g. headless Linux with no Secret
    Service/dbus). Not a ``KeyMaterialError`` subclass — that hierarchy is
    about a dedup repository's own encryption key, an unrelated "key"."""
