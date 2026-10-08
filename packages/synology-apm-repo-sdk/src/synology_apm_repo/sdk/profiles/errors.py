"""The errors the connection-profile store raises."""

from __future__ import annotations

from ..errors import ApmRepoError, NotFoundError


class ProfileNotFoundError(NotFoundError):
    """The named S3/Azure/SMB connection profile does not exist in
    ``profiles.json``."""


class ProfileConfigCorruptError(ApmRepoError):
    """``profiles.json`` failed to parse, or failed schema validation (bad
    ``schema_version``, unknown ``kind``, missing/malformed field). Not a
    ``DataCorruptError``: the file is local configuration, not repository
    data."""


class ProfileFieldError(ValueError):
    """A connection-profile field is missing or malformed (a blank required
    field, a non-numeric port): a user-input problem, hence a ``ValueError``
    rather than part of the on-disk-data hierarchy.

    Attributes:
        field: The canonical field name (``"bucket"``, ``"port"``, ...).
    """

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field


class ProfileSecretBackendUnavailableError(ApmRepoError):
    """No usable OS keyring backend can store or retrieve a profile's
    secret fields: ``keyring`` failed to import or resolved no real
    backend (e.g. headless Linux without Secret Service)."""
