"""The connect dialog's I/O: building an ``ObjectStore`` from its fields and
scanning one for repositories. The dialog keeps only its widgets."""

from __future__ import annotations

from synology_apm_repo.browser.core.connect.validate import remote_config, validate_local
from synology_apm_repo.sdk import LocalFsStore, NotFoundError, ObjectStore, Repository, Session
from synology_apm_repo.sdk.presentation import ProgressCallback, ProgressMeter
from synology_apm_repo.sdk.profiles import BackendKind, store_from_config


def local_store(raw_path: str) -> tuple[ObjectStore, str]:
    """A ``LocalFsStore`` for the typed path, with the label to show for it.

    Raises:
        ConnectValidationError: The path is not usable.
    """
    path = validate_local(raw_path.strip())
    return LocalFsStore(path), str(path)


async def remote_store(backend: BackendKind, fields: dict[str, str | bool]) -> tuple[ObjectStore, str]:
    """The store a remote backend's form fields describe, with its label.

    Raises:
        ConnectValidationError: A field is missing or malformed.
        ProfileFieldError: Azure rejected the account URL.
    """
    config = remote_config(backend, fields)
    return await store_from_config(config, fields), config.label


async def scan_repositories(
    session: Session, store: ObjectStore, label: str, *, on_progress: ProgressCallback
) -> list[Repository]:
    """Every repository ``session`` discovers in ``store``; ``on_progress``
    receives the scan's progress.

    Raises:
        NotFoundError: The store holds no repository.
    """
    meter = ProgressMeter(callback=on_progress)
    repos = [repo async for repo in session.discover(store, progress=meter.update)]
    if not repos:
        raise NotFoundError(f"no repository found at {label!r}")
    return repos
