"""``Session``/``Repository``: the SDK's outward-facing door.

CLI and TUI code should only ever import from here, plus: ``units.base``
for ``Node``/``RestorableUnit``/``ContentSource``, ``units.node_ref`` for
address logic, ``presentation`` for display formatting, ``storage`` for
the ``ObjectStore`` types ``Session.open_remote()``/profile-connection
flows construct by hand, ``errors`` for the exception types every caller
catches (``ApmRepoError`` and its subclasses), ``identifiers`` for
``CatalogId``/other id casts, and ``profiles`` for saved-connection CRUD
(``profile add``/``list``/``show``/``remove``, the browser's own profile
manager and remote-connect dialog). Everything else below the Repository
Layer is an
implementation detail this package owns and hides. See
``ARCHITECTURE.md``'s "Repository Layer" section for the
``Session``/``Repository``/``Catalog`` split and what each covers; every
public name from all three is re-exported here, plus the value types
their own methods hand back (``Connection``, ``Version``/``Workload``,
``ExportResult``, ``KeyVerification``) so a caller never has to reach
past this module just to type-annotate or format one.

Three narrower, explicitly-named exceptions to the above exist and are not
re-exported here because each is scoped to exactly one call site rather
than being part of the general CLI/TUI vocabulary: the CLI's ``dump``
command reaches into ``sdk.diagnostics`` directly (that module's own
docstring explains why it stays a sibling of this package rather than
living inside it), the TUI's ``UnitScreen`` reaches into
``units.device_disk_fs``/``units.resolve``/``units.saas.site`` for three
node-navigation helpers with no Repository-layer equivalent, and the
TUI's own entry point (``browser.app``) reaches into
``concurrency.preload_resource_tracker()`` before starting the app (see
that call site's own comment).
"""

from __future__ import annotations

from ..catalog.connection import Connection
from ..catalog.version import Version
from ..catalog.workload import Workload
from ..dedup.dedup_file import ExportResult
from ..dedup.keys import KeyVerification
from .catalog import Catalog, Frame
from .repository import Finding, KeyStatus, Repository, Stage, Symptom, VerifyLevel
from .session import Session, TraceEvent

__all__ = [
    "Catalog",
    "Connection",
    "ExportResult",
    "Finding",
    "Frame",
    "KeyStatus",
    "KeyVerification",
    "Repository",
    "Session",
    "Stage",
    "Symptom",
    "TraceEvent",
    "Version",
    "VerifyLevel",
    "Workload",
]
