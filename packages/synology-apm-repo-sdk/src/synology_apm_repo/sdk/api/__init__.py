"""``Session``/``Repository``: the SDK's outward-facing door.

CLI and TUI code should only ever import from here, plus: ``units.base``
for its whole public surface — ``Node``/``RestorableUnit``/
``ContentSource`` and the render-site narrowing helpers built on them
(``UnitKind``, ``FileState``, ``UnitProvider``, ``ClosableUnitProvider``,
``node_file_state``, ``node_kind_label``, ``node_leaf_kind``,
``node_modified_time`` — the shared vocabulary every workload provider
(Device, FS, SaaS, ...) implements ``UnitProvider`` against, so CLI/TUI
render sites never need to know which workload they're looking at),
``units.node_ref`` for address
logic, ``presentation`` for display formatting, ``storage`` for
the ``ObjectStore`` types ``Session.open_remote()``/profile-connection
flows construct by hand, ``errors`` for the exception types every caller
catches (``ApmRepoError`` and its subclasses), ``identifiers`` for
``CatalogId``/other id casts, and ``profiles`` for saved-connection CRUD
(``profile add``/``list``/``show``/``remove``, the browser's own profile
manager and remote-connect dialog). Everything else below the Repository
Layer is an implementation detail this package owns and hides. See
``ARCHITECTURE.md``'s "Repository Layer" section for the
``Session``/``Repository``/``Catalog`` split and what each covers; every
public name from all three is re-exported here, plus the value types
their own methods hand back (``Connection``, ``Version``/``Workload``,
``ExportResult``, ``KeyVerification``, ``RepositoryLayout``) so a caller
never has to reach past this module just to type-annotate or format one.
``group_findings``/
``sort_key``/``AugmentedFinding``/``group_count_label`` are the one
exception to that hand-back-value-types framing: not something a
``Repository``/``Catalog`` method returns, but the grouping/ordering/
labeling logic a caller rendering ``Repository.verify()``'s own ``Finding``
list needs — re-exported here anyway, from ``dedup.verify_report``, since
``Finding`` itself already is.

Narrower, explicitly-named exceptions to the above exist and are not
re-exported here, each scoped to one call site (or a small cluster of call
sites sharing the same helper) rather than being part of the general
CLI/TUI vocabulary: the CLI's ``dump`` command reaches into
``sdk.diagnostics`` directly (it bypasses the catalog entirely, addressing
a raw file by store-relative path for forensics on data outside any
discovered repository, so it stays a sibling of this package rather than
living inside it); the TUI's
``UnitScreen`` alone reaches into ``units.resolve`` for its own goto-ref
chain walk, with no Repository-layer equivalent; both ``UnitScreen`` and
``browser.core.unit.select`` reach into ``units.saas.site`` for its
SharePoint List-overview/List-category presentational predicates
(``is_list_overview``/``is_flat_category``), again with no Repository-layer
equivalent; the TUI's own entry point (``browser.app``) reaches into
``concurrency.preload_resource_tracker()`` before starting the app
(Textual's own output capture replaces ``sys.stderr`` with a stream whose
``fileno()`` returns a sentinel, which crashes the tracker's ordinary lazy
launch later, so it must launch now while ``sys.stderr`` is still real);
and ``UnitScreen`` alone also reaches into
``concurrency.bounded_gather()`` for its own SharePoint List-overview
per-item fetch, the same bounded-fan-out helper
``units.verify_reachable`` uses, again with no Repository-layer
equivalent.
"""

from __future__ import annotations

from ..catalog.connection import Connection
from ..catalog.version import Version
from ..catalog.workload import Workload
from ..dedup.dedup_file import ExportResult
from ..dedup.keys import KeyVerification
from ..dedup.verify_report import AugmentedFinding, group_count_label, group_findings, sort_key
from ..storage.layout import RepositoryLayout
from .catalog import Catalog, Frame
from .repository import Finding, KeyStatus, Repository, Stage, Symptom, VerifyLevel
from .session import Session, TraceEvent

__all__ = [
    "AugmentedFinding",
    "Catalog",
    "Connection",
    "ExportResult",
    "Finding",
    "Frame",
    "KeyStatus",
    "KeyVerification",
    "Repository",
    "RepositoryLayout",
    "Session",
    "Stage",
    "Symptom",
    "TraceEvent",
    "Version",
    "VerifyLevel",
    "Workload",
    "group_count_label",
    "group_findings",
    "sort_key",
]
