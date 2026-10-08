"""The value types a ``verify`` run reports: ``Finding`` and the
``Stage``/``Symptom``/``VerifyLevel`` it is classified by. Plain data,
shared by the checks that produce findings (``dedup/``, ``units/``), the
facade that returns them (``api/``) and their rendering
(``presentation.verify_report``).
"""

from __future__ import annotations

import dataclasses
import enum


class Symptom(enum.Enum):
    """The symptom categories a finding can carry."""

    CORRUPTION = "Corruption"
    FILE_MISSING = "FileMissing"
    MISMATCH = "Mismatch"
    DATA_MISSING = "DataMissing"
    KEY_MISSING = "KeyMissing"
    REPAIRED_VIA_PARITY = "RepairedViaParity"
    """A CRC mismatch that Redundancy-blob self-repair
    (``format.redundancy.attempt_repair``) reconstructed and confirmed
    correct. Still reported: repeated repairs signal failing storage."""


class VerifyLevel(enum.Enum):
    """How thorough a ``verify`` run is. ``FULL`` reads every live chunk in a
    touched bucket and checks its ciphertext CRC32 and, unless the bucket is
    encrypted and no vault key was given, its decrypt+decompress+SHA-256
    fingerprint: as costly as exporting the
    whole repository. ``QUICK`` reads no chunk content, only each touched
    bucket's structural checks."""

    QUICK = "quick"
    FULL = "full"


class Stage(enum.StrEnum):
    """The stages a ``Finding`` can come from; a ``str`` subclass, so
    ``Finding.stage`` renders and serializes as its value."""

    REPO_INFO = "RepoInfo"
    FILE_MAP = "FileMap"
    COMPOSITION = "Composition"
    BUCKET = "Bucket"
    ENCRYPT_KEY = "EncryptKey"
    VERSION = "Version"
    """A catalog/workload/version whose metadata says it should resolve to
    content but doesn't, at the version or the workload/connection
    enumeration level."""


@dataclasses.dataclass(frozen=True, slots=True)
class Finding:
    """One integrity-check result. ``path`` is whatever store-relative
    path (or ``file_map`` path, or workload/version display info, for a
    ``Stage.VERSION`` finding) the finding is about.

    ``ref`` (``None`` unless set) is a canonical
    ``repo_path#cat:<id>/wl:<id>/ver:<uid>`` ref (``units.node_ref.NodeRef``)
    naming the catalog/workload/version being checked, set by the
    reachability walk. For data shared by several versions
    through dedup, it names the first version whose check hit the finding;
    later versions sharing the checked data add none."""

    stage: Stage
    symptom: Symptom
    path: str
    detail: str
    ref: str | None = None
