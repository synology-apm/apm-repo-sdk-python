"""The value types of a ``verify`` run: the ``VerifyLevel`` and
``VerifyTuning`` it runs with, and each ``Finding`` it reports, classified
by ``Stage``/``Symptom``. Plain data,
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


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyTuning:
    """How a ``verify`` run spreads its reads; the defaults suit most stores.

    Attributes:
        max_concurrent_reads: Buckets checked, and at FULL level sized, at
            once in this process. A QUICK check or a FULL sizing is a few
            small reads: several hide a remote store's latency; past what
            the store itself serves, more only add latency.
        max_concurrent_decodes: Of those, FULL checks at once, each holding
            one read plus 4 MiB of plaintext: bounds this process's memory.
            Not applied to FULL checks in worker processes (one bucket
            each), which run whenever the store can be reopened in one.

    Raises:
        ValueError: ``max_concurrent_reads`` or ``max_concurrent_decodes``
            is less than 1.
    """

    max_concurrent_reads: int = 16
    max_concurrent_decodes: int = 4

    def __post_init__(self) -> None:
        for name in ("max_concurrent_reads", "max_concurrent_decodes"):
            value = getattr(self, name)
            if value < 1:
                raise ValueError(f"{name} must be 1 or more, got {value}")


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
    ``repo_path#@cat:<id>/wl:<id>/ver:<uid>`` ref (``units.node_ref.NodeRef``)
    naming the catalog/workload/version being checked, set by the
    reachability walk. For data shared by several versions
    through dedup, it names the first version whose check hit the finding;
    later versions sharing the checked data add none."""

    stage: Stage
    symptom: Symptom
    path: str
    detail: str
    ref: str | None = None
