"""The SDK's ``__all__``-carrying public modules: the top-level package
(which re-exports the whole ``api`` facade) plus ``sdk.export``/
``sdk.presentation``/``sdk.profiles``, each name exported from one of them
only; internals stay out of them."""

from __future__ import annotations

from types import ModuleType

import pytest

import synology_apm_repo.sdk as sdk
from synology_apm_repo.sdk import api, export, presentation, profiles

_SINK_CONTRACT = [
    "AbortOutcome",
    "BufferedExportSink",
    "ExportSink",
    "ExportWriter",
    "FlushableSegment",
    "LocalFileSink",
    "RandomAccessExportSink",
    "SegmentWriter",
    "SinkCaps",
    "SinkDescriptor",
    "WorkerTarget",
    "WorkerWriter",
    "run_export",
]

_INTERNAL = ["ExportExecutor", "ExportTuning", "OffsetWriter", "needs_zero_fill", "run_sink_export"]


@pytest.mark.parametrize("name", _SINK_CONTRACT)
def test_a_sink_contract_name_is_exported_from_sdk_export_only(name: str) -> None:
    assert name in export.__all__
    assert name not in sdk.__all__
    assert name not in api.__all__


@pytest.mark.parametrize("name", _INTERNAL)
def test_sink_internals_are_not_exported(name: str) -> None:
    assert name not in export.__all__
    assert name not in sdk.__all__
    assert name not in api.__all__


def test_no_name_is_exported_from_two_public_modules() -> None:
    public = (sdk, export, presentation, profiles)
    seen: dict[str, str] = {}
    duplicates = []
    for module in public:
        for name in module.__all__:
            if name in seen:
                duplicates.append((name, seen[name], module.__name__))
            seen.setdefault(name, module.__name__)
    assert duplicates == []


def test_the_top_level_re_exports_the_whole_facade() -> None:
    assert [name for name in api.__all__ if name not in sdk.__all__] == []


@pytest.mark.parametrize(
    ("module", "name"),
    [
        (sdk, "DirCache"),
        (sdk, "KeyMaterial"),
        (sdk, "client_kwargs_with_secrets"),
        (profiles, "client_kwargs_with_secrets"),
    ],
)
def test_internal_helpers_are_not_exported(module: ModuleType, name: str) -> None:
    assert name not in module.__all__


def test_every_name_in_all_exists() -> None:
    for module in (sdk, api, export, presentation, profiles):
        assert [name for name in module.__all__ if not hasattr(module, name)] == []
        assert len(module.__all__) == len(set(module.__all__))
