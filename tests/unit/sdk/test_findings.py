"""Unit tests for ``synology_apm_repo.sdk.findings``: the ``Finding``
dataclass and the enum values ``--json`` output exposes."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.findings import Finding, Stage, Symptom, VerifyLevel, VerifyTuning


class TestFindingType:
    def test_finding_is_frozen(self) -> None:
        finding = Finding(Stage.VERSION, Symptom.DATA_MISSING, "path", "detail")
        with pytest.raises(AttributeError, match="cannot assign to field 'path'"):
            finding.path = "other"  # type: ignore[misc]

    def test_ref_defaults_to_none(self) -> None:
        assert Finding(Stage.VERSION, Symptom.DATA_MISSING, "path", "detail").ref is None


class TestEnumValues:
    """``Symptom``/``VerifyLevel``'s ``.value`` strings are part of the
    CLI's ``--json`` output contract."""

    def test_symptom_values(self) -> None:
        assert {s.value for s in Symptom} == {
            "Corruption",
            "FileMissing",
            "Mismatch",
            "DataMissing",
            "KeyMissing",
            "RepairedViaParity",
        }

    def test_verify_level_values(self) -> None:
        assert VerifyLevel.QUICK.value == "quick"
        assert VerifyLevel.FULL.value == "full"


@pytest.mark.parametrize("field", ["max_concurrent_reads", "max_concurrent_decodes"])
@pytest.mark.parametrize("value", [0, -1])
def test_verify_tuning_rejects_a_bound_below_one(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        VerifyTuning(**{field: value})
