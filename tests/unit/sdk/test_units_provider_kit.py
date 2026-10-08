"""``units/provider_kit.py``: the helpers every ``UnitProvider`` builds its nodes with."""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest

from synology_apm_repo.sdk.errors import ApmRepoError, NotRestorableError
from synology_apm_repo.sdk.units.provider_kit import mtime_from_epoch, not_restorable, paginate


def test_mtime_from_epoch_is_none_for_none() -> None:
    assert mtime_from_epoch(None) is None


def test_mtime_from_epoch_converts_a_real_epoch() -> None:
    assert mtime_from_epoch(0) == datetime.fromtimestamp(0, UTC)


def test_mtime_from_epoch_degrades_to_none_for_an_out_of_range_value() -> None:
    """An out-of-range raw epoch degrades to ``None`` rather than raising out
    of ``children()`` and failing the whole folder listing."""
    assert mtime_from_epoch(99999999999999) is None  # ValueError: year out of range
    assert mtime_from_epoch(-(2**62)) is None  # OverflowError or OSError, by platform


def test_not_restorable_names_the_kind_and_ref() -> None:
    with pytest.raises(NotRestorableError, match=r"^node 'stray' is not a restorable unit$"):
        not_restorable("node", "stray")


def test_not_restorable_ref_uses_repr_not_str() -> None:
    # ref is often a tuple key; !r must show its shape.
    with pytest.raises(NotRestorableError, match=re.escape("item ('a', 'b') is not a restorable unit")):
        not_restorable("item", ("a", "b"))


class TestPaginate:
    def test_no_offset_no_limit_returns_everything(self) -> None:
        assert paginate([1, 2, 3], 0, None) == [1, 2, 3]

    def test_offset_only(self) -> None:
        assert paginate([1, 2, 3, 4], 2, None) == [3, 4]

    def test_offset_and_limit(self) -> None:
        assert paginate([1, 2, 3, 4, 5], 1, 2) == [2, 3]

    def test_offset_past_end_returns_empty(self) -> None:
        assert paginate([1, 2], 10, None) == []

    def test_returns_a_list_not_the_original_sequence_type(self) -> None:
        result = paginate((1, 2, 3), 0, None)
        assert isinstance(result, list)


def test_not_restorable_raises_an_sdk_error() -> None:

    with pytest.raises(NotRestorableError, match="is not a restorable unit") as caught:
        not_restorable("node", "stray")

    assert isinstance(caught.value, ApmRepoError)
    assert not isinstance(caught.value, ValueError)
