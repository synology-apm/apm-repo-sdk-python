"""Unit tests for ``browser.core.remote_data`` — plain value-object
behavior (construction, equality, defaults) plus exhaustive ``match``
dispatch over all four ``RemoteData`` variants, since that's the whole
reason this module exists: a selector reading a model's ``RemoteData``
field is expected to ``match`` over exactly these four cases, ending in
``case _: assert_never(data)``."""

from __future__ import annotations

from typing import assert_never

from synology_apm_repo.browser.core.remote_data import (
    FailureInfo,
    FailureKind,
    Loading,
    NotAsked,
    NoValue,
    RemoteData,
    Success,
    has_ever_resolved,
    loading_preserving,
    value_or_stale,
)


def _describe(data: RemoteData[int]) -> str:
    match data:
        case NotAsked():
            return "not asked"
        case Loading(previous=previous):
            return f"loading (previous={previous})"
        case Success(value=value):
            return f"success: {value}"
        case FailureInfo(message=message):
            return f"failed: {message}"
        case _:
            assert_never(data)


def test_not_asked_is_the_default_starting_state() -> None:
    assert _describe(NotAsked()) == "not asked"


def test_loading_with_no_previous_value_defaults_to_no_value() -> None:
    loading: Loading[int] = Loading()
    assert isinstance(loading.previous, NoValue)
    assert _describe(loading) == "loading (previous=NoValue())"


def test_loading_can_carry_the_last_known_good_value() -> None:
    loading = Loading(previous=7)
    assert _describe(loading) == "loading (previous=7)"


def test_success_carries_its_value() -> None:
    assert _describe(Success(value=42)) == "success: 42"


def test_failure_info_defaults_to_kind_other() -> None:
    failure = FailureInfo(message="boom")
    assert failure.kind is FailureKind.OTHER
    assert _describe(failure) == "failed: boom"


def test_failure_info_can_carry_key_required_kind() -> None:
    """The one failure kind ``update()`` reacts to structurally (pushing
    ``KeyDialog``) rather than just rendering the message like every other
    failure kind does."""
    failure = FailureInfo(message="key needed", kind=FailureKind.KEY_REQUIRED)
    assert failure.kind is FailureKind.KEY_REQUIRED


def test_equal_variants_compare_equal() -> None:
    """Frozen-dataclass field-wise equality -- what lets an ``update()``
    branch that returns an untouched slice compare cheaply against a
    freshly-selected one (Store._notify's own slice-diffing)."""
    assert Success(value=1) == Success(value=1)
    assert Success(value=1) != Success(value=2)
    assert NotAsked() == NotAsked()
    assert Loading(previous=1) == Loading(previous=1)
    assert FailureInfo(message="x") == FailureInfo(message="x")


def test_loading_preserving_carries_a_real_success_forward() -> None:
    assert loading_preserving(Success(value=7)) == Loading(previous=7)


def test_loading_preserving_has_nothing_to_carry_from_any_other_state() -> None:
    assert loading_preserving(NotAsked()) == Loading()
    assert loading_preserving(Loading(previous=7)) == Loading()
    assert loading_preserving(FailureInfo(message="boom")) == Loading()


def test_has_ever_resolved_is_true_once_a_real_value_exists() -> None:
    assert has_ever_resolved(Success(value=7)) is True
    assert has_ever_resolved(Loading(previous=7)) is True


def test_has_ever_resolved_is_false_before_anything_has_ever_resolved() -> None:
    assert has_ever_resolved(NotAsked()) is False
    assert has_ever_resolved(Loading()) is False
    assert has_ever_resolved(FailureInfo(message="boom")) is False


def test_value_or_stale_returns_no_value_before_anything_has_resolved() -> None:
    assert isinstance(value_or_stale(NotAsked()), NoValue)
    assert isinstance(value_or_stale(Loading()), NoValue)
    assert isinstance(value_or_stale(FailureInfo(message="boom")), NoValue)


def test_value_or_stale_falls_back_to_a_real_previous_value() -> None:
    assert value_or_stale(Loading(previous=7)) == 7
    assert value_or_stale(Success(value=7)) == 7


def test_value_or_stale_distinguishes_a_none_valued_success_from_no_value() -> None:
    """The exact hazard a bare-``None`` sentinel would get wrong: a ``T``
    that legitimately includes ``None`` as a resolved value."""
    state: RemoteData[str | None] = Success(value=None)
    value = value_or_stale(state)
    assert value is None  # the real value, not NoValue()
    assert not isinstance(value, NoValue)


def test_has_ever_resolved_is_true_for_a_success_whose_own_value_is_none() -> None:
    state: RemoteData[str | None] = Success(value=None)
    assert has_ever_resolved(state) is True
