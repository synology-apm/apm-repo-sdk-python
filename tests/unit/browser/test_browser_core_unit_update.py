"""Unit tests for ``browser.core.unit.update`` -- every branch, no
Textual/App/Pilot involved at all. Each test asserts on the returned
``(model, cmds)`` pair directly -- ``Cmd`` is data, never a callable, so
this is a one-line test with no effects layer involved."""

from __future__ import annotations

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.browser.core.unit.cmd import CloseProvider, LoadChildren, LoadRoot, Notify
from synology_apm_repo.browser.core.unit.model import PROVIDER_SLOT, LoadedLevel, UnitModel, children_slot
from synology_apm_repo.browser.core.unit.msg import (
    ChainStepResolved,
    ChildrenLoaded,
    ChildrenLoadFailed,
    ChildrenRequested,
    FilterClosed,
    FilterOpened,
    FilterTextChanged,
    FolderSelected,
    LoadMoreRequested,
    MoreChildrenLoaded,
    MoreChildrenLoadFailed,
    RootLoaded,
    RootLoadFailed,
    RootRequested,
)
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE, update
from synology_apm_repo.browser.strings import (
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_ALREADY_LOADING_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _node(name: str = "item") -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=False)


def _leaf(name: str = "leaf") -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True)


def test_root_requested_bumps_epoch_and_mints_a_load_root_command() -> None:
    model = UnitModel()
    new_model, cmds = update(model, RootRequested(invalidate=False, force_raw=True))

    assert new_model.epoch == Epoch(1)
    assert new_model.inflight == {PROVIDER_SLOT: RequestId(1)}
    assert cmds == (LoadRoot(epoch=Epoch(1), request=RequestId(1), invalidate=False, force_raw=True),)


def test_root_requested_also_closes_a_provider_already_open() -> None:
    model = UnitModel(provider=ProviderHandle(7), root=_node())
    new_model, cmds = update(model, RootRequested(invalidate=True, force_raw=False))

    assert new_model.provider is None
    assert new_model.root is None
    assert cmds == (
        CloseProvider(provider=ProviderHandle(7)),
        LoadRoot(epoch=Epoch(1), request=RequestId(1), invalidate=True, force_raw=False),
    )


def test_root_requested_clears_loaded_errors_filter_root_error_and_selected() -> None:
    ref = NodeRef("repo", ("x",))
    child = _leaf("child")
    model = UnitModel(
        loaded={ref: LoadedLevel(children=(child,), exhausted=True)},
        node_index={child.ref: child},
        errors={ref: "boom"},
        root_error="a previous root load failed",
        filter=None,
        selected=ref,
        pending=frozenset({children_slot(ref)}),
    )
    model = update(model, FilterOpened(ref=ref))[0]
    assert model.filter is not None  # test setup: a filter really is open before RootRequested below

    new_model, _cmds = update(model, RootRequested(invalidate=False, force_raw=False))

    assert new_model.loaded == {}
    assert new_model.node_index == {}
    assert new_model.errors == {}
    assert new_model.filter is None
    assert new_model.root_error is None
    assert new_model.selected is None
    # An epoch bump invalidates every in-flight children fetch, the same
    # reasoning already applied to `inflight` above.
    assert new_model.pending == frozenset()


def test_root_loaded_publishes_when_epoch_and_request_match() -> None:
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(1)})
    root = _node("root")
    new_model, cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(1), root=root)
    )

    assert new_model.provider == ProviderHandle(1)
    assert new_model.root is root
    assert new_model.selected == root.ref
    assert cmds == ()


def test_root_loaded_sets_selected_even_for_a_leaf_root() -> None:
    """Harmless: a leaf's own ref is never used as a LoadChildren target
    either way."""
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(1)})
    root = _leaf("solo")
    new_model, _cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(1), root=root)
    )

    assert new_model.selected == root.ref


def test_root_loaded_closes_a_superseded_provider_instead_of_publishing_it() -> None:
    model = UnitModel(epoch=Epoch(2), inflight={PROVIDER_SLOT: RequestId(2)})
    new_model, cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(9), root=_node())
    )

    assert new_model is model
    assert cmds == (CloseProvider(provider=ProviderHandle(9)),)


def test_root_loaded_dropped_on_a_request_mismatch_within_the_same_epoch() -> None:
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(2)})
    new_model, cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(9), root=_node())
    )

    assert new_model is model
    assert cmds == (CloseProvider(provider=ProviderHandle(9)),)


def test_root_load_failed_sets_root_error_when_current() -> None:
    """Rendered into the detail pane, not a toast: there's no existing
    content yet for a toast to leave visible, so the failure needs to
    stay on screen rather than fade."""
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(1)})
    new_model, cmds = update(model, RootLoadFailed(epoch=Epoch(1), request=RequestId(1), message="boom"))

    assert new_model.root_error == "boom"
    assert cmds == ()


def test_root_loaded_clears_a_prior_root_error() -> None:
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(1)}, root_error="stale error")
    new_model, _cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(1), root=_node())
    )

    assert new_model.root_error is None


def test_root_load_failed_is_a_no_op_when_superseded() -> None:
    model = UnitModel(epoch=Epoch(2), inflight={PROVIDER_SLOT: RequestId(2)})
    new_model, cmds = update(model, RootLoadFailed(epoch=Epoch(1), request=RequestId(1), message="boom"))

    assert new_model is model
    assert cmds == ()


def test_children_requested_mints_a_load_children_command() -> None:
    node = _node("folder")
    model = UnitModel(provider=ProviderHandle(3))
    new_model, cmds = update(model, ChildrenRequested(node=node))

    assert new_model.inflight == {children_slot(node.ref): RequestId(1)}
    assert new_model.pending == frozenset({children_slot(node.ref)})
    assert cmds == (
        LoadChildren(
            epoch=Epoch(0),
            request=RequestId(1),
            provider=ProviderHandle(3),
            node=node,
            offset=0,
            limit=CHILDREN_PAGE_SIZE,
        ),
    )


def test_children_loaded_populates_the_level_when_current() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(1)})
    children = (_leaf("a"), _leaf("b"))
    new_model, cmds = update(
        model, ChildrenLoaded(epoch=Epoch(0), request=RequestId(1), ref=node.ref, children=children, exhausted=True)
    )

    assert new_model.loaded[node.ref] == LoadedLevel(children=children, exhausted=True)
    assert new_model.node_index == {children[0].ref: children[0], children[1].ref: children[1]}
    assert cmds == ()


def test_children_loaded_clears_a_prior_error_for_the_same_ref() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(1)}, errors={node.ref: "boom"})
    new_model, _ = update(
        model, ChildrenLoaded(epoch=Epoch(0), request=RequestId(1), ref=node.ref, children=(), exhausted=True)
    )

    assert node.ref not in new_model.errors


def test_children_loaded_dropped_when_superseded() -> None:
    """A stale result must leave `pending` alone, not just `loaded` --
    clearing it unconditionally could wipe out a still-in-flight newer
    request's own marker for this same slot."""
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(2)}, pending=frozenset({slot}))
    new_model, cmds = update(
        model, ChildrenLoaded(epoch=Epoch(0), request=RequestId(1), ref=node.ref, children=(), exhausted=True)
    )

    assert new_model is model
    assert cmds == ()


def test_children_load_failed_records_an_error_when_current() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(1)})
    new_model, cmds = update(
        model, ChildrenLoadFailed(epoch=Epoch(0), request=RequestId(1), ref=node.ref, message="boom")
    )

    assert new_model.errors == {node.ref: "boom"}
    assert cmds == ()


def test_children_load_failed_dropped_when_superseded() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(2)}, pending=frozenset({slot}))
    new_model, cmds = update(
        model, ChildrenLoadFailed(epoch=Epoch(0), request=RequestId(1), ref=node.ref, message="boom")
    )

    assert new_model is model
    assert cmds == ()


def test_load_more_requested_warns_when_nothing_loaded() -> None:
    node = _node("folder")
    model = UnitModel()
    new_model, cmds = update(model, LoadMoreRequested(node=node))

    assert new_model is model
    assert cmds == (Notify(message=UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING, severity="warning"),)


def test_load_more_requested_warns_when_already_exhausted() -> None:
    node = _node("folder")
    model = UnitModel(loaded={node.ref: LoadedLevel(children=(), exhausted=True)})
    new_model, cmds = update(model, LoadMoreRequested(node=node))

    assert new_model is model
    assert cmds == (Notify(message=UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING, severity="warning"),)


def test_load_more_requested_mints_a_load_children_command_at_the_next_offset() -> None:
    node = _node("folder")
    level = LoadedLevel(children=(_leaf("a"),), exhausted=False)
    model = UnitModel(provider=ProviderHandle(5), loaded={node.ref: level})
    new_model, cmds = update(model, LoadMoreRequested(node=node))

    assert new_model.inflight == {children_slot(node.ref): RequestId(1)}
    assert new_model.pending == frozenset({children_slot(node.ref)})
    assert cmds == (
        LoadChildren(
            epoch=Epoch(0),
            request=RequestId(1),
            provider=ProviderHandle(5),
            node=node,
            offset=1,
            limit=CHILDREN_PAGE_SIZE,
        ),
    )


def test_load_more_requested_warns_when_already_pending() -> None:
    """The reported bug's load-more analogue: pressing ``+`` again before
    the first load-more's own fetch resolves must not mint a second
    ``LoadChildren`` -- checked before ``existing_level.exhausted`` would
    even matter, since a not-yet-resolved page hasn't updated ``exhausted``
    yet either way."""
    node = _node("folder")
    level = LoadedLevel(children=(_leaf("a"),), exhausted=False)
    model = UnitModel(loaded={node.ref: level}, pending=frozenset({children_slot(node.ref)}))
    new_model, cmds = update(model, LoadMoreRequested(node=node))

    assert new_model is model
    assert cmds == (Notify(message=UNIT_LOAD_MORE_ALREADY_LOADING_WARNING, severity="warning"),)


def test_more_children_loaded_appends_and_notifies_when_current() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    level = LoadedLevel(children=(_leaf("a"),), exhausted=False)
    model = UnitModel(inflight={slot: RequestId(2)}, loaded={node.ref: level})
    more = (_leaf("b"),)
    new_model, cmds = update(
        model, MoreChildrenLoaded(epoch=Epoch(0), request=RequestId(2), ref=node.ref, more=more, exhausted=True)
    )

    new_level = new_model.loaded[node.ref]
    assert [c.name for c in new_level.children] == ["a", "b"]
    assert new_level.next_offset == 2
    assert new_level.exhausted is True
    assert new_model.node_index == {more[0].ref: more[0]}
    assert len(cmds) == 1
    assert isinstance(cmds[0], Notify)


def test_more_children_loaded_dropped_when_superseded() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(3)}, pending=frozenset({slot}))
    new_model, cmds = update(
        model, MoreChildrenLoaded(epoch=Epoch(0), request=RequestId(1), ref=node.ref, more=(), exhausted=True)
    )

    assert new_model is model
    assert cmds == ()


def test_more_children_load_failed_notifies_when_current() -> None:
    node = _node("folder")
    slot = children_slot(node.ref)
    model = UnitModel(inflight={slot: RequestId(1)}, pending=frozenset({slot}))
    new_model, cmds = update(
        model, MoreChildrenLoadFailed(epoch=Epoch(0), request=RequestId(1), ref=node.ref, message="boom")
    )

    assert slot not in new_model.pending
    assert cmds == (Notify(message="boom", severity="warning"),)


def test_chain_step_resolved_unconditionally_replaces_the_level() -> None:
    ref = NodeRef("repo", ("root", "x"))
    model = UnitModel(loaded={ref: LoadedLevel(children=(_leaf("stale"),), exhausted=False)})
    fresh = (_leaf("a"), _leaf("b"))
    new_model, cmds = update(model, ChainStepResolved(ref=ref, children=fresh))

    assert new_model.loaded[ref] == LoadedLevel(children=fresh, exhausted=True)
    assert new_model.node_index == {fresh[0].ref: fresh[0], fresh[1].ref: fresh[1]}
    assert cmds == ()


def test_chain_step_resolved_does_not_touch_an_unrelated_refs_identity() -> None:
    ref_a = NodeRef("repo", ("a",))
    ref_b = NodeRef("repo", ("b",))
    level_b = LoadedLevel(children=(), exhausted=True)
    model = UnitModel(loaded={ref_b: level_b})
    new_model, _ = update(model, ChainStepResolved(ref=ref_a, children=()))

    assert new_model.loaded[ref_b] is level_b


def test_chain_step_resolved_clears_the_refs_own_in_flight_children_slot() -> None:
    ref = NodeRef("repo", ("root", "x"))
    slot = children_slot(ref)
    model = UnitModel(inflight={slot: RequestId(2)})
    new_model, _ = update(model, ChainStepResolved(ref=ref, children=()))

    assert slot not in new_model.inflight


def test_chain_step_resolved_clears_the_refs_own_pending_slot() -> None:
    """Mirrors the ``inflight`` scrub above, for the same reason: an
    in-flight ordinary fetch for the same ``ref`` must not still count as
    "pending" once this walk step has exhaustively resolved it."""
    ref = NodeRef("repo", ("root", "x"))
    slot = children_slot(ref)
    model = UnitModel(pending=frozenset({slot}))
    new_model, _ = update(model, ChainStepResolved(ref=ref, children=()))

    assert slot not in new_model.pending


def test_chain_step_resolved_leaves_an_unrelated_slot_and_untouched_inflight_dict_alone() -> None:
    other_slot = children_slot(NodeRef("repo", ("other",)))
    model = UnitModel(inflight={other_slot: RequestId(4)})
    new_model, _ = update(model, ChainStepResolved(ref=NodeRef("repo", ("root", "x")), children=()))

    assert new_model.inflight is model.inflight


def test_chain_step_resolved_invalidates_a_late_load_more_for_the_same_ref() -> None:
    """The race this closes: a goto walk resolves `ref` with its own
    exhaustive list while an ordinary load-more for that same `ref` is
    still in flight from before the walk started. Without clearing
    `ref`'s own inflight slot here, the load-more's later arrival would
    pass its own is_stale() check unchanged and corrupt the walk's
    already-exhaustive list by appending a stale page onto it."""
    ref = NodeRef("repo", ("root", "x"))
    slot = children_slot(ref)
    stale_request = RequestId(2)
    model = UnitModel(inflight={slot: stale_request}, loaded={ref: LoadedLevel(children=(), exhausted=False)})

    fresh = (_leaf("a"), _leaf("b"))
    model, _ = update(model, ChainStepResolved(ref=ref, children=fresh))

    new_model, cmds = update(
        model,
        MoreChildrenLoaded(epoch=Epoch(0), request=stale_request, ref=ref, more=(_leaf("stale-page"),), exhausted=True),
    )

    assert new_model is model
    assert cmds == ()
    assert new_model.loaded[ref] == LoadedLevel(children=fresh, exhausted=True)


def test_filter_opened_sets_the_filter_state() -> None:
    ref = NodeRef("repo", ("folder",))
    model = UnitModel()
    new_model, cmds = update(model, FilterOpened(ref=ref))

    assert new_model.filter is not None
    assert new_model.filter.ref == ref
    assert new_model.filter.text == ""
    assert cmds == ()


def test_filter_text_changed_updates_the_open_filters_text() -> None:
    ref = NodeRef("repo", ("folder",))
    model = update(UnitModel(), FilterOpened(ref=ref))[0]
    new_model, cmds = update(model, FilterTextChanged(text="needle"))

    assert new_model.filter is not None
    assert new_model.filter.text == "needle"
    assert cmds == ()


def test_filter_text_changed_is_a_no_op_with_no_filter_open() -> None:
    model = UnitModel()
    new_model, cmds = update(model, FilterTextChanged(text="needle"))

    assert new_model is model
    assert cmds == ()


def test_filter_closed_clears_the_filter_state() -> None:
    ref = NodeRef("repo", ("folder",))
    model = update(UnitModel(), FilterOpened(ref=ref))[0]
    new_model, cmds = update(model, FilterClosed())

    assert new_model.filter is None
    assert cmds == ()


def test_filter_closed_is_a_no_op_with_no_filter_open() -> None:
    model = UnitModel()
    new_model, cmds = update(model, FilterClosed())

    assert new_model is model
    assert cmds == ()


def test_folder_selected_updates_selected_with_no_cmd() -> None:
    ref = NodeRef("repo", ("folder",))
    model = UnitModel()
    new_model, cmds = update(model, FolderSelected(ref=ref))

    assert new_model.selected == ref
    assert cmds == ()


__all__: list[str] = []
