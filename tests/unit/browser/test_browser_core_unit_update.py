"""Unit tests for ``browser.core.unit.update``; each test asserts on the
returned ``(model, cmds)`` pair, with no Textual or effects layer."""

from __future__ import annotations

import pytest

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.browser.core.unit.cmd import (
    CancelDetailFetch,
    CloseProvider,
    LoadChildren,
    LoadListOverview,
    LoadPreview,
    LoadRoot,
    Notify,
    OpenUnit,
    ResolveGoto,
    ShowUnit,
)
from synology_apm_repo.browser.core.unit.model import (
    DETAIL_SLOT,
    GOTO_SLOT,
    LIST_OVERVIEW_ITEM_CAP,
    LIST_OVERVIEW_MAX_CONCURRENT,
    PREVIEW_READ_LIMIT,
    PROVIDER_SLOT,
    UNIT_OPEN_SLOT,
    DetailError,
    DetailIdle,
    DetailLoading,
    DetailPreview,
    DetailState,
    GotoState,
    Landing,
    LoadedLevel,
    UnitModel,
    UnitPurpose,
    children_slot,
    detail_loading,
)
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenLoaded,
    ChildrenLoadFailed,
    ChildrenRequested,
    DetailRequested,
    DetailResolved,
    FilterClosed,
    FilterOpened,
    FilterTextChanged,
    FolderSelected,
    GotoFailed,
    GotoNotFound,
    GotoRequested,
    GotoResolved,
    LoadMoreRequested,
    MoreChildrenLoaded,
    MoreChildrenLoadFailed,
    RootLoaded,
    RootLoadFailed,
    RootRequested,
    UnitOpened,
    UnitOpenFailed,
    UnitOpenRequested,
    VerboseSet,
)
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE, update
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_ALREADY_LOADING_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
)
from synology_apm_repo.sdk.units.base import Node, NodeRole, RestorableUnit
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact
from synology_apm_repo.sdk.units.node_ref import NodeRef


async def _unread_content() -> bytes:
    raise AssertionError("this test never reads a unit's content")


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
    assert model.filter is not None

    new_model, _cmds = update(model, RootRequested(invalidate=False, force_raw=False))

    assert new_model.loaded == {}
    assert new_model.node_index == {}
    assert new_model.errors == {}
    assert new_model.filter is None
    assert new_model.root_error is None
    assert new_model.selected is None
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
    model = UnitModel(epoch=Epoch(1), inflight={PROVIDER_SLOT: RequestId(1)})
    root = _leaf("solo")
    new_model, _cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(1), root=root)
    )

    assert new_model.selected == root.ref


@pytest.mark.parametrize(
    "model_epoch",
    [
        pytest.param(2, id="closes_a_superseded_provider_instead_of_publishing_it"),
        pytest.param(1, id="dropped_on_a_request_mismatch_within_the_same_epoch"),
    ],
)
def test_stale_root_loaded_closes_its_provider(model_epoch: int) -> None:
    model = UnitModel(epoch=Epoch(model_epoch), inflight={PROVIDER_SLOT: RequestId(2)})
    new_model, cmds = update(
        model, RootLoaded(epoch=Epoch(1), request=RequestId(1), provider=ProviderHandle(9), root=_node())
    )

    assert new_model is model
    assert cmds == (CloseProvider(provider=ProviderHandle(9)),)


def test_root_load_failed_sets_root_error_when_current() -> None:
    """A current root failure is stored as ``root_error`` (shown in the detail pane), with no command."""
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


def test_children_requested_for_an_already_loaded_level_is_a_no_op() -> None:
    node = _node("folder")
    model = UnitModel(provider=ProviderHandle(3), loaded={node.ref: LoadedLevel(children=(), exhausted=True)})
    new_model, cmds = update(model, ChildrenRequested(node=node))

    assert new_model is model
    assert cmds == ()


def test_children_requested_while_its_fetch_is_still_pending_is_a_no_op() -> None:
    node = _node("folder")
    model = UnitModel(provider=ProviderHandle(3))
    model, _ = update(model, ChildrenRequested(node=node))
    new_model, cmds = update(model, ChildrenRequested(node=node))

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
    """A stale result leaves both ``loaded`` and ``pending`` untouched."""
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


# -- goto ---------------------------------------------------------------------

_ROOT = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
_PROVIDER = ProviderHandle(3)


def _resolving(target: NodeRef, *, root_expand_skipped: bool = False, **changes: object) -> UnitModel:
    """A loaded root with ``target``'s goto in flight as request 9."""
    fields: dict[str, object] = {
        "provider": _PROVIDER,
        "root": _ROOT,
        "selected": _ROOT.ref,
        "goto": GotoState(target=target, root_expand_skipped=root_expand_skipped),
        "inflight": {GOTO_SLOT: RequestId(9)},
        "next_request": RequestId(10),
        **changes,
    }
    return UnitModel(**fields)  # type: ignore[arg-type]


def test_goto_requested_before_the_root_loads_waits_for_it() -> None:
    target = NodeRef("repo", ("root", "x"))
    new_model, cmds = update(UnitModel(), GotoRequested(target=target))

    assert new_model.goto == GotoState(target=target)
    assert cmds == ()


def test_a_waiting_goto_is_resolved_when_the_root_loads_and_skips_its_auto_expand() -> None:
    target = NodeRef("repo", ("root", "x"))
    model = UnitModel(goto=GotoState(target=target), inflight={PROVIDER_SLOT: RequestId(1)}, next_request=RequestId(2))
    new_model, cmds = update(model, RootLoaded(epoch=Epoch(0), request=RequestId(1), provider=_PROVIDER, root=_ROOT))

    assert new_model.goto == GotoState(target=target, root_expand_skipped=True)
    assert new_model.inflight[GOTO_SLOT] == RequestId(2)
    assert cmds == (ResolveGoto(epoch=Epoch(0), request=RequestId(2), provider=_PROVIDER, target=target),)


def test_goto_requested_with_a_loaded_root_resolves_at_once() -> None:
    target = NodeRef("repo", ("root", "x"))
    model = UnitModel(provider=_PROVIDER, root=_ROOT, next_request=RequestId(5))
    new_model, cmds = update(model, GotoRequested(target=target))

    assert new_model.goto == GotoState(target=target)
    assert cmds == (ResolveGoto(epoch=Epoch(0), request=RequestId(5), provider=_PROVIDER, target=target),)


def test_a_reload_drops_a_goto_being_resolved_but_keeps_one_still_waiting() -> None:
    target = NodeRef("repo", ("root", "x"))
    resolving, _ = update(_resolving(target), RootRequested(invalidate=False, force_raw=False))
    waiting, _ = update(UnitModel(goto=GotoState(target=target)), RootRequested(invalidate=False, force_raw=False))

    assert resolving.goto is None
    assert waiting.goto == GotoState(target=target)


def test_goto_resolved_to_a_folder_loads_every_step_selects_and_lands_on_it() -> None:
    folder_a, folder_b = _node("a"), _node("b")
    sibling = _node("sibling")
    model = _resolving(folder_b.ref)
    new_model, cmds = update(
        model,
        GotoResolved(
            epoch=Epoch(0),
            request=RequestId(9),
            chain=(_ROOT, folder_a, folder_b),
            children_by_step=((folder_a, sibling), (folder_b,)),
        ),
    )

    assert new_model.loaded == {
        _ROOT.ref: LoadedLevel(children=(folder_a, sibling), exhausted=True),
        folder_a.ref: LoadedLevel(children=(folder_b,), exhausted=True),
    }
    assert new_model.selected == folder_b.ref
    assert new_model.goto is None
    assert new_model.landing == Landing(request=RequestId(9), folder=folder_b.ref)
    assert new_model.detail == DetailState(node=folder_b, body=DetailIdle())
    assert cmds == ()


def test_goto_resolved_to_a_leaf_selects_its_folder_and_previews_it() -> None:
    folder, target = _node("a"), _leaf("file")
    model = _resolving(target.ref)
    new_model, cmds = update(
        model,
        GotoResolved(
            epoch=Epoch(0), request=RequestId(9), chain=(_ROOT, folder, target), children_by_step=((folder,), (target,))
        ),
    )

    assert new_model.selected == folder.ref
    assert new_model.landing == Landing(request=RequestId(9), folder=folder.ref, leaf=target.ref, expand=True)
    assert cmds == (
        LoadPreview(
            epoch=Epoch(0), request=RequestId(10), provider=_PROVIDER, node=target, read_limit=PREVIEW_READ_LIMIT
        ),
    )


def test_goto_resolved_replaces_a_partly_loaded_level_and_leaves_others_alone() -> None:
    other_ref = NodeRef("repo", ("other",))
    other = LoadedLevel(children=(), exhausted=True)
    folder = _node("a")
    model = _resolving(
        folder.ref, loaded={_ROOT.ref: LoadedLevel(children=(_leaf("stale"),), exhausted=False), other_ref: other}
    )
    new_model, _ = update(
        model, GotoResolved(epoch=Epoch(0), request=RequestId(9), chain=(_ROOT, folder), children_by_step=((folder,),))
    )

    assert new_model.loaded[_ROOT.ref] == LoadedLevel(children=(folder,), exhausted=True)
    assert new_model.loaded[other_ref] is other


def test_goto_resolved_drops_a_late_page_of_a_step_it_loaded_completely() -> None:
    folder = _node("a")
    slot = children_slot(_ROOT.ref)
    model = _resolving(
        folder.ref,
        inflight={GOTO_SLOT: RequestId(9), slot: RequestId(4)},
        pending=frozenset({slot}),
        loaded={_ROOT.ref: LoadedLevel(children=(), exhausted=False)},
    )
    model, _ = update(
        model, GotoResolved(epoch=Epoch(0), request=RequestId(9), chain=(_ROOT, folder), children_by_step=((folder,),))
    )
    assert slot not in model.inflight
    assert slot not in model.pending

    new_model, cmds = update(
        model,
        MoreChildrenLoaded(epoch=Epoch(0), request=RequestId(4), ref=_ROOT.ref, more=(_leaf("x"),), exhausted=True),
    )
    assert new_model is model
    assert cmds == ()


def test_a_superseded_or_pre_reload_goto_result_is_dropped() -> None:
    folder = _node("a")
    resolved = {"chain": (_ROOT, folder), "children_by_step": ((folder,),)}
    model = _resolving(folder.ref)

    for epoch, request in ((Epoch(0), RequestId(8)), (Epoch(1), RequestId(9))):
        new_model, cmds = update(model, GotoResolved(epoch=epoch, request=request, **resolved))  # type: ignore[arg-type]
        assert new_model is model
        assert cmds == ()


def test_goto_not_found_warns_and_expands_a_root_it_left_collapsed() -> None:
    target = NodeRef("repo", ("root", "missing"))
    new_model, cmds = update(
        _resolving(target, root_expand_skipped=True), GotoNotFound(epoch=Epoch(0), request=RequestId(9))
    )

    assert new_model.goto is None
    assert new_model.landing == Landing(request=RequestId(9), folder=_ROOT.ref, expand=True)
    assert cmds == (Notify(message=GOTO_REF_NOT_FOUND_WARNING, severity="warning"),)


def test_a_goto_superseding_one_that_kept_the_root_collapsed_still_expands_it_on_failure() -> None:
    first = NodeRef("repo", ("root", "first"))
    model, cmds = update(_resolving(first, root_expand_skipped=True), GotoRequested(target=NodeRef("repo", ("x",))))
    assert model.goto is not None and model.goto.root_expand_skipped
    (resolve,) = cmds
    assert isinstance(resolve, ResolveGoto)

    new_model, _ = update(model, GotoNotFound(epoch=Epoch(0), request=resolve.request))

    assert new_model.landing == Landing(request=resolve.request, folder=_ROOT.ref, expand=True)


def test_a_failed_root_load_drops_a_waiting_goto() -> None:
    model = UnitModel(goto=GotoState(target=NodeRef("repo", ("x",))), inflight={PROVIDER_SLOT: RequestId(1)})
    new_model, _ = update(model, RootLoadFailed(epoch=Epoch(0), request=RequestId(1), message="boom"))

    assert new_model.goto is None
    assert new_model.root_error == "boom"


def test_goto_failed_warns_and_leaves_the_view_where_it_was() -> None:
    target = NodeRef("repo", ("root", "x"))
    new_model, cmds = update(_resolving(target), GotoFailed(epoch=Epoch(0), request=RequestId(9), message="boom"))

    assert new_model.goto is None
    assert new_model.landing is None
    assert cmds == (Notify(message="boom", severity="warning"),)


def test_goto_failed_after_the_root_was_left_collapsed_warns_and_expands_it() -> None:
    target = NodeRef("repo", ("root", "x"))
    new_model, cmds = update(
        _resolving(target, root_expand_skipped=True), GotoFailed(epoch=Epoch(0), request=RequestId(9), message="boom")
    )

    assert new_model.goto is None
    assert new_model.landing == Landing(request=RequestId(9), folder=_ROOT.ref, expand=True)
    assert cmds == (Notify(message="boom", severity="warning"),)


# -- opening a unit -------------------------------------------------------------


def test_unit_open_requested_opens_it_through_the_provider() -> None:
    leaf = _leaf("file")
    model = UnitModel(provider=_PROVIDER, next_request=RequestId(3))
    new_model, cmds = update(model, UnitOpenRequested(node=leaf, purpose=UnitPurpose.EXPORT))

    assert new_model.inflight[UNIT_OPEN_SLOT] == RequestId(3)
    assert cmds == (
        OpenUnit(epoch=Epoch(0), request=RequestId(3), provider=_PROVIDER, node=leaf, purpose=UnitPurpose.EXPORT),
    )


def test_unit_open_requested_without_a_provider_does_nothing() -> None:
    model = UnitModel()
    assert update(model, UnitOpenRequested(node=_leaf(), purpose=UnitPurpose.EXPORT)) == (model, ())


def test_only_the_latest_opened_unit_is_shown() -> None:
    unit = RestorableUnit(ref=_leaf().ref, name="leaf", is_leaf=True, content=LazyArtifact(_unread_content))
    model = UnitModel(provider=_PROVIDER, inflight={UNIT_OPEN_SLOT: RequestId(4)})

    _, cmds = update(
        model, UnitOpened(epoch=Epoch(0), request=RequestId(4), unit=unit, purpose=UnitPurpose.HEX_PREVIEW)
    )
    assert cmds == (ShowUnit(unit=unit, purpose=UnitPurpose.HEX_PREVIEW),)
    superseded = update(
        model, UnitOpened(epoch=Epoch(0), request=RequestId(3), unit=unit, purpose=UnitPurpose.HEX_PREVIEW)
    )
    assert superseded == (model, ())


def test_unit_open_failed_warns() -> None:
    model = UnitModel(inflight={UNIT_OPEN_SLOT: RequestId(4)})
    _, cmds = update(model, UnitOpenFailed(epoch=Epoch(0), request=RequestId(4), message="boom"))

    assert cmds == (Notify(message="boom", severity="warning"),)


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
    folder = _node("folder")
    model = UnitModel()
    new_model, cmds = update(model, FolderSelected(folder=folder))

    assert new_model.selected == folder.ref
    assert cmds == ()


def test_selecting_a_flat_category_fetches_its_children() -> None:
    category = Node(ref=NodeRef("repo", ("cat",)), name="cat", is_leaf=False, role=NodeRole.FLAT_CATEGORY)
    model = UnitModel(provider=_PROVIDER)
    new_model, cmds = update(model, FolderSelected(folder=category))

    assert new_model.selected == category.ref
    assert cmds == (
        LoadChildren(
            epoch=Epoch(0),
            request=RequestId(1),
            provider=_PROVIDER,
            node=category,
            offset=0,
            limit=CHILDREN_PAGE_SIZE,
        ),
    )


def test_selecting_a_list_overview_group_keeps_the_previous_folder() -> None:
    group = Node(ref=NodeRef("repo", ("list",)), name="list", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
    model = UnitModel(selected=_ROOT.ref)
    assert update(model, FolderSelected(folder=group)) == (model, ())


# -- detail pane ------------------------------------------------------------


def _list_group() -> Node:
    return Node(ref=NodeRef("repo", ("root", "list")), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)


def _live_model() -> UnitModel:
    return UnitModel(epoch=Epoch(2), provider=ProviderHandle(7), root=_node("root"))


def test_selecting_a_leaf_shows_it_loading_and_fetches_its_preview() -> None:
    leaf = _leaf("a")

    new_model, cmds = update(_live_model(), DetailRequested(node=leaf))

    assert new_model.detail == DetailState(node=leaf, body=DetailLoading())
    assert new_model.inflight[DETAIL_SLOT] == RequestId(1)
    assert cmds == (
        LoadPreview(
            epoch=Epoch(2), request=RequestId(1), provider=ProviderHandle(7), node=leaf, read_limit=PREVIEW_READ_LIMIT
        ),
    )


def test_selecting_a_list_overview_group_fetches_its_overview() -> None:
    group = _list_group()

    new_model, cmds = update(_live_model(), DetailRequested(node=group))

    assert new_model.detail == DetailState(node=group, body=DetailLoading())
    assert cmds == (
        LoadListOverview(
            epoch=Epoch(2),
            request=RequestId(1),
            provider=ProviderHandle(7),
            node=group,
            item_cap=LIST_OVERVIEW_ITEM_CAP,
            read_limit=PREVIEW_READ_LIMIT,
            max_concurrent=LIST_OVERVIEW_MAX_CONCURRENT,
        ),
    )


def test_selecting_an_ordinary_folder_fetches_nothing() -> None:
    folder = _node("dir")

    new_model, cmds = update(_live_model(), DetailRequested(node=folder))

    assert new_model.detail == DetailState(node=folder, body=DetailIdle())
    assert cmds == ()


def test_selecting_without_a_provider_fetches_nothing() -> None:
    leaf = _leaf("a")

    new_model, cmds = update(UnitModel(), DetailRequested(node=leaf))

    assert new_model.detail == DetailState(node=leaf, body=DetailIdle())
    assert cmds == ()


def test_a_later_selection_supersedes_an_earlier_fetch_even_with_no_fetch_of_its_own() -> None:
    model, _ = update(_live_model(), DetailRequested(node=_leaf("a")))

    model, cmds = update(model, DetailRequested(node=_node("dir")))

    assert cmds == (CancelDetailFetch(),)
    assert model.inflight[DETAIL_SLOT] == RequestId(2)
    assert not detail_loading(model, RequestId(1))


def test_detail_resolved_replaces_the_body_of_the_awaited_fetch() -> None:
    leaf = _leaf("a")
    model, _ = update(_live_model(), DetailRequested(node=leaf))

    new_model, cmds = update(model, DetailResolved(epoch=Epoch(2), request=RequestId(1), body=DetailPreview("hi")))

    assert new_model.detail == DetailState(node=leaf, body=DetailPreview("hi"))
    assert cmds == ()


def test_detail_resolved_for_a_superseded_request_is_dropped() -> None:
    model, _ = update(_live_model(), DetailRequested(node=_leaf("a")))
    model, _ = update(model, DetailRequested(node=_leaf("b")))

    new_model, cmds = update(model, DetailResolved(epoch=Epoch(2), request=RequestId(1), body=DetailPreview("late")))

    assert new_model == model
    assert cmds == ()


def test_detail_resolved_from_before_a_reset_is_dropped() -> None:
    model, _ = update(_live_model(), DetailRequested(node=_leaf("a")))
    model, _ = update(model, RootRequested(invalidate=False, force_raw=False))

    new_model, _ = update(model, DetailResolved(epoch=Epoch(2), request=RequestId(1), body=DetailError("x")))

    assert new_model == model


def test_detail_resolved_with_nothing_selected_is_dropped() -> None:
    model = UnitModel(epoch=Epoch(2), inflight={DETAIL_SLOT: RequestId(1)})

    new_model, _ = update(model, DetailResolved(epoch=Epoch(2), request=RequestId(1), body=DetailPreview("x")))

    assert new_model.detail is None


def test_a_reset_clears_the_detail() -> None:
    model, _ = update(_live_model(), DetailRequested(node=_leaf("a")))

    new_model, _ = update(model, RootRequested(invalidate=False, force_raw=False))

    assert new_model.detail is None


def test_verbose_set_updates_the_mirrored_flag() -> None:
    new_model, cmds = update(UnitModel(), VerboseSet(verbose=True))

    assert new_model.verbose is True
    assert cmds == ()


def test_a_selection_with_no_fetch_cancels_nothing_when_none_is_running() -> None:
    model, _ = update(_live_model(), DetailRequested(node=_node("dir")))

    _, cmds = update(model, DetailRequested(node=_node("other")))

    assert cmds == ()


def test_a_selection_that_fetches_supersedes_the_running_fetch_without_a_cancel_command() -> None:
    """A newer fetching selection issues only ``LoadPreview``; its exclusive worker group cancels the old fetch."""
    model, _ = update(_live_model(), DetailRequested(node=_leaf("a")))

    _, cmds = update(model, DetailRequested(node=_leaf("b")))

    assert [type(cmd) for cmd in cmds] == [LoadPreview]
