"""``update(model, msg) -> (model, cmds)`` for ``UnitScreen``'s store.

Every fetch result is checked against the model's current
``epoch``/``request`` first and dropped on a mismatch: a worker already past
its last ``await`` when cancelled still publishes."""

from __future__ import annotations

import dataclasses
from typing import assert_never

from synology_apm_repo.browser.core.keys import Epoch, RequestId, filter_closed, filter_text_changed, is_stale
from synology_apm_repo.browser.core.keys import next_request as _next_request
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
    UnitCmd,
)
from synology_apm_repo.browser.core.unit.model import (
    DETAIL_SLOT,
    GOTO_SLOT,
    LIST_OVERVIEW_ITEM_CAP,
    LIST_OVERVIEW_MAX_CONCURRENT,
    PREVIEW_READ_LIMIT,
    PROVIDER_SLOT,
    UNIT_OPEN_SLOT,
    DetailIdle,
    DetailLoading,
    DetailState,
    FilterState,
    GotoState,
    Landing,
    LoadedLevel,
    UnitModel,
    children_slot,
    has_pending_children,
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
    UnitMsg,
    UnitOpened,
    UnitOpenFailed,
    UnitOpenRequested,
    VerboseSet,
)
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_ALREADY_LOADING_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
    UNIT_LOAD_MORE_NOTIFY,
)
from synology_apm_repo.sdk import Node, NodeRole

#: Children one expansion, or one ``+`` load-more, fetches from the SDK.
CHILDREN_PAGE_SIZE = 500


_Result = tuple[UnitModel, tuple[UnitCmd, ...]]
"""What ``update`` and each ``_on_*`` handler return: the next model and the commands to run."""


def _on_root_requested(model: UnitModel, msg: RootRequested) -> _Result:
    new_epoch = Epoch(model.epoch + 1)
    request, model = _next_request(model)
    close_cmds = (CloseProvider(provider=model.provider),) if model.provider is not None else ()
    new_model = dataclasses.replace(
        model,
        epoch=new_epoch,
        inflight={PROVIDER_SLOT: request},
        pending=frozenset(),
        provider=None,
        root=None,
        root_error=None,
        loaded={},
        node_index={},
        errors={},
        filter=None,
        selected=None,
        detail=None,
        # A goto waiting for the root still waits; one being resolved read
        # the provider this load replaces.
        goto=model.goto if model.goto is not None and model.provider is None else None,
        landing=None,
    )
    cmd = LoadRoot(epoch=new_epoch, request=request, invalidate=msg.invalidate, force_raw=msg.force_raw)
    return new_model, (*close_cmds, cmd)


def _on_root_loaded(model: UnitModel, msg: RootLoaded) -> _Result:
    if is_stale(model.epoch, model.inflight, PROVIDER_SLOT, msg.epoch, msg.request):
        # Superseded: nothing else holds this provider, so close it here.
        return model, (CloseProvider(provider=msg.provider),)
    model = dataclasses.replace(model, provider=msg.provider, root=msg.root, root_error=None, selected=msg.root.ref)
    if model.goto is None:
        return model, ()
    # The view sees the pending goto as the root renders and leaves the
    # root collapsed for the landing.
    return _start_goto(model, GotoState(target=model.goto.target, root_expand_skipped=True))


def _on_children_requested(model: UnitModel, msg: ChildrenRequested) -> _Result:
    if msg.node.ref in model.loaded or has_pending_children(model, msg.node.ref):
        # Already fetched (possibly empty) or in flight; callers re-request
        # freely.
        return model, ()
    slot = children_slot(msg.node.ref)
    request, model = _next_request(model)
    new_model = dataclasses.replace(model, inflight={**model.inflight, slot: request}, pending=model.pending | {slot})
    assert model.provider is not None  # a node is only ever expandable once the root/provider has loaded
    children_cmd = LoadChildren(
        epoch=model.epoch,
        request=request,
        provider=model.provider,
        node=msg.node,
        offset=0,
        limit=CHILDREN_PAGE_SIZE,
    )
    return new_model, (children_cmd,)


def _on_children_loaded(model: UnitModel, msg: ChildrenLoaded) -> _Result:
    if is_stale(model.epoch, model.inflight, children_slot(msg.ref), msg.epoch, msg.request):
        # `pending` is left alone: it was reset by RootRequested or belongs
        # to the newer request for this slot.
        return model, ()
    level = LoadedLevel(children=msg.children, exhausted=msg.exhausted)
    errors = model.errors
    if msg.ref in errors:
        errors = {k: v for k, v in errors.items() if k != msg.ref}
    return (
        dataclasses.replace(
            model,
            loaded={**model.loaded, msg.ref: level},
            node_index={**model.node_index, **{child.ref: child for child in msg.children}},
            errors=errors,
            pending=model.pending - {children_slot(msg.ref)},
        ),
        (),
    )


def _on_children_load_failed(model: UnitModel, msg: ChildrenLoadFailed) -> _Result:
    if is_stale(model.epoch, model.inflight, children_slot(msg.ref), msg.epoch, msg.request):
        return model, ()
    # Shown as an error leaf under `ref` (select.py's error_leaf_ref); a
    # load-more failure, with children already on screen, is a toast.
    return (
        dataclasses.replace(
            model, errors={**model.errors, msg.ref: msg.message}, pending=model.pending - {children_slot(msg.ref)}
        ),
        (),
    )


def _on_load_more_requested(model: UnitModel, msg: LoadMoreRequested) -> _Result:
    existing_level = model.loaded.get(msg.node.ref)
    if existing_level is None:
        return model, (Notify(message=UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING, severity="warning"),)
    if existing_level.exhausted:
        return model, (Notify(message=UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING, severity="warning"),)
    if has_pending_children(model, msg.node.ref):
        return model, (Notify(message=UNIT_LOAD_MORE_ALREADY_LOADING_WARNING, severity="warning"),)
    slot = children_slot(msg.node.ref)
    request, model = _next_request(model)
    new_model = dataclasses.replace(model, inflight={**model.inflight, slot: request}, pending=model.pending | {slot})
    assert model.provider is not None
    more_cmd = LoadChildren(
        epoch=model.epoch,
        request=request,
        provider=model.provider,
        node=msg.node,
        offset=existing_level.next_offset,
        limit=CHILDREN_PAGE_SIZE,
    )
    return new_model, (more_cmd,)


def _on_more_children_loaded(model: UnitModel, msg: MoreChildrenLoaded) -> _Result:
    if is_stale(model.epoch, model.inflight, children_slot(msg.ref), msg.epoch, msg.request):
        return model, ()
    existing_level = model.loaded.get(msg.ref)
    # Always present: only a RootRequested reset removes it, and is_stale()
    # catches that.
    base = existing_level.children if existing_level is not None else ()  # pragma: no cover - defensive only
    new_level = LoadedLevel(children=(*base, *msg.more), exhausted=msg.exhausted)
    new_model = dataclasses.replace(
        model,
        loaded={**model.loaded, msg.ref: new_level},
        node_index={**model.node_index, **{child.ref: child for child in msg.more}},
        pending=model.pending - {children_slot(msg.ref)},
    )
    notify = Notify(message=UNIT_LOAD_MORE_NOTIFY.format(loaded=len(msg.more), total=len(new_level.children)))
    return new_model, (notify,)


def _start_goto(model: UnitModel, goto: GotoState) -> _Result:
    request, model = _next_request(model)
    assert model.provider is not None
    resolve = ResolveGoto(epoch=model.epoch, request=request, provider=model.provider, target=goto.target)
    return dataclasses.replace(model, goto=goto, inflight={**model.inflight, GOTO_SLOT: request}), (resolve,)


def _on_goto_requested(model: UnitModel, msg: GotoRequested) -> _Result:
    if model.provider is None:
        # Resolved by _on_root_loaded.
        return dataclasses.replace(model, goto=GotoState(target=msg.target)), ()
    # A goto superseding one that kept the root collapsed inherits that, so
    # if it fails in turn the root still gets expanded.
    skipped = model.goto is not None and model.goto.root_expand_skipped
    return _start_goto(model, GotoState(target=msg.target, root_expand_skipped=skipped))


def _select_folder(model: UnitModel, folder: Node) -> _Result:
    """``FolderSelected``'s effect (see there), also a goto landing's."""
    if folder.role is NodeRole.LIST_OVERVIEW:
        return model, ()
    model = dataclasses.replace(model, selected=folder.ref)
    if folder.role is NodeRole.FLAT_CATEGORY:
        return _on_children_requested(model, ChildrenRequested(node=folder))
    return model, ()


def _on_goto_resolved(model: UnitModel, msg: GotoResolved) -> _Result:
    if is_stale(model.epoch, model.inflight, GOTO_SLOT, msg.epoch, msg.request):
        return model, ()
    # Each step's complete child list replaces whatever it had loaded; its
    # children slot is cleared so an older, paginated fetch's result drops.
    steps = {step.ref: children for step, children in zip(msg.chain, msg.children_by_step, strict=False)}
    slots = {children_slot(ref) for ref in steps}
    model = dataclasses.replace(
        model,
        loaded={**model.loaded, **{ref: LoadedLevel(children=c, exhausted=True) for ref, c in steps.items()}},
        node_index={**model.node_index, **{child.ref: child for c in steps.values() for child in c}},
        inflight={k: v for k, v in model.inflight.items() if k not in slots},
        pending=model.pending - slots,
        goto=None,
    )
    target = msg.chain[-1]
    # A leaf shows as a file-table row of its parent folder, expanded like
    # every other step whose children the goto loaded.
    folder = msg.chain[-2] if target.is_leaf and len(msg.chain) > 1 else target
    landing = (
        Landing(request=msg.request, folder=folder.ref)
        if folder is target
        else Landing(request=msg.request, folder=folder.ref, leaf=target.ref, expand=True)
    )
    model, select_cmds = _select_folder(dataclasses.replace(model, landing=landing), folder)
    model, detail_cmds = _on_detail_requested(model, DetailRequested(node=target))
    return model, (*select_cmds, *detail_cmds)


def _goto_ended_unlanded(model: UnitModel, request: RequestId, notify: Notify) -> _Result:
    """A goto that found nothing to land on; a root it left collapsed is
    expanded after all."""
    goto = model.goto
    landing = model.landing
    if goto is not None and goto.root_expand_skipped and model.root is not None:
        landing = Landing(request=request, folder=model.root.ref, expand=True)
    return dataclasses.replace(model, goto=None, landing=landing), (notify,)


def _on_unit_open_requested(model: UnitModel, msg: UnitOpenRequested) -> _Result:
    provider = model.provider
    if provider is None:
        return model, ()
    request, model = _next_request(model)
    open_cmd = OpenUnit(epoch=model.epoch, request=request, provider=provider, node=msg.node, purpose=msg.purpose)
    return dataclasses.replace(model, inflight={**model.inflight, UNIT_OPEN_SLOT: request}), (open_cmd,)


def _on_detail_requested(model: UnitModel, msg: DetailRequested) -> _Result:
    was_loading = model.detail is not None and isinstance(model.detail.body, DetailLoading)
    request, model = _next_request(model)
    inflight = {**model.inflight, DETAIL_SLOT: request}
    handle = model.provider
    fetch: LoadPreview | LoadListOverview
    fetches = msg.node.is_leaf or (msg.node.role is NodeRole.LIST_OVERVIEW)
    if handle is None or not fetches:
        detail = DetailState(node=msg.node, body=DetailIdle())
        cancel = (CancelDetailFetch(),) if was_loading else ()
        return dataclasses.replace(model, inflight=inflight, detail=detail), cancel
    if msg.node.is_leaf:
        fetch = LoadPreview(
            epoch=model.epoch, request=request, provider=handle, node=msg.node, read_limit=PREVIEW_READ_LIMIT
        )
    else:
        fetch = LoadListOverview(
            epoch=model.epoch,
            request=request,
            provider=handle,
            node=msg.node,
            item_cap=LIST_OVERVIEW_ITEM_CAP,
            read_limit=PREVIEW_READ_LIMIT,
            max_concurrent=LIST_OVERVIEW_MAX_CONCURRENT,
        )
    detail = DetailState(node=msg.node, body=DetailLoading())
    return dataclasses.replace(model, inflight=inflight, detail=detail), (fetch,)


def update(model: UnitModel, msg: UnitMsg) -> _Result:
    match msg:
        case RootRequested():
            return _on_root_requested(model, msg)
        case RootLoaded():
            return _on_root_loaded(model, msg)
        case RootLoadFailed(epoch=epoch, request=request, message=message):
            if is_stale(model.epoch, model.inflight, PROVIDER_SLOT, epoch, request):
                return model, ()
            # Shown by the detail pane (detail_view), not a toast. A goto
            # waiting for this root is dropped rather than firing after some
            # later reload.
            return dataclasses.replace(model, root_error=message, goto=None), ()
        case ChildrenRequested():
            return _on_children_requested(model, msg)
        case ChildrenLoaded():
            return _on_children_loaded(model, msg)
        case ChildrenLoadFailed():
            return _on_children_load_failed(model, msg)
        case LoadMoreRequested():
            return _on_load_more_requested(model, msg)
        case MoreChildrenLoaded():
            return _on_more_children_loaded(model, msg)
        case MoreChildrenLoadFailed(epoch=epoch, request=request, ref=ref, message=message):
            if is_stale(model.epoch, model.inflight, children_slot(ref), epoch, request):
                return model, ()
            new_model = dataclasses.replace(model, pending=model.pending - {children_slot(ref)})
            return new_model, (Notify(message=message, severity="warning"),)
        case GotoRequested():
            return _on_goto_requested(model, msg)
        case GotoResolved():
            return _on_goto_resolved(model, msg)
        case GotoNotFound(epoch=epoch, request=request):
            if is_stale(model.epoch, model.inflight, GOTO_SLOT, epoch, request):
                return model, ()
            return _goto_ended_unlanded(model, request, Notify(message=GOTO_REF_NOT_FOUND_WARNING, severity="warning"))
        case GotoFailed(epoch=epoch, request=request, message=message):
            if is_stale(model.epoch, model.inflight, GOTO_SLOT, epoch, request):
                return model, ()
            return _goto_ended_unlanded(model, request, Notify(message=message, severity="warning"))
        case UnitOpenRequested():
            return _on_unit_open_requested(model, msg)
        case UnitOpened(epoch=epoch, request=request, unit=unit, purpose=purpose):
            if is_stale(model.epoch, model.inflight, UNIT_OPEN_SLOT, epoch, request):
                return model, ()
            return model, (ShowUnit(unit=unit, purpose=purpose),)
        case UnitOpenFailed(epoch=epoch, request=request, message=message):
            if is_stale(model.epoch, model.inflight, UNIT_OPEN_SLOT, epoch, request):
                return model, ()
            return model, (Notify(message=message, severity="warning"),)
        case FilterOpened(ref=ref):
            return dataclasses.replace(model, filter=FilterState(ref=ref)), ()
        case FilterTextChanged(text=text):
            return (
                filter_text_changed(model, lambda m: m.filter, lambda m, s: dataclasses.replace(m, filter=s), text),
                (),
            )
        case FilterClosed():
            return filter_closed(model, lambda m: m.filter, lambda m: dataclasses.replace(m, filter=None)), ()
        case FolderSelected(folder=folder):
            return _select_folder(model, folder)
        case DetailRequested():
            return _on_detail_requested(model, msg)
        case DetailResolved(epoch=epoch, request=request, body=body):
            if is_stale(model.epoch, model.inflight, DETAIL_SLOT, epoch, request) or model.detail is None:
                return model, ()
            return dataclasses.replace(model, detail=DetailState(node=model.detail.node, body=body)), ()
        case VerboseSet(verbose=verbose):
            return dataclasses.replace(model, verbose=verbose), ()
        case _:
            assert_never(msg)
