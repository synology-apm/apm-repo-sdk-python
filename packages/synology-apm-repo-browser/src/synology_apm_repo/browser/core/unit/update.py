"""``update(model, msg) -> (model, cmds)`` for ``UnitScreen``'s store --
pure, synchronous, exhaustive (``case _: assert_never(msg)``).

Every fetch-result case checks its ``epoch``/``request`` against the
model's current ones before touching anything else, and drops silently
on a mismatch -- a worker already past its last ``await`` when cancelled
still runs to completion and tries to publish."""

from __future__ import annotations

import dataclasses
from typing import assert_never

from synology_apm_repo.browser.core.keys import Epoch, filter_closed, filter_text_changed, is_stale
from synology_apm_repo.browser.core.keys import next_request as _next_request
from synology_apm_repo.browser.core.unit.cmd import CloseProvider, LoadChildren, LoadRoot, Notify, UnitCmd
from synology_apm_repo.browser.core.unit.model import (
    PROVIDER_SLOT,
    FilterState,
    LoadedLevel,
    UnitModel,
    children_slot,
    has_pending_children,
)
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
    UnitMsg,
)
from synology_apm_repo.browser.strings import (
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_ALREADY_LOADING_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
    UNIT_LOAD_MORE_NOTIFY,
)

#: One tree-node expansion loads at most this many children up front;
#: ``+`` (load-more) fetches the next page of the same size -- a real
#: SDK-side pagination pushdown (bounding actual I/O), not just deferred
#: widget construction.
CHILDREN_PAGE_SIZE = 500


def update(model: UnitModel, msg: UnitMsg) -> tuple[UnitModel, tuple[UnitCmd, ...]]:
    match msg:
        case RootRequested(invalidate=invalidate, force_raw=force_raw):
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
            )
            cmd = LoadRoot(epoch=new_epoch, request=request, invalidate=invalidate, force_raw=force_raw)
            return new_model, (*close_cmds, cmd)

        case RootLoaded(epoch=epoch, request=request, provider=provider, root=root):
            if is_stale(model.epoch, model.inflight, PROVIDER_SLOT, epoch, request):
                # Superseded by a later reset while this fetch was still in
                # flight -- this provider was never published anywhere, so
                # nothing else will ever close it unless this does.
                return model, (CloseProvider(provider=provider),)
            return dataclasses.replace(model, provider=provider, root=root, root_error=None, selected=root.ref), ()

        case RootLoadFailed(epoch=epoch, request=request, message=message):
            if is_stale(model.epoch, model.inflight, PROVIDER_SLOT, epoch, request):
                return model, ()
            # Rendered into the detail pane via `root_error`, not a toast
            # -- not returned as a Cmd here since the screen's own
            # subscription to this field handles the actual render.
            return dataclasses.replace(model, root_error=message), ()

        case ChildrenRequested(node=node):
            if has_pending_children(model, node.ref):
                # Defense in depth: the screen's own on_tree_node_expanded/
                # _select_folder_ref already guard this before dispatching,
                # but update() shouldn't rely on every future caller
                # remembering to check first.
                return model, ()
            slot = children_slot(node.ref)
            request, model = _next_request(model)
            new_model = dataclasses.replace(
                model, inflight={**model.inflight, slot: request}, pending=model.pending | {slot}
            )
            assert model.provider is not None  # a node is only ever expandable once the root/provider has loaded
            children_cmd = LoadChildren(
                epoch=model.epoch,
                request=request,
                provider=model.provider,
                node=node,
                offset=0,
                limit=CHILDREN_PAGE_SIZE,
            )
            return new_model, (children_cmd,)

        case ChildrenLoaded(epoch=epoch, request=request, ref=ref, children=children, exhausted=exhausted):
            if is_stale(model.epoch, model.inflight, children_slot(ref), epoch, request):
                # Leave `pending` untouched here -- it's either already been
                # reset wholesale by whatever invalidated this result
                # (RootRequested), or it's still correctly tracking a
                # genuinely newer request for this same slot, which will
                # clear it itself when *it* resolves. Clearing it for a
                # stale result unconditionally would wipe that newer
                # request's own marker out from under it.
                return model, ()
            level = LoadedLevel(children=children, exhausted=exhausted)
            errors = model.errors
            if ref in errors:
                errors = {k: v for k, v in errors.items() if k != ref}
            return (
                dataclasses.replace(
                    model,
                    loaded={**model.loaded, ref: level},
                    node_index={**model.node_index, **{child.ref: child for child in children}},
                    errors=errors,
                    pending=model.pending - {children_slot(ref)},
                ),
                (),
            )

        case ChildrenLoadFailed(epoch=epoch, request=request, ref=ref, message=message):
            if is_stale(model.epoch, model.inflight, children_slot(ref), epoch, request):
                # Leave `pending` untouched -- same reasoning as
                # ChildrenLoaded above.
                return model, ()
            # Shown as a synthetic error leaf under `ref` (see select.py's
            # own error_leaf_ref), not just a toast -- unlike
            # MoreChildrenLoadFailed below, `ref` has no real children on
            # screen yet for a toast to leave the user looking at.
            return (
                dataclasses.replace(
                    model, errors={**model.errors, ref: message}, pending=model.pending - {children_slot(ref)}
                ),
                (),
            )

        case LoadMoreRequested(node=node):
            existing_level = model.loaded.get(node.ref)
            if existing_level is None:
                return model, (Notify(message=UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING, severity="warning"),)
            if existing_level.exhausted:
                return model, (Notify(message=UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING, severity="warning"),)
            if has_pending_children(model, node.ref):
                return model, (Notify(message=UNIT_LOAD_MORE_ALREADY_LOADING_WARNING, severity="warning"),)
            slot = children_slot(node.ref)
            request, model = _next_request(model)
            new_model = dataclasses.replace(
                model, inflight={**model.inflight, slot: request}, pending=model.pending | {slot}
            )
            assert model.provider is not None
            more_cmd = LoadChildren(
                epoch=model.epoch,
                request=request,
                provider=model.provider,
                node=node,
                offset=existing_level.next_offset,
                limit=CHILDREN_PAGE_SIZE,
            )
            return new_model, (more_cmd,)

        case MoreChildrenLoaded(epoch=epoch, request=request, ref=ref, more=more, exhausted=exhausted):
            if is_stale(model.epoch, model.inflight, children_slot(ref), epoch, request):
                # Leave `pending` untouched -- same reasoning as
                # ChildrenLoaded above.
                return model, ()
            existing_level = model.loaded.get(ref)
            # `existing_level` is always present here: LoadMoreRequested's
            # own case only ever dispatches a fetch when model.loaded
            # already has `ref`'s entry, and nothing removes a key from
            # `loaded` short of a full RootRequested reset -- which the
            # is_stale() check above already catches via the epoch bump.
            base = existing_level.children if existing_level is not None else ()  # pragma: no cover - defensive only
            new_level = LoadedLevel(children=(*base, *more), exhausted=exhausted)
            new_model = dataclasses.replace(
                model,
                loaded={**model.loaded, ref: new_level},
                node_index={**model.node_index, **{child.ref: child for child in more}},
                pending=model.pending - {children_slot(ref)},
            )
            notify = Notify(message=UNIT_LOAD_MORE_NOTIFY.format(loaded=len(more), total=len(new_level.children)))
            return new_model, (notify,)

        case MoreChildrenLoadFailed(epoch=epoch, request=request, ref=ref, message=message):
            if is_stale(model.epoch, model.inflight, children_slot(ref), epoch, request):
                # Leave `pending` untouched -- same reasoning as
                # ChildrenLoaded above.
                return model, ()
            new_model = dataclasses.replace(model, pending=model.pending - {children_slot(ref)})
            return new_model, (Notify(message=message, severity="warning"),)

        case ChainStepResolved(ref=ref, children=children):
            # Always the full, exhaustive sibling list -- unconditionally
            # replaces whatever was there. Not epoch/request-gated: a goto
            # walk only runs against the currently-live provider. Also
            # clears `ref`'s children slot from `inflight`/`pending`, so an
            # ordinary fetch still in flight from before this walk step
            # can't land afterwards and corrupt this exhaustive list.
            level = LoadedLevel(children=children, exhausted=True)
            slot = children_slot(ref)
            inflight = (
                {k: v for k, v in model.inflight.items() if k != slot} if slot in model.inflight else model.inflight
            )
            return (
                dataclasses.replace(
                    model,
                    loaded={**model.loaded, ref: level},
                    node_index={**model.node_index, **{child.ref: child for child in children}},
                    inflight=inflight,
                    pending=model.pending - {slot},
                ),
                (),
            )

        case FilterOpened(ref=ref):
            return dataclasses.replace(model, filter=FilterState(ref=ref)), ()

        case FilterTextChanged(text=text):
            return (
                filter_text_changed(model, lambda m: m.filter, lambda m, s: dataclasses.replace(m, filter=s), text),
                (),
            )

        case FilterClosed():
            return filter_closed(model, lambda m: m.filter, lambda m: dataclasses.replace(m, filter=None)), ()

        case FolderSelected(ref=ref):
            return dataclasses.replace(model, selected=ref), ()

        case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
            assert_never(msg)
