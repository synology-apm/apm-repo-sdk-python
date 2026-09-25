"""``CompositeSaasProvider``: bundles more than one already-built
sub-provider behind one dispatch surface — composition, not one more
workload built on ``provider.py``'s own ``SaasWorkloadProvider`` base
(see the class docstring for the shape this covers). See
``units/dispatch.py`` for its one construction site.
"""

from __future__ import annotations

import dataclasses
from types import TracebackType
from typing import Self

from ...catalog.version import Version
from ...dedup.repository import DedupRepo
from ..base import ClosableUnitProvider, Node, RestorableUnit, not_restorable, paginate
from ..node_ref import NodeRef, canonical_ref_for


class CompositeSaasProvider:
    """``UnitProvider`` for the one shape no single-app provider covers:
    M365's ``USER_EXCHANGE``/
    ``GROUP_EXCHANGE`` sub_types bundle Mail, Contacts and Calendars as
    three independently-populated services within the same version, all
    reachable at once — not alternatives to pick between. Every other
    SaaS sub_type maps 1:1 to one provider; this class is what a caller
    gets instead when more than one sub-provider recognizes the same
    version: a synthetic root (``"Exchange"``) whose children are each
    sub-provider's own root as siblings, with every deeper
    ``children()``/``unit()`` call routed back to whichever
    sub-provider owns that node.

    **Ref/key prefixing, not object identity**: every ``Node`` handed
    out gets ``attrs["key"]`` rewritten to ``(tag, *original_key)``
    (and its ``ref`` rebuilt to match), with the ``tag`` segment
    stripped back off before a node reaches the sub-provider that built
    it — a ``NodeRef`` round-tripped through ``str()``/``parse()``
    carries this prefix as an ordinary extra segment, so it resolves
    correctly on a fresh lookup like any other multi-segment ref."""

    def __init__(
        self,
        repo: DedupRepo,
        version: Version,
        sub_providers: dict[str, ClosableUnitProvider],
    ) -> None:
        self._repo = repo
        self._version = version
        self._sub_providers = sub_providers

    async def close(self) -> None:
        for provider in self._sub_providers.values():
            await provider.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def _ref_for(self, extra: tuple[str, ...]) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    def _tag_node(self, node: Node, tag: str, rest: tuple[str, ...]) -> Node:
        key = (tag, *rest)
        return dataclasses.replace(node, ref=self._ref_for(key), attrs={**node.attrs, "key": key})

    def root(self) -> Node:
        return Node(ref=self._ref_for(()), name="Exchange", is_leaf=False, attrs={"key": ()})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        key = tuple(node.attrs.get("key", ()))
        if key == ():
            tops = [self._tag_node(provider.root(), tag, ()) for tag, provider in self._sub_providers.items()]
            return paginate(tops, offset, limit)
        tag, *rest = key
        provider = self._sub_providers.get(tag)
        if provider is None:
            return []
        sub_node = dataclasses.replace(node, attrs={**node.attrs, "key": tuple(rest)})
        children = await provider.children(sub_node, offset, limit)
        return [self._tag_node(child, tag, tuple(child.attrs.get("key", ()))) for child in children]

    async def unit(self, node: Node) -> RestorableUnit:
        key = tuple(node.attrs.get("key", ()))
        if not key:
            not_restorable("node", node.name)
        tag, *rest = key
        provider = self._sub_providers[tag]
        sub_node = dataclasses.replace(node, attrs={**node.attrs, "key": tuple(rest)})
        return await provider.unit(sub_node)
