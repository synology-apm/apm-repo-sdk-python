"""``CompositeSaasProvider``: several already-built SaaS sub-providers
behind one tree, built by ``units/dispatch.py``'s ``saas_provider_for``.
"""

from __future__ import annotations

import dataclasses
from typing import override

from ..._util.closing import AsyncClosing, close_all
from ...catalog.version import Version
from ...dedup.repository import DedupRepo
from ...units.provider_kit import not_restorable, paginate
from ..base import ClosableUnitProvider, Node, RestorableUnit
from ..node_ref import NodeRef, canonical_ref_for


@dataclasses.dataclass(frozen=True, slots=True)
class _Tagged:
    """``Node.handle`` of a composite node: the owning sub-provider's tag
    and that sub-provider's own handle for it."""

    tag: str
    inner: object


_ROOT = _Tagged("", None)


class CompositeSaasProvider(AsyncClosing):
    """``UnitProvider`` for a version more than one sub-provider recognizes
    (M365 ``USER_EXCHANGE``/``GROUP_EXCHANGE``: Mail, Contacts, Calendars
    and Archive Mail side by side). A synthetic ``"Exchange"`` root lists
    each sub-provider's root; deeper calls route to the owning
    sub-provider.

    Every ``Node`` handed out has its ``ref`` prefixed with the
    sub-provider's tag and its ``handle`` wrapped as ``_Tagged``, unwrapped
    again before the node reaches that sub-provider, so a ref round-tripped
    through a string still resolves. Closing closes every sub-provider."""

    def __init__(
        self,
        repo: DedupRepo,
        version: Version,
        sub_providers: dict[str, ClosableUnitProvider],
    ) -> None:
        self._repo = repo
        self._version = version
        self._sub_providers = sub_providers

    @override
    async def close(self) -> None:
        await close_all(
            [p.close for p in self._sub_providers.values()],
            "CompositeSaasProvider.close() failed to close every sub-provider",
        )

    def _ref_for(self, extra: tuple[str, ...]) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    def _tag_node(self, node: Node, tag: str) -> Node:
        return dataclasses.replace(
            node, ref=self._ref_for((tag, *node.ref.extra_segments)), handle=_Tagged(tag, node.handle)
        )

    def _route(self, node: Node) -> tuple[str, ClosableUnitProvider, Node] | None:
        """The owning sub-provider and the node as it built it, or ``None``
        for the root or a node no sub-provider owns."""
        handle = node.handle
        if not isinstance(handle, _Tagged) or handle is _ROOT:
            return None
        provider = self._sub_providers.get(handle.tag)
        if provider is None:
            return None
        return handle.tag, provider, dataclasses.replace(node, handle=handle.inner)

    def root(self) -> Node:
        return Node(ref=self._ref_for(()), name="Exchange", is_leaf=False, handle=_ROOT)

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if node.handle is _ROOT:
            tops = [self._tag_node(provider.root(), tag) for tag, provider in self._sub_providers.items()]
            return paginate(tops, offset, limit)
        routed = self._route(node)
        if routed is None:
            return []
        tag, provider, sub_node = routed
        return [self._tag_node(child, tag) for child in await provider.children(sub_node, offset, limit)]

    async def unit(self, node: Node) -> RestorableUnit:
        routed = self._route(node)
        if routed is None:
            not_restorable("node", node.name)
        _tag, provider, sub_node = routed
        # The sub-provider's unit carries its own untagged ref; keep the listed one.
        return dataclasses.replace(await provider.unit(sub_node), ref=node.ref)
