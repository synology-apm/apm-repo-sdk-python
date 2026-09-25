"""Shared tree-expansion helpers for SaaS application-layer
providers. Every service-level DB in this project's real schemas
expands into a browsable tree one of two shapes: parent-pointer
recursion (``parent_folder_id`` + a root id — Drive, Site's document
libraries, Contact folders) or a flat list, optionally grouped by one
key (Calendar events by ``calendar_id``, Mail by folder, Site's lists
ungrouped). FS's own third strategy (absolute-path lookup) is
FS-specific and lives in ``units/fs.py``, not duplicated here.

``TreeStrategy`` is the interface ``SaasWorkloadProvider``'s
``children()``/``unit()`` drive: ``children_of`` is ``async`` — every
call is a real one-page SQL fetch, never a full-table scan — while
``row_for`` stays synchronous, a lookup into the per-key cache
``children_of`` populates as it goes; every key is an opaque ref-segment tuple, ``()``
meaning the provider root. Its concrete implementations —
``SyntheticGroupedTree`` (Contact, and M365 Mail's own degrade path,
``synthetic_grouped.py``), ``NamedGroupFlatTree`` (Calendar,
``named_group_flat.py``), ``RecursiveTree`` (Drive, ``recursive.py``),
``NamedGroupRecursiveTree`` (Site, ``named_group_recursive.py``),
``RecursiveGroupFlatTree`` (M365 Mail's real folder hierarchy,
``recursive_group_flat.py``), and ``CategorizedGroupTree`` (a further
synthetic split wrapped around any of the above whose own root lists
named groups, ``categorized.py``) — cover exactly those shapes. The shared plumbing every
one of them builds on (the ``TreeStrategy`` protocol itself, the
lazy-table/``ORDER BY``/leaf-listing helpers) lives in ``_base.py``.
"""

from __future__ import annotations

from ._base import FolderPredicate, TreeStrategy
from .categorized import CategorizedGroupTree, categories_present_in_order
from .named_group_flat import NamedGroupFlatTree
from .named_group_recursive import NamedGroupRecursiveTree
from .recursive import RecursiveTree
from .recursive_group_flat import RecursiveGroupFlatTree
from .synthetic_grouped import SyntheticGroupedTree

__all__ = [
    "CategorizedGroupTree",
    "FolderPredicate",
    "NamedGroupFlatTree",
    "NamedGroupRecursiveTree",
    "RecursiveGroupFlatTree",
    "RecursiveTree",
    "SyntheticGroupedTree",
    "TreeStrategy",
    "categories_present_in_order",
]
