"""Tree shapes for SaaS application-layer providers. ``TreeStrategy`` is
the interface ``SaasWorkloadProvider`` drives; every key is an opaque
ref-segment tuple, ``()`` being the provider root.

- ``SyntheticGroupedTree``: a flat table grouped by one column, or not at
  all (Contact, GWS Mail, M365 Mail's fallback).
- ``NamedGroupFlatTree``: groups from their own table, flat leaves
  (Calendar).
- ``RecursiveTree``: parent-pointer recursion over one table (Drive).
- ``NamedGroupRecursiveTree``: groups from their own table, leaves
  recursing by parent pointer (Site).
- ``RecursiveGroupFlatTree``: a recursive group table, flat leaves (M365
  Mail's folders).
- ``CategorizedGroupTree``: a synthetic category level over another
  tree's top-level entries (Calendar, Site, Teams channels).
"""

from __future__ import annotations

from ._base import FolderPredicate, Key, Row, SupportsTable, TreeEntry, TreeStrategy
from .categorized import CategorizedGroupTree, categories_present_in_order
from .named_group_flat import NamedGroupFlatTree
from .named_group_recursive import NamedGroupRecursiveTree
from .recursive import RecursiveTree
from .recursive_group_flat import RecursiveGroupFlatTree
from .synthetic_grouped import SyntheticGroupedTree

__all__ = [
    "CategorizedGroupTree",
    "FolderPredicate",
    "Key",
    "NamedGroupFlatTree",
    "NamedGroupRecursiveTree",
    "RecursiveGroupFlatTree",
    "RecursiveTree",
    "Row",
    "SupportsTable",
    "SyntheticGroupedTree",
    "TreeEntry",
    "TreeStrategy",
    "categories_present_in_order",
]
