"""``load_list_overview``: a SharePoint List's items for the
spreadsheet-style overview, read by the SDK's ``read_site_list_items``."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.content_preview import visible_site_fields
from synology_apm_repo.sdk import Node, UnitProvider, read_site_list_items


@dataclasses.dataclass(frozen=True, slots=True)
class ListOverview:
    """A List's items as display rows.

    Attributes:
        rows: One ``visible_site_fields`` dict per readable item, in the
            order ``provider.children()`` returned them.
        truncated: The List has at least as many items as were fetched, so
            the overview may not show all of them.
    """

    rows: list[dict[str, object]]
    truncated: bool


async def load_list_overview(
    provider: UnitProvider, node: Node, *, item_cap: int, read_limit: int, max_concurrent: int
) -> ListOverview:
    """``read_site_list_items`` with each item reduced to the fields worth
    showing (``visible_site_fields``).

    Raises:
        ApmRepoError: Listing the List's items failed.
    """
    items = await read_site_list_items(
        provider, node, item_cap=item_cap, read_limit=read_limit, max_concurrent=max_concurrent
    )
    return ListOverview(rows=[visible_site_fields(row) for row in items.rows], truncated=items.truncated)
