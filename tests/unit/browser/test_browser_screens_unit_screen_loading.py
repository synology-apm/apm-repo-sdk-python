"""UnitScreen tree loading: a provider error at the root."""

from __future__ import annotations

from textual.widgets import Static

from support.model_factories import make_version
from support.pilot import wait_until
from synology_apm_repo.sdk.errors import ApmRepoError
from unit.browser.unit_screen_fakes import (
    FakeApp,
    FakeRepo,
)


async def test_load_root_provider_error_shows_in_the_detail_pane() -> None:
    repo = FakeRepo(None, provider_error=ApmRepoError("repo is locked"))
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        detail = app.screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert "repo is locked" in str(detail.render())
