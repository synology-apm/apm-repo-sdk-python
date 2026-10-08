"""``ScreenHostApp``: the ``ApmRepoBrowserApp`` surface a screen under test
reads, for the test files that mount a real screen on a bare ``App``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from textual.app import App
from textual.screen import Screen

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.sdk.api import Repository, Session


class ScreenHostApp(App[None]):
    """``verbose``, ``jobs``, ``session``/``resources`` and
    ``repo_handle``/``current_repo``, then ``screens()`` pushed in order on
    mount. ``repo`` becomes the current repository. ``session`` defaults to
    a real, empty ``Session``, which does no I/O and no-ops its bookkeeping
    for a repository it never discovered; pass a placeholder when the test
    must prove nothing reaches it."""

    def __init__(self, *, repo: object | None = None, session: Session | None = None) -> None:
        super().__init__()
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.session = session if session is not None else Session()
        self.resources = ResourceTable(self.session)
        self.repo_handle: RepoHandle | None = (
            self.resources.put_repo(cast(Repository, repo)) if repo is not None else None
        )

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def screens(self) -> Sequence[Screen[Any]]:
        """The screens to push on mount, bottom first."""
        return ()

    def on_mount(self) -> None:
        for screen in self.screens():
            self.push_screen(screen)
