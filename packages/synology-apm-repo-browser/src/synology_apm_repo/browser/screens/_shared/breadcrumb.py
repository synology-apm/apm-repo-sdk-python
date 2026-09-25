"""Shared breadcrumb-text builder."""

from __future__ import annotations

from rich.text import Text

from synology_apm_repo.sdk.presentation.format import pluralize


def _breadcrumb_with_tasks_hint(text: str, job_count: int, width: int) -> str:
    """``text`` unchanged when there are no background jobs (or ``width``
    is too narrow to fit both parts without overlap -- showing nothing
    extra beats a garbled line). With at least one job and room to
    spare, ``text`` padded out to a right-aligned "N Task(s) (t)" suffix
    within ``width``, so a background export's own running-job count
    stays visible without costing the screen a whole extra line above
    the footer.

    Stays a plain ``str`` (not a multi-column Rich renderable) on
    purpose: ``Static.update()`` re-parses a ``str`` as markup exactly
    like every other breadcrumb write already relies on, and
    ``Static.render()`` -- what every existing breadcrumb test already
    inspects via ``str(...)`` -- only stringifies sensibly for a plain
    string/``Content``, not an arbitrary Rich renderable (a
    ``rich.table.Table`` has no ``__str__`` of its own). ``width`` is
    the ``#breadcrumb`` widget's own *usable* (padding already
    subtracted) width, computed by the caller from the *screen's* own
    size -- valid immediately from ``on_mount`` onward, unlike the
    widget's own ``.size``, which stays ``(0, 0)`` until the first real
    layout pass completes. No key binding lives here -- ``t``
    (``toggle_worklist``) is unchanged, already reachable via
    ``keymap.py``'s own ``WORKLIST_BINDING``; this is presentation
    only."""
    if not job_count:
        return text
    hint = f"{job_count} {pluralize(job_count, 'Task')} (t)"
    # text can carry real markup (e.g. _set_loading_indicator's own
    # "Loading" suffix) -- measuring its *plain* length, not len(text)
    # itself, is what keeps the padding correct either way.
    plain_length = len(Text.from_markup(text).plain)
    gap = width - plain_length - len(hint)
    if gap < 1:
        return text
    return f"{text}{' ' * gap}{hint}"
