"""Shared breadcrumb-text builder."""

from __future__ import annotations

from rich.text import Text

from synology_apm_repo.sdk.presentation import pluralize


def breadcrumb_with_tasks_hint(text: str, job_count: int, width: int) -> str:
    """``text`` unchanged when there are no background jobs or ``width`` is
    too narrow for both parts. Otherwise ``text`` padded to a right-aligned
    "N Tasks (t)" suffix within ``width``.

    ``width`` is the ``#breadcrumb`` widget's usable width, padding
    subtracted."""
    if not job_count:
        return text
    hint = f"{job_count} {pluralize(job_count, 'Task')} (t)"
    # text can carry markup, so measure its plain length.
    plain_length = len(Text.from_markup(text).plain)
    gap = width - plain_length - len(hint)
    if gap < 1:
        return text
    return f"{text}{' ' * gap}{hint}"
