"""Connection / workload / version: cheap SQLite reads that never touch
Pool/Composition bytes. The SQLite plumbing they build on (``peel()``,
``SqliteSource``, ``Table``) lives in ``storage`` so the Dedup Layer can use
it too.
"""

from __future__ import annotations
