"""Connection / workload / version: purely cheap SQLite
reads, nothing here ever touches Pool/Composition bytes. ``peel()``/
``SqliteSource``/``Table`` are the shared plumbing every workload-DB
accessor in this layer and above builds on — they live in ``storage``
(the Storage Layer), not here, specifically so the Dedup Layer
(``dedup/``) can depend on them too without an upward layer violation.
"""

from __future__ import annotations
