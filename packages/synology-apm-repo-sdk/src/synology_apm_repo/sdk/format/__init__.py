"""Pure ``bytes -> dataclass`` codecs, zero I/O.

Every module here takes bytes in, returns a dataclass (or bytes) out. None
of them open a file, know a path, or import anything from ``storage``/
``dedup``/... above them.
"""

from __future__ import annotations
