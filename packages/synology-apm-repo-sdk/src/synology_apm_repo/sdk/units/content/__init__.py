"""Content Layer — concrete ``ContentSource`` implementations and the pure
content-rendering functions behind them: Dissect-based filesystem parsing
(``disk_fs.py``), VM/PC/PS disk-fragment stitching (``pcps_disk.py``), and SaaS
artifact byte-assembly (the ``saas_*.py`` modules). The ``ContentSource``
Protocol itself is defined in ``units/base.py`` (Unit Layer); this package
only holds implementations of it.
"""

from __future__ import annotations
