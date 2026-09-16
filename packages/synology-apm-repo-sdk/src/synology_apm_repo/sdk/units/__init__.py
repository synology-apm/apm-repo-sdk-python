"""Restorable units: tree navigation over a workload version, in terms of
``Node``/``RestorableUnit`` — content decoding itself lives one layer down,
in ``units.content``. ``NodeRef`` is the shared addressing scheme every
provider's ``Node``/``RestorableUnit`` tree uses.
"""

from __future__ import annotations
