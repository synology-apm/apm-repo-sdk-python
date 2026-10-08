"""S3/Azure ``db/<name>.<N>`` generation selection (FORMAT-SPEC.md:
Multi-generation selection). The single place this rule lives;
``dedup/repository.py`` uses it for ``db/<name>`` and ``repo_info``.

The naive "largest ``.<N>`` suffix" rule (``resolve_seq_file``) is not enough
here: a ``db/<name>`` generation can be written to S3/Azure before its
transaction commits, so the largest suffix can be uncommitted or superseded.
The answer needs the transaction log:

1. **``latest_txn``** (``latest_transaction_id``): the largest
   ``repo_transactions/repo_transaction.<N>`` filename, opened and
   parsed — the answer is that file's *embedded* ``transaction_id``,
   never the filename's own ``<N>`` (the two are not the same number).
2. **``file_map``/``repo_info``** (anything not in ``SUPPLEMENTAL_TABLES``):
   among ``db/<name>.<N>``'s suffixes, the largest one **strictly less
   than** ``latest_txn`` — a generation written at or after the latest
   committed transaction is exactly the "written but not yet committed"
   case above.
3. **The 9 supplemental tables** (``SUPPLEMENTAL_TABLES``) use an
   independent, simpler rule: the largest suffix that also has a
   matching ``suppl_transaction_ids/<N>`` marker file — a numbering
   completely unrelated to ``repo_transactions/``'s own.

A logical name with no ``.<N>`` variant falls back to the bare name.
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable

from ..errors import NotFoundError
from ..format.repo_transaction import parse_repo_transaction
from .base import ObjectStore, join_path, list_names

#: The supplemental tables, resolved by the ``suppl_transaction_ids/`` marker
#: rule, not the transaction-log rule (FORMAT-SPEC.md: Multi-generation
#: selection). ``copy_target_file`` is listed for completeness; it shares
#: ``copy_target_version``'s physical file (see ``PHYSICAL_NAME_ALIASES``).
SUPPLEMENTAL_TABLES = frozenset(
    {
        "agent_connection",
        "connection_config",
        "copy_file",
        "copy_source_version",
        "copy_target_version",
        "copy_target_version_meta",
        "copy_target_file",
        "file_meta",
        "workload_config",
    }
)

#: Logical ``db/<name>`` names with no on-disk object of their own:
#: ``copy_target_file`` lives inside the ``copy_target_version[.N]`` file.
#: ``DedupRepo.db`` applies it before generation resolution, on both layouts.
PHYSICAL_NAME_ALIASES: dict[str, str] = {"copy_target_file": "copy_target_version"}

Listdir = Callable[[str], Awaitable[list[str]]]
"""A function returning a directory's entry names, such as ``DirCache.listdir``."""

REPO_TRANSACTIONS_DIR = "repo_transactions"
SUPPL_TRANSACTION_IDS_DIR = "suppl_transaction_ids"


def _numeric_suffixes(names: list[str], base: str) -> list[int]:
    """The ``<N>`` of every ``<base>.<N>`` entry in ``names`` with a numeric
    ``<N>``; bare ``<base>`` never matches."""
    prefix = base + "."
    result = []
    for name in names:
        if name.startswith(prefix):
            suffix = name[len(prefix) :]
            if suffix.isdigit():
                result.append(int(suffix))
    return result


async def latest_transaction_id(store: ObjectStore, transactions_dir: str, *, listdir: Listdir | None = None) -> int:
    """The latest *committed* transaction id, per FORMAT-SPEC.md:
    Multi-generation selection: the ``transaction_id`` embedded in the
    highest-numbered ``repo_transaction.<N>`` file.

    Args:
        store: The repository's store.
        transactions_dir: The ``repo_transactions`` directory.
        listdir: How to list ``transactions_dir``; defaults to listing
            ``store`` directly (pass a ``DirCache.listdir`` to share the
            listing).

    Raises:
        NotFoundError: ``transactions_dir`` holds no ``repo_transaction.<N>``.
        DataCorruptError: That file is corrupt (see ``parse_repo_transaction``).
    """
    names = await listdir(transactions_dir) if listdir is not None else await list_names(store, transactions_dir)
    suffixes = _numeric_suffixes(names, "repo_transaction")
    if not suffixes:
        raise NotFoundError("no repo_transaction.<N> files found", ref=transactions_dir)
    latest_filename_n = max(suffixes)
    path = join_path(transactions_dir, f"repo_transaction.{latest_filename_n}")
    txn = parse_repo_transaction(await store.read(path))
    return txn.transaction_id


async def resolve_generation(
    store: ObjectStore,
    db_dir: str,
    name: str,
    *,
    transactions_dir: str,
    suppl_dir: str,
    listdir: Listdir | None = None,
) -> str:
    """The correct ``db/<name>[.<N>]`` logical path for ``name`` on an
    ``OBJECT_STORE`` layout: the largest ``.<N>`` strictly less than the
    latest *committed* transaction for most tables, or the largest ``.<N>``
    with a matching ``suppl_transaction_ids`` marker for the 9 supplemental
    tables (FORMAT-SPEC.md: Multi-generation selection). A name with no
    ``.<N>`` variant resolves to the bare name.

    Args:
        store: The repository's store.
        db_dir: The ``db`` directory, joined with ``layout.repo_root``.
        name: Logical ``db/<name>``.
        transactions_dir: ``repo_transactions`` directory, likewise joined.
        suppl_dir: ``suppl_transaction_ids`` directory, likewise joined.
        listdir: How to list the three directories; defaults to listing
            ``store`` directly (pass a ``DirCache.listdir`` to share listings
            across the several ``db/<name>`` resolutions one repository does).

    Returns:
        The store-relative path of the chosen generation.

    Raises:
        NotFoundError: ``.<N>`` variants exist but none is valid under the
            applicable rule (an inconsistent or partial repository copy), or
            ``transactions_dir`` holds no ``repo_transaction.<N>``.
        DataCorruptError: The latest ``repo_transaction.<N>`` is corrupt.
    """
    list_dir: Listdir = listdir if listdir is not None else functools.partial(list_names, store)
    entries = await list_dir(db_dir)
    suffixes = _numeric_suffixes(entries, name)
    if not suffixes:
        return join_path(db_dir, name)

    if name in SUPPLEMENTAL_TABLES:
        suppl_ids = {int(n) for n in await list_dir(suppl_dir) if n.isdigit()}
        valid = [s for s in suffixes if s in suppl_ids]
        if not valid:
            raise NotFoundError(
                f"no {name!r} generation has a matching supplemental marker",
                ref=f"{db_dir}: candidates={sorted(suffixes)} markers={sorted(suppl_ids)} suppl_dir={suppl_dir!r}",
            )
        chosen = max(valid)
    else:
        latest_txn = await latest_transaction_id(store, transactions_dir, listdir=list_dir)
        valid = [s for s in suffixes if s < latest_txn]
        if not valid:
            raise NotFoundError(
                f"no {name!r} generation is committed",
                ref=f"{db_dir}: candidates={sorted(suffixes)} latest_txn={latest_txn}",
            )
        chosen = max(valid)
    return join_path(db_dir, f"{name}.{chosen}")
