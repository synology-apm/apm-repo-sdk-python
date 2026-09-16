"""S3/Azure ``db/<name>.<N>`` generation selection (FORMAT-SPEC.md:
generation-selection). The single place this rule lives; ``dircache.py``, ``seqid.py``,
``s3.py`` and ``dedup/repository.py``'s ``db()`` all point here rather than
restating it.

**Why the naive "largest ``.<N>`` suffix" rule** (``resolve_seq_file``)
**is not enough here**, unlike every other per-generation file this SDK
reads: a ``db/<name>`` generation can be written to S3/Azure *before* the
transaction that references it is actually committed, so the largest
suffix present can be a not-yet-committed or long-superseded generation.
The correct answer needs the transaction log:

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

A logical name with **no** ``.<N>`` variant at all falls back to the
bare, unsuffixed name — the same file the naive rule would have picked
anyway.
"""

from __future__ import annotations

from ..errors import NotFoundError
from ..format.repo_transaction import parse_repo_transaction
from .base import ObjectStore, join_path

#: This project's supplemental-table set — resolved by the
#: ``suppl_transaction_ids/`` marker rule, not the transaction-log rule.
#: See FORMAT-SPEC.md: generation-selection.
#: ``copy_target_file`` shares its physical file with ``copy_target_version``
#: (same on-disk name) but is listed here anyway for completeness — see
#: ``PHYSICAL_NAME_ALIASES`` for where that sharing is actually enforced,
#: not just documented.
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

#: Logical ``db/<name>`` names that are not their own on-disk object at
#: all — ``copy_target_file`` has no ``copy_target_file[.N]`` object
#: anywhere on a real repository; the ``copy_target_file`` *table* lives inside
#: whichever generation ``copy_target_version[.N]`` resolves to (same
#: physical sqlite file — both tables are written through one connection
#: on the write side).
#: Without this alias, ``DedupRepo.db("copy_target_file")`` would
#: search ``db/`` for an object literally named that and raise ``NotFoundError``
#: — there is none — starving the PC/PS browse path
#: (``PcpsDiskTree.object_nodes()``) of its whole object list. Consulted by
#: ``DedupRepo.db`` *before* any generation resolution happens, so
#: it applies uniformly on both ``VAULT`` and ``OBJECT_STORE`` layouts —
#: this is a fact about how the two tables are physically stored, not an
#: object-store-specific generation-selection quirk.
PHYSICAL_NAME_ALIASES: dict[str, str] = {"copy_target_file": "copy_target_version"}

REPO_TRANSACTIONS_DIR = "repo_transactions"
SUPPL_TRANSACTION_IDS_DIR = "suppl_transaction_ids"


def _numeric_suffixes(names: list[str], base: str) -> list[int]:
    """Every ``<base>.<N>`` entry in ``names`` with a purely-numeric
    ``<N>`` — ``<base>`` itself (bare, no suffix) never matches."""
    prefix = base + "."
    result = []
    for name in names:
        if name.startswith(prefix):
            suffix = name[len(prefix) :]
            if suffix.isdigit():
                result.append(int(suffix))
    return result


async def latest_transaction_id(store: ObjectStore, transactions_dir: str) -> int:
    """The latest *committed* transaction id, per FORMAT-SPEC.md: generation-selection."""
    names = await store.listdir(transactions_dir)
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
) -> str:
    """The correct ``db/<name>[.<N>]`` logical path for ``name`` on an
    ``OBJECT_STORE`` layout (FORMAT-SPEC.md: generation-selection's two-branch rule —
    this module's own docstring). ``db_dir``/``transactions_dir``/
    ``suppl_dir`` are already joined with ``layout.repo_root`` by the
    caller.

    Raises ``NotFoundError`` if ``name`` has ``.<N>`` variants present but
    none of them is actually valid per the rule that applies to it (a
    genuinely inconsistent/partial repository copy, not something this
    function should silently paper over).
    """
    entries = await store.listdir(db_dir)
    suffixes = _numeric_suffixes(entries, name)
    if not suffixes:
        return join_path(db_dir, name)

    if name in SUPPLEMENTAL_TABLES:
        suppl_ids = {int(n) for n in await store.listdir(suppl_dir) if n.isdigit()}
        valid = [s for s in suffixes if s in suppl_ids]
        if not valid:
            raise NotFoundError(
                f"no {name!r} generation has a matching supplemental marker",
                ref=f"{db_dir}: candidates={sorted(suffixes)} markers={sorted(suppl_ids)} suppl_dir={suppl_dir!r}",
            )
        chosen = max(valid)
    else:
        latest_txn = await latest_transaction_id(store, transactions_dir)
        valid = [s for s in suffixes if s < latest_txn]
        if not valid:
            raise NotFoundError(
                f"no {name!r} generation is committed",
                ref=f"{db_dir}: candidates={sorted(suffixes)} latest_txn={latest_txn}",
            )
        chosen = max(valid)
    return join_path(db_dir, f"{name}.{chosen}")
