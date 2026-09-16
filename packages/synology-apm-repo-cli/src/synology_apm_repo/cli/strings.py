"""All user-facing help text lives here so it can't drift between
commands. Command docstrings stay inline — typer surfaces them via
``--help``.
"""

from __future__ import annotations

# -- root app / global flags (main.py) -----------------------------------

APP_HELP = "Offline browser/exporter for Synology APV/Object-Storage dedup repositories."
VERBOSE_HELP = "Show internal identifiers."
JSON_HELP = "Structured JSON output, keyed by stable internal ids."
PROGRESS_HELP = (
    "Progress display on stderr: auto (default, live bar on a real terminal, "
    "periodic text line otherwise), always (force the live bar), or never (fully silent)."
)
TRACE_HELP = "Log every ObjectStore call (path/offset/length/elapsed) to stderr as it happens."
QUIET_HELP = "Suppress success-confirmation output (errors and each command's primary report are unaffected)."
NO_INPUT_HELP = (
    "Never prompt; profile add reads secrets from stdin instead, and profile remove "
    "requires --force for anything that would otherwise ask for confirmation."
)
VERSION_HELP = "Show the installed version and exit."

# -- shared across multiple commands --------------------------------------

KEY_HELP = 'Key string ("<userKeyID>@<base64(userKey)>") for an encrypted repository.'
REPO_PATH_HELP = (
    "Path to a repository root (or a directory containing exactly one). With --profile, "
    "omit this argument — the profile's bucket/container/share root is scanned instead."
)
SHOW_REF_HELP = "Also print each item's canonical NodeRef."

# -- per-command (cat/export share one ref phrasing; ls/tree each have
# their own, since ls's points readers at its own --help) --------------

REF_HELP_SINGLE_ITEM = (
    "A <path>#<fragment> ref naming exactly one item. With --profile, <path> is a "
    "store-relative sub-path instead of a filesystem path. A name containing a literal "
    "'/' needs percent-encoding within its segment — see `synology-apm-repo-cli ls --help`."
)  # cat, export
REF_HELP_LS = "<path>, optionally followed by #<name>/<name>/... — see the description above for what each part means."
REF_HELP_TREE = (
    r"A <path>\[#<fragment>] ref. With --profile, <path> is a store-relative sub-path instead of a filesystem path. "
    "A name containing a literal '/' needs percent-encoding within its segment — see "
    "`synology-apm-repo-cli ls --help`."
)

CAT_OFFSET_HELP = "Byte offset to start reading from."
CAT_LENGTH_HELP = "Number of bytes to read (default: to the end)."

EXPORT_OUTPUT_HELP = "Destination file path."
EXPORT_SPARSE_HELP = "Skip writing zero/hole regions (default: on)."
EXPORT_KEEP_PARTIAL_HELP = "Keep the .part file after a Ctrl-C cancel instead of deleting it."
EXPORT_FORCE_HELP = "Overwrite the destination file if it already exists (default: refuse)."

VERIFY_LEVEL_HELP = (
    "quick (default): every reachable composition record and bucket is checked structurally; "
    "no chunk content is read or checked. full: every live chunk in every touched bucket also "
    "gets decoded and checked — as thorough, and as slow, as a full export of the whole "
    "repository."
)

TREE_DEPTH_HELP = "Maximum levels to descend."

OBJECT_DB_ID_HELP = (
    "Manually override automatic ObjectDB sequence disambiguation — "
    "'<streamUuid>_<offset>_<length>', from an online SnapshotDB or a prior --verbose browse."
)

# -- dump bucket|composition|chunkmap -------------------------------------

DUMP_BUCKET_PATH_HELP = (
    "Path to a .buk file. With --profile, this is a store-relative sub-path instead of a filesystem path."
)
DUMP_BUCKET_CHUNK_HELP = "Also show one chunk's SizeStore entry/locator by index."
DUMP_COMPOSITION_PATH_HELP = (
    "Path to a composition sub-file (e.g. c0). With --profile, this is a store-relative "
    "sub-path instead of a filesystem path."
)
DUMP_OFFSET_WALK_HELP = "Byte offset to start walking records from."
DUMP_LIMIT_RECORDS_HELP = "Maximum number of records to walk/print."
DUMP_VERIFY_MAP_HELP = "Also read and CRC-check each record's full chunk-map array (expensive)."
DUMP_CHUNKMAP_OFFSET_HELP = "The record's head_off (from `dump composition`)."
DUMP_LIMIT_ENTRIES_HELP = "Maximum number of chunk-map entries to print."
DUMP_VERIFY_HELP = "CRC-check the full chunk-map array before printing."

# -- profile add|list|show|remove -----------------------------------------

PROFILE_OPTION_HELP = (
    "Open a saved connection profile (see `synology-apm-repo-cli profile add`) instead of a local filesystem path."
)
PROFILE_NAME_HELP = "Profile name."
PROFILE_BACKEND_HELP = "Backend this profile connects to."
PROFILE_BUCKET_HELP = "S3 bucket name."
PROFILE_CONTAINER_HELP = "Azure container name."
PROFILE_ENDPOINT_HELP = "S3 endpoint URL (omit to use AWS's default endpoint)."
PROFILE_REGION_HELP = "S3 region (omit to use the client's default)."
PROFILE_ACCOUNT_URL_HELP = (
    "Azure storage account URL, e.g. https://<account>.blob.core.windows.net (Azurite: http://<host>:10000/<account>)."
)
PROFILE_VERIFY_HELP = (
    "Scan the bucket/container/share for repositories before saving, failing without saving if none are found "
    "(default: on)."
)
PROFILE_VERIFY_TLS_HELP = (
    "Verify the server's TLS certificate (S3/Azure only, ignored for --backend smb; default: on). "
    "Turn off only for a known endpoint with a self-signed/internal cert, e.g. an internal test S3 server."
)
PROFILE_FORCE_HELP = "Overwrite an existing profile of the same name without asking."
PROFILE_REMOVE_FORCE_HELP = "Skip the confirmation prompt."
PROFILE_SERVER_HELP = "SMB server hostname or IP address."
PROFILE_SHARE_HELP = "SMB share name."
PROFILE_PORT_HELP = "SMB server port (default: 445)."
PROFILE_USERNAME_HELP = 'SMB username, optionally "DOMAIN\\username" (omit for an anonymous/guest session).'
