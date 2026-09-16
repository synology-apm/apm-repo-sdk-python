"""Screen titles + key-press hints — the presentation-layer strings this
package owns. Centralized for the same reason ``cli/strings.py`` exists:
without an owner these get duplicated with slightly different wording
across screens (every navigable screen's status-bar hint line
independently spelling out "j/k move · l/Enter open · h/Esc back"), and
CLI and TUI displaying the same thing differently is a bug.

Dynamic, runtime-composed messages (an error string built from a real
exception, a status line embedding a real filename) stay inline next to
the code that builds them — only the static copy moves here, the same
line ``cli/strings.py`` draws for docstrings vs. flag help text.
"""

from __future__ import annotations

# Key-hint fragments shared verbatim across BROWSE_STATUS_BAR/UNIT_STATUS_BAR/
# HELP_KEYS_TEXT below — composing those three from these instead of each
# re-spelling the same hint keeps a future key-hint rename from silently
# missing one of the near-duplicate literals. Fragments that actually differ
# between screens (e.g. "l/Enter open" vs. "l/Enter expand/open") stay
# spelled out at each call site rather than forced into a shared fragment.
_NAV_MOVE = "j/k move"
_NAV_BACK = "h/Esc back"
_FILTER_KEYS = "v verify · r refresh · / filter"
_GOTO_KEY = "g goto"

# -- BrowseScreen (repository/backup-source picking now lives entirely in
# ConnectDialog, auto-opened on launch — see that screen's own module
# docstring) ------------------------------------------------------------

BROWSE_STATUS_BAR = f"{_NAV_MOVE} · l/Enter open · {_NAV_BACK} · {_FILTER_KEYS} · {_GOTO_KEY} · c connect · d verbose"
BROWSE_FILTER_PLACEHOLDER = "filter (Esc to clear)"
BROWSE_VERSIONS_EMPTY_LABEL = "(no available versions)"

# -- KeyDialog (a centered modal, automatically pushed by BrowseScreen —
# no keybinding reaches it) ------------------------------------------------

KEY_PROMPT = "This repository is encrypted. Paste a key string (<userKeyID>@<base64(userKey)>), or Esc to cancel:"
KEY_INPUT_PLACEHOLDER = "<userKeyID>@<base64>"

# -- ConnectDialog (auto-opened on launch; ``c`` reopens it from BrowseScreen) --

CONNECT_PROMPT = "Connect to a local directory, an S3/Azure Blob Storage-backed repository, or an SMB share:"
CONNECT_BACKEND_LOCAL_LABEL = "Local"
CONNECT_BACKEND_S3_LABEL = "S3"
CONNECT_BACKEND_AZURE_LABEL = "Azure"
CONNECT_BACKEND_SMB_LABEL = "SMB"
CONNECT_LOCAL_PROMPT = 'Browse or type a directory path (select ".." to go up):'
CONNECT_SUBMIT_LABEL = "Connect"
CONNECT_SUBMIT_LABEL_LOCAL = "Open"
CONNECT_NO_PATH_WARNING = "enter a directory path"
CONNECT_PATH_NOT_A_DIRECTORY_WARNING = "not a directory"
CONNECT_S3_BUCKET_PLACEHOLDER = "bucket name"
CONNECT_S3_BROWSE_BUCKETS_LABEL = "Browse"
CONNECT_S3_ENDPOINT_PLACEHOLDER = "endpoint URL (optional — any S3-compatible endpoint, else real AWS S3)"
CONNECT_S3_REGION_PLACEHOLDER = "region (optional)"
CONNECT_S3_ACCESS_KEY_PLACEHOLDER = "access key ID (optional — else the default AWS credential chain)"
CONNECT_S3_SECRET_KEY_PLACEHOLDER = "secret access key"
CONNECT_S3_VERIFY_TLS_LABEL = "verify TLS certificate"
CONNECT_AZURE_ACCOUNT_URL_PLACEHOLDER = (
    "account URL (https://<account>.blob.core.windows.net, or Azurite: http://<host>:10000/<account>)"
)
CONNECT_AZURE_CREDENTIAL_PLACEHOLDER = "account key or SAS token (optional — else the default Azure credential chain)"
CONNECT_AZURE_CONTAINER_PLACEHOLDER = "container name"
CONNECT_AZURE_BROWSE_CONTAINERS_LABEL = "Browse"
CONNECT_NO_BUCKET_WARNING = "enter a bucket name"
CONNECT_NO_CONTAINER_WARNING = "enter a container name"
CONNECT_S3_PROFILE_SELECT_PROMPT = "load a saved S3 profile"
CONNECT_AZURE_PROFILE_SELECT_PROMPT = "load a saved Azure profile"
CONNECT_SMB_PROFILE_SELECT_PROMPT = "load a saved SMB profile"
CONNECT_SAVE_PROFILE_LABEL = "Save as profile..."
CONNECT_DELETE_PROFILE_LABEL = "Delete profile"
CONNECT_PROFILE_NAME_PLACEHOLDER = "profile name"
CONNECT_PROFILE_NAME_CONFIRM_LABEL = "Save"
CONNECT_PROFILE_NAME_REQUIRED_WARNING = "enter a profile name"
CONNECT_NETWORK_TIMEOUT_WARNING = "timed out contacting endpoint — check the endpoint URL/network"
CONNECT_SMB_SERVER_PLACEHOLDER = "server hostname or IP address"
CONNECT_SMB_SHARE_PLACEHOLDER = "share name"
CONNECT_SMB_USERNAME_PLACEHOLDER = (
    'username, optionally "DOMAIN\\username" (optional — else an anonymous/guest session)'
)
CONNECT_SMB_PASSWORD_PLACEHOLDER = "password"
CONNECT_NO_SERVER_WARNING = "enter a server hostname or IP address"
CONNECT_NO_SHARE_WARNING = "enter a share name"
CONNECT_SMB_INVALID_PORT_WARNING = "port must be a whole number"

# -- UnitScreen ---------------------------------------------------------

UNIT_STATUS_BAR = (
    f"{_NAV_MOVE} · l/Enter expand/open · {_NAV_BACK} · e export · i detail · "
    f"{_FILTER_KEYS} · y copy ref · {_GOTO_KEY} · + load more"
)
UNIT_NOTHING_SELECTED_WARNING = "select an item to export first"
UNIT_FILTER_PLACEHOLDER = "filter (Esc to clear)"
UNIT_HEX_NOTHING_SELECTED_WARNING = "select a leaf item first"
UNIT_COPY_REF_NOTHING_SELECTED_WARNING = "select an item to copy its ref first"
UNIT_COPY_REF_NOTIFY = "copied ref to clipboard"

# -- pagination ("load more", ``+``) ---------------------------------------

UNIT_LOAD_MORE_NOTIFY = "loaded {loaded} more ({total} total)"
UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING = "nothing loaded under this level yet — expand it first"
UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING = "everything at this level is already loaded"
UNIT_FILTER_PARTIAL_LOAD_WARNING = "filtering only the {loaded} items loaded so far — press + to load more first"

# -- goto-ref (``g``) -------------------------------------------------------

GOTO_REF_PLACEHOLDER = "paste a canonical ref (Esc to cancel)"
GOTO_REF_NOT_CANONICAL_WARNING = (
    "g only accepts a canonical ref (the form y copies) — human/raw refs aren't supported yet"
)
GOTO_REF_PARSE_ERROR_WARNING = "not a valid ref"
GOTO_REF_NOT_FOUND_WARNING = "ref not found in this repository"

# -- HexPreviewScreen -----------------------------------------------------

HEX_STATUS_BAR = "+/- page · x/X page · Esc/h back"
HEX_WINDOW_SIZE = 512

# -- ExportScreen -----------------------------------------------------

# The ``_LABEL`` string is a permanent ``Static`` above the field, since
# Textual only ever renders an Input's ``placeholder=`` while ``value`` is
# empty (``Input.render_line()``) and the field starts pre-filled
# (``./<name>``) — a placeholder alone would never be seen.
# ``_PLACEHOLDER`` stays as the short in-box hint for the (real but
# secondary) case where the user clears the field back to empty.
EXPORT_DST_LABEL = "Destination path"
EXPORT_DST_PLACEHOLDER = "destination path"
EXPORT_START_LABEL = "Export"
EXPORT_CANCEL_LABEL = "Cancel"
EXPORT_STATUS_BAR = "b: continue in background and keep browsing"
EXPORT_NO_DESTINATION_WARNING = "enter a destination path"
EXPORT_NOTHING_RUNNING_WARNING = "nothing exporting yet"
EXPORT_NOTIFY_TITLE = "Export"
EXPORT_BACKGROUNDED_TITLE = "Backgrounded"

# -- DiagnosticsScreen ----------------------------------------------------

DIAGNOSTICS_COLUMNS = ("stage", "symptom", "path", "detail")
DIAGNOSTICS_QUICK_STATUS = "Integrity check — quick level. Press f for a full check."
DIAGNOSTICS_FULL_RUNNING_STATUS = "Integrity check — full level (this may take a while)..."

# -- WorklistScreen -------------------------------------------------------

WORKLIST_COLUMNS = ("job", "progress", "status")
WORKLIST_STATUS_BAR = "Background jobs — x to cancel the selected one, Esc to go back"
WORKLIST_EMPTY_STATUS = "No background jobs. Esc to go back."

# -- HelpScreen (app.py's ``?`` key) ---------------------------------------

HELP_TITLE = "Keys"
HELP_COLUMNS = ("key", "action")
HELP_VERBOSE_NOTE = (
    "d toggles verbose mode: internal identifiers, canonical NodeRef form, "
    "hex previews, and verify findings — off by default."
)
