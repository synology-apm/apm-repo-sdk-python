# apm-repo-sdk-python

[![CI](https://github.com/synology-apm/apm-repo-sdk-python/actions/workflows/ci.yml/badge.svg)](https://github.com/synology-apm/apm-repo-sdk-python/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://synology-apm.github.io/apm-repo-sdk-python/)
[![PyPI - synology-apm-repo-sdk](https://img.shields.io/pypi/v/synology-apm-repo-sdk?label=synology-apm-repo-sdk)](https://pypi.org/project/synology-apm-repo-sdk/)
[![PyPI - synology-apm-repo-cli](https://img.shields.io/pypi/v/synology-apm-repo-cli?label=synology-apm-repo-cli)](https://pypi.org/project/synology-apm-repo-cli/)
[![PyPI - synology-apm-repo-browser](https://img.shields.io/pypi/v/synology-apm-repo-browser?label=synology-apm-repo-browser)](https://pypi.org/project/synology-apm-repo-browser/)

An offline, read-only reader for Synology ActiveProtect's dedup backup
repository format (APV/Object-Storage) — it never talks to a running
ActiveProtect service, only decodes the on-disk bytes of a copied-out
repository. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full picture.

## Prerequisites

Pick whichever matches the install method you use below — you don't need both:

- `uv` — provisions Python automatically and powers the `uvx`/`uv tool`/`uv add` commands below; see the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) for macOS, Windows, and Linux
- `pip` — usually bundled with your Python installation; see the [installation instructions](https://pip.pypa.io/en/stable/installation/) if you need to install it separately
- Python 3.11 or later (provisioned automatically when using `uv`/`uvx`; required on your own interpreter for a plain `pip install`)

---

## Browser (TUI)

`synology-apm-repo-browser` is a Textual TUI for the same repositories —
useful when you want to browse interactively instead of scripting against
the CLI.

### Install the Browser

Run directly without installing:

```bash
uvx synology-apm-repo-browser --help
```

Or install with pip:

```bash
pip install synology-apm-repo-browser
```

### Browser Quick Start

```bash
synology-apm-repo-browser
```

Launches straight into a connect dialog — point it at a local repository
path or a remote S3/Azure/SMB store, enter a decryption key if the repository
is encrypted, then browse connections → workloads → versions → files. Press
`c` to reconnect, `d` to toggle verbose mode, `?` for a full list of
keys, `q` to quit.

See [`packages/synology-apm-repo-browser/README.md`](packages/synology-apm-repo-browser/README.md)
for the full flag list.

---

## CLI

`synology-apm-repo-cli` browses and exports a repository from your terminal —
`ls`/`tree`/`cat`/`export` against a `<path>#<segment>/<segment>/...` ref,
plus `doctor`, `key`, `verify`, `dump`, and `profile`.

### Install the CLI

Run directly without installing:

```bash
uvx synology-apm-repo-cli --help
```

Or install with pip:

```bash
pip install synology-apm-repo-cli
```

### CLI Quick Start

```bash
synology-apm-repo-cli ls "/path/to/repository#Test-Workload-01/my-vm/2026-08-07 09:00"
```

### Full CLI Command Reference

See [`packages/synology-apm-repo-cli/README.md`](packages/synology-apm-repo-cli/README.md)
for `tree`/`cat`/`export`/`verify` examples, the full command list, and each
command's own `--help` options.

---

## Accessing repository data

Point the CLI/browser at a local path to a copied-out repository — a
directory containing `@ActiveProtectVault`. If that repository lives on a
Synology NAS, you can hit a permissions error reading its files over SMB
(mounted or connected to directly) even though the share itself is
accessible — the ACL on `@ActiveProtectVault` itself is what's denying
access.

**Fix**: enable "include inherited permissions" on `@ActiveProtectVault`
itself, applied to the folder, its sub-folders, and its files.

---

## Developer Guide

Building your own automation on top of the SDK, or contributing to this repository? Start here.

### SDK

`synology-apm-repo-sdk` is the async-native, fully typed Python interface
that both the CLI and browser are built on.

#### Install the SDK

```bash
uv add synology-apm-repo-sdk        # inside a uv project
pip install synology-apm-repo-sdk   # any other environment
```

See [`packages/synology-apm-repo-sdk/README.md`](packages/synology-apm-repo-sdk/README.md)
for a full usage example.

### Install From Source (Contributing)

```bash
git clone https://github.com/synology-apm/apm-repo-sdk-python.git
cd apm-repo-sdk-python

uv sync --all-packages
make test

# Run your local changes without installing
uv run synology-apm-repo-cli --help
uv run synology-apm-repo-browser
```

See [`CLAUDE.md`](CLAUDE.md) for the development guide and
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the full pre-commit gate and
sample-data conventions; see [`tests/CLAUDE.md`](tests/CLAUDE.md) for how
the test suite is organized and how to (re-)record a fixture against a
real sample.

### API Reference

The full SDK API reference (every public class, method, and type signature)
is generated from source with Sphinx and published at:

**https://synology-apm.github.io/apm-repo-sdk-python/**

To build it locally instead (e.g. to preview docstring changes):

```bash
uv sync --group docs   # first time only
make docs
```

Then open `docs/_build/html/index.html` in your browser.

---

## Documentation Index

| Document | Description |
|----------|-------------|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | The layering contract (Codec/Storage/Dedup/Catalog/Content/Unit/Repository), packaging/distribution shape, async-native design, presentation principles |
| [`FORMAT-SPEC.md`](FORMAT-SPEC.md) | The on-disk format specification this project decodes |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Commit message convention, the pre-commit gate, sample-data handling, release/publish-channel status |
| [`packages/synology-apm-repo-sdk/README.md`](packages/synology-apm-repo-sdk/README.md) | SDK install and usage example |
| [`packages/synology-apm-repo-cli/README.md`](packages/synology-apm-repo-cli/README.md) | CLI install and full command list |
| [`packages/synology-apm-repo-browser/README.md`](packages/synology-apm-repo-browser/README.md) | Browser (TUI) install, usage, and flags |
| [API Reference](https://synology-apm.github.io/apm-repo-sdk-python/) | Full SDK API reference — every public class, method, and type signature (Sphinx, hosted on GitHub Pages) |
| [`tests/CLAUDE.md`](tests/CLAUDE.md) | Testing conventions: synthetic vs real-data-replay tests, fixture recording/replay mechanisms |
| [`CLAUDE.md`](CLAUDE.md) | Development guide — code conventions and the Post-change Checklist |
