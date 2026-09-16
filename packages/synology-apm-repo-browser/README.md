# synology-apm-repo-browser

Textual TUI browser and exporter for Synology APV/Object-Storage
dedup backup repositories, built on `synology-apm-repo-sdk`. Offline,
read-only.

## Install

Run directly without installing:

```bash
uvx synology-apm-repo-browser --help
```

Or install with pip:

```bash
pip install synology-apm-repo-browser
```

Installs the `synology-apm-repo-browser` command. Install
`synology-apm-repo-cli` separately (`pip install synology-apm-repo-cli`) if
you also want the CLI.

## Usage

```bash
synology-apm-repo-browser
```

Launches straight into a connect dialog — point it at a local repository
path or a remote S3/Azure/SMB store, enter a decryption key if the repository
is encrypted, then browse connections → workloads → versions → files.
Press `c` to reconnect to a different repository, `d` to toggle verbose
mode, `?` for a full list of keys, `q` to quit.

Flags: `-h`/`--help`, `--version`, and `--no-sparse-export` (exports write
sparse files — skipping zero/hole regions — by default; this turns that
off for the whole session).

## Requirements

Python 3.11+.

## License

Apache-2.0.
