# synology-apm-repo-cli

Command-line browser and exporter for Synology APV/Object-Storage
dedup backup repositories, built on `synology-apm-repo-sdk`. Offline,
read-only.

## Install

Run directly without installing:

```bash
uvx synology-apm-repo-cli --help
```

Or install with pip:

```bash
pip install synology-apm-repo-cli
```

Installs the `synology-apm-repo-cli` command.

## Usage

```bash
synology-apm-repo-cli ls "/path/to/repository#Test-Workload-01/my-vm/2026-08-07 09:00"
synology-apm-repo-cli tree "/path/to/repository#Test-Workload-01/my-vm/2026-08-07 09:00"
synology-apm-repo-cli cat "/path/to/repository#Test-Workload-01/my-vm/2026-08-07 09:00/Documents/report.pdf" > report.pdf
synology-apm-repo-cli export "/path/to/repository#Test-Workload-01/my-vm/2026-08-07 09:00/Documents/report.pdf" -o ./report.pdf
synology-apm-repo-cli verify /path/to/repository --level full
```

`ls`/`tree`/`cat`/`export`'s argument is a single
`<path>#<segment>/<segment>/...` ref; `doctor`/`key`/`verify` take a plain
repository path instead, and `dump`/`profile` are sub-command groups of
their own — run `synology-apm-repo-cli <command> --help` for that command's
own options. Commands: `doctor`, `ls`, `tree`, `cat`, `export`, `key`,
`verify`, `dump`, `profile`.

## Requirements

Python 3.11+.

## License

Apache-2.0.
