# synology-apm-repo-sdk

Offline, read-only Python SDK for the Synology APV/Object-Storage
dedup backup repository on-disk format. No network access, no writes to the
repository — it only decodes what's already on disk (local filesystem, a
mounted S3/Azure object store, or an SMB share) into browsable catalogs,
workloads, versions, and restorable files.

## Install

```bash
uv add synology-apm-repo-sdk        # inside a uv project
pip install synology-apm-repo-sdk   # any other environment
```

Full API reference: https://synology-apm.github.io/apm-repo-sdk-python/

## Usage

```python
import asyncio

from synology_apm_repo.sdk import Session


async def main() -> None:
    async with Session() as session:
        for repo in await session.open("/path/to/repository"):
            if repo.is_encrypted:
                verification = await repo.set_key("<userKeyID>@<base64 userKey>")
                if not verification.ok:
                    print(f"wrong key for {repo}")
                    continue
            for catalog in await repo.catalogs():
                for workload in await catalog.workloads():
                    for version in await catalog.versions(workload):
                        print(catalog.display_name, workload.display_name, version.display_name)


asyncio.run(main())
```

Everything the SDK does is `async def`; `Session`/`Repository` (both usable
as async context managers) are the entry point for a normal consumer —
`Repository` is one opened bucket or vault, `Catalog` (from
`Repository.catalogs()`) is one connection's worth of data within it.

## Requirements

Python 3.11+.

## License

Apache-2.0.
