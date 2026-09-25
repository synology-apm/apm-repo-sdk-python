Synology APM Repository SDK
============================

Offline, read-only Python SDK for Synology APV/Object-Storage dedup
repository formats.

**Installation**

.. code-block:: bash

   pip install synology-apm-repo-sdk

**Quick start**

.. code-block:: python

   import asyncio
   from synology_apm_repo.sdk import Session

   async def main():
       async with Session() as session:
           async for repo in session.discover("/path/to/repository"):
               if repo.is_encrypted:
                   verification = await repo.set_key("<userKeyID>@<base64 userKey>")
                   if not verification.ok:
                       print(f"wrong key for {repo}")
                       continue
               for catalog in await repo.catalogs():
                   for workload in await catalog.workloads():
                       print(workload.display_name)

   asyncio.run(main())

.. toctree::
   :maxdepth: 2
   :caption: Repository & catalog

   api/synology_apm_repo.sdk.api.repository
   api/synology_apm_repo.sdk.api.key_manager
   api/synology_apm_repo.sdk.api.catalog
   api/synology_apm_repo.sdk.api.session
   api/synology_apm_repo.sdk.diagnostics
   api/synology_apm_repo.sdk.catalog.connection
   api/synology_apm_repo.sdk.catalog.workload
   api/synology_apm_repo.sdk.catalog.version
   api/synology_apm_repo.sdk.catalog.workload_config
   api/synology_apm_repo.sdk.errors
   api/synology_apm_repo.sdk.identifiers
   api/synology_apm_repo.sdk.asynccache
   api/synology_apm_repo.sdk.concurrency

.. toctree::
   :maxdepth: 2
   :caption: Dedup engine

   api/synology_apm_repo.sdk.dedup.repository
   api/synology_apm_repo.sdk.dedup.dedup_file
   api/synology_apm_repo.sdk.dedup.composition_reader
   api/synology_apm_repo.sdk.dedup.chunk_walk
   api/synology_apm_repo.sdk.dedup.pool
   api/synology_apm_repo.sdk.dedup.pool_descriptor
   api/synology_apm_repo.sdk.dedup.export_scheduler
   api/synology_apm_repo.sdk.dedup.presized_file
   api/synology_apm_repo.sdk.dedup.fingerprint
   api/synology_apm_repo.sdk.dedup.keys
   api/synology_apm_repo.sdk.dedup.verify_checks
   api/synology_apm_repo.sdk.dedup.verify_report

.. toctree::
   :maxdepth: 2
   :caption: On-disk format primitives

   api/synology_apm_repo.sdk.format.repo_info
   api/synology_apm_repo.sdk.format.repo_transaction
   api/synology_apm_repo.sdk.format.bucket
   api/synology_apm_repo.sdk.format.composition
   api/synology_apm_repo.sdk.format.chunkmap
   api/synology_apm_repo.sdk.format.addressing
   api/synology_apm_repo.sdk.format.headers
   api/synology_apm_repo.sdk.format.compression
   api/synology_apm_repo.sdk.format.crypto
   api/synology_apm_repo.sdk.format.redundancy
   api/synology_apm_repo.sdk.format.const

.. toctree::
   :maxdepth: 2
   :caption: Storage backends

   api/synology_apm_repo.sdk.storage.base
   api/synology_apm_repo.sdk.storage.store_descriptor
   api/synology_apm_repo.sdk.storage.local
   api/synology_apm_repo.sdk.storage.s3
   api/synology_apm_repo.sdk.storage.azure
   api/synology_apm_repo.sdk.storage.smb
   api/synology_apm_repo.sdk.storage.prefix_listing
   api/synology_apm_repo.sdk.storage.layout
   api/synology_apm_repo.sdk.storage.sqlite
   api/synology_apm_repo.sdk.storage.sqlite_source
   api/synology_apm_repo.sdk.storage.table
   api/synology_apm_repo.sdk.storage.generations
   api/synology_apm_repo.sdk.storage.recording
   api/synology_apm_repo.sdk.storage.dircache
   api/synology_apm_repo.sdk.storage.seqid

.. toctree::
   :maxdepth: 2
   :caption: Restorable units

   api/synology_apm_repo.sdk.units.base
   api/synology_apm_repo.sdk.units.dispatch
   api/synology_apm_repo.sdk.units.fs
   api/synology_apm_repo.sdk.units.device
   api/synology_apm_repo.sdk.units.device_pcps
   api/synology_apm_repo.sdk.units.device_disk_fs
   api/synology_apm_repo.sdk.units.device_kind
   api/synology_apm_repo.sdk.units.node_ref
   api/synology_apm_repo.sdk.units.resolve
   api/synology_apm_repo.sdk.units.file_map_tree
   api/synology_apm_repo.sdk.units.verify_reachable
   api/synology_apm_repo.sdk.units.verify_extents
   api/synology_apm_repo.sdk.units.verify_bucket_check

.. toctree::
   :maxdepth: 2
   :caption: Content Layer (units/content/)

   api/synology_apm_repo.sdk.units.content.disk_fs
   api/synology_apm_repo.sdk.units.content.pcps_disk
   api/synology_apm_repo.sdk.units.content.saas_artifact
   api/synology_apm_repo.sdk.units.content.saas_mail
   api/synology_apm_repo.sdk.units.content.saas_calendar
   api/synology_apm_repo.sdk.units.content.saas_contact
   api/synology_apm_repo.sdk.units.content.saas_site
   api/synology_apm_repo.sdk.units.content.saas_teams_chat

.. toctree::
   :maxdepth: 2
   :caption: SaaS workloads (M365 / GWS)

   api/synology_apm_repo.sdk.units.saas.provider
   api/synology_apm_repo.sdk.units.saas.composite_provider
   api/synology_apm_repo.sdk.units.saas.stream
   api/synology_apm_repo.sdk.units.saas.tree_strategy.synthetic_grouped
   api/synology_apm_repo.sdk.units.saas.tree_strategy.named_group_flat
   api/synology_apm_repo.sdk.units.saas.tree_strategy.recursive
   api/synology_apm_repo.sdk.units.saas.tree_strategy.named_group_recursive
   api/synology_apm_repo.sdk.units.saas.tree_strategy.recursive_group_flat
   api/synology_apm_repo.sdk.units.saas.tree_strategy.categorized
   api/synology_apm_repo.sdk.units.saas.mail
   api/synology_apm_repo.sdk.units.saas.calendar
   api/synology_apm_repo.sdk.units.saas.contact
   api/synology_apm_repo.sdk.units.saas.drive
   api/synology_apm_repo.sdk.units.saas.site
   api/synology_apm_repo.sdk.units.saas.teams_chat
   api/synology_apm_repo.sdk.units.saas.services
   api/synology_apm_repo.sdk.units.saas.raw_object
   api/synology_apm_repo.sdk.units.saas.objectdb
   api/synology_apm_repo.sdk.units.saas.object_name_index

.. toctree::
   :maxdepth: 2
   :caption: Profiles & presentation

   api/synology_apm_repo.sdk.profiles
   api/synology_apm_repo.sdk.presentation.format
   api/synology_apm_repo.sdk.presentation.progress
   api/synology_apm_repo.sdk.presentation.markup
   api/synology_apm_repo.sdk.presentation.icons
   api/synology_apm_repo.sdk.presentation.logging_setup
   api/synology_apm_repo.sdk.presentation.export_target
