"""
Tools for managing Landtable's internal state.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from logging import getLogger
from typing import cast
from typing import Dict
from urllib.parse import urlparse

from aetcd import Client
from aetcd import Event

import landtable.state.models as models
from landtable.backends import BackendResolver
from landtable.backends.abstract import DatabaseBackend
from landtable.exceptions import APINotFoundException
from landtable.identifiers import DatabaseIdentifier
from landtable.identifiers import Identifier
from landtable.identifiers import TableIdentifier
from landtable.identifiers import WorkspaceIdentifier
from landtable.tracing import Tracer

logger = getLogger(__name__)


@dataclass
class CachedEntry[T]:
    created_at: float
    """
    Creation time of this cache entry, in seconds.
    """

    inner: T


class LandtableState:
    url: str
    db: Client
    workspace_cache: Dict[str, CachedEntry[models.LandtableWorkspace]]
    table_cache: Dict[str, CachedEntry[models.LandtableTable]]
    database_cache: Dict[str, CachedEntry[models.BaseLandtableDatabase]]
    meta: CachedEntry[models.LandtableMeta] | None
    task_obj: asyncio.Task | None
    cache_expiry_time: int = 10
    resolver: BackendResolver

    def __init__(self, url: str) -> None:
        parsed_url = urlparse(url, scheme="etcd", allow_fragments=False)

        self.url = url
        self.db = Client(
            host=parsed_url.hostname or "localhost", port=parsed_url.port or 2379
        )
        self.workspace_cache = dict()
        self.table_cache = dict()
        self.database_cache = dict()
        self.meta = None
        self.task_obj = None
        self.resolver = BackendResolver()

    async def task(self):
        async for event in await self.db.watch_prefix(b"/landtable"):
            event = cast(Event, event)  # grrgrgrrr aetcd has improper typing

            match event.kv.key.split(b"/")[2:]:
                case [b"meta"]:
                    self.meta = CachedEntry(
                        created_at=time.monotonic(),
                        inner=models.LandtableMeta(
                            state=self, **json.loads(event.kv.value)
                        ),
                    )
                case [b"workspaces", workspace_id, b"meta"]:
                    self.workspace_cache[str(workspace_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=models.LandtableWorkspace(
                            state=self, **json.loads(event.kv.value)
                        ),
                    )
                case [b"databases", database_id]:
                    self.database_cache[str(database_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=models.BaseLandtableDatabase(
                            state=self, **json.loads(event.kv.value)
                        ),
                    )
                case [b"workspaces", _, b"tables", table_id]:
                    self.table_cache[str(table_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=models.LandtableTable(**json.loads(event.kv.value)),
                    )
                case _:
                    logger.warn(
                        f"Received unknown etcd update event: {event.kv.key.decode()}"
                    )

    async def connect(self):
        with Tracer.from_context().trace("etcd", "etcd connect"):
            await self.db.connect()

        await self.resolver.initialise()

        self.task_obj = asyncio.create_task(self.task())

    async def shutdown(self):
        assert self.task_obj, "connect() never called"

        self.task_obj.cancel()
        await self.db.close()

    async def fetch_database(
        self, database: DatabaseIdentifier
    ) -> tuple[models.BaseLandtableDatabase, DatabaseBackend]:
        if (
            entry := self.database_cache.get(str(database))
        ) is not None and time.monotonic() - entry.created_at < self.cache_expiry_time:
            Tracer.from_context().instant_event(
                "configFetch", f"cache hit on {database}"
            )

            return entry.inner, self.resolver.fetch_backend_for_config_type(
                entry.inner.type
            )

        with Tracer.from_context().trace("configFetch", f"cache miss on {database}"):
            database_bytes = await self.db.get(
                f"/landtable/databases/{database}".encode()
            )

            if database_bytes is None:
                raise APINotFoundException(
                    message=f"database {database} does not exist"
                )

            resolved_database = models.BaseLandtableDatabase(
                state=self, **json.loads(database_bytes.value)
            )

            cache_entry = CachedEntry(
                created_at=time.monotonic(), inner=resolved_database
            )

            self.database_cache[str(database)] = cache_entry

        return resolved_database, self.resolver.fetch_backend_for_config_type(
            resolved_database.type
        )

    async def fetch_workspace(
        self, workspace: str | WorkspaceIdentifier
    ) -> models.LandtableWorkspace:
        """
        Fetch a workspace. Do not cache the result of this call.
        Raises an exception if the workspace could not be found.
        """

        if (
            entry := self.workspace_cache.get(str(workspace))
        ) is not None and time.monotonic() - entry.created_at < self.cache_expiry_time:
            Tracer.from_context().instant_event(
                "configFetch", f"cache hit on {workspace}"
            )
            return entry.inner

        with Tracer.from_context().trace("configFetch", f"cache miss on {workspace}"):
            if not isinstance(workspace, Identifier):
                if workspace[:4] != "lwk:":
                    logger.debug(
                        f'workspace "{workspace}" does not begin with lwk:, looking for aliases'
                    )

                    alias = await self.db.get(
                        f"/landtable/workspaceAliases/{workspace}".encode()
                    )

                    if alias is None:
                        raise APINotFoundException(
                            message=f"workspace {workspace} does not exist"
                        )

                    workspace = alias.value.decode()

            workspace = str(workspace)

            workspace_bytes = await self.db.get(
                f"/landtable/workspaces/{workspace}/meta".encode()
            )

            if workspace_bytes is None:
                raise APINotFoundException(
                    message=f"workspace {workspace} does not exist"
                )

            resolved_workspace = models.LandtableWorkspace(
                state=self, **json.loads(workspace_bytes.value)
            )

            cache_entry = CachedEntry(
                created_at=time.monotonic(), inner=resolved_workspace
            )

            self.workspace_cache[str(resolved_workspace.id)] = cache_entry
            self.workspace_cache[resolved_workspace.name] = cache_entry

        return resolved_workspace

    async def fetch_table(
        self, workspace_id: WorkspaceIdentifier, table: str | TableIdentifier
    ) -> models.LandtableTable:
        """
        Fetch a table. Do not cache the result of this call.
        Does not check for workspace aliases.
        """

        if (
            entry := self.table_cache.get(str(table))
        ) is not None and time.monotonic() - entry.created_at < self.cache_expiry_time:
            Tracer.from_context().instant_event(
                "configFetch", f"cache hit on {workspace_id}/{table}"
            )
            return entry.inner

        with Tracer.from_context().trace(
            "configFetch", f"cache miss on {workspace_id}/{table}"
        ):
            if not isinstance(table, Identifier):
                if table[:4] != "ltb:":
                    alias = await self.db.get(
                        f"/landtable/workspaces/{workspace_id}/tableAliases/{table}".encode()
                    )

                    if alias is None:
                        raise APINotFoundException(
                            message=f"table {workspace_id}/{table} does not exist"
                        )

                    table = alias.value.decode()

            table = str(table)

            table_bytes = await self.db.get(
                f"/landtable/workspaces/{workspace_id}/tables/{table}".encode()
            )

            if table_bytes is None:
                raise APINotFoundException(
                    message=f"table {workspace_id}/{table} does not exist"
                )

            resolved_table = models.LandtableTable(**json.loads(table_bytes.value))

            cache_entry = CachedEntry(created_at=time.monotonic(), inner=resolved_table)

            self.table_cache[str(resolved_table.id)] = cache_entry
            self.table_cache[resolved_table.name] = cache_entry

        return resolved_table
