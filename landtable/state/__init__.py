"""
Tools for managing Landtable's internal state.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
import asyncio
import json
import time
from dataclasses import dataclass
from logging import getLogger
from typing import cast
from typing import Dict

from aetcd import Client
from aetcd import Event

from landtable.exceptions import APINotFoundException
from landtable.state.models import BaseLandtableDatabase
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtableMeta
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace
from landtable.tracing import debug_tracer
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
    workspace_cache: Dict[str, CachedEntry[LandtableWorkspace]]
    table_cache: Dict[str, CachedEntry[LandtableTable]]
    meta: CachedEntry[LandtableMeta]
    task_obj: asyncio.Task
    cache_expiry_time: int = 10

    def __init__(self, url: str) -> None:
        self.db = Client(url)

    async def task(self):
        async for event in await self.db.watch_prefix(b"/landtable"):
            event = cast(Event, event)  # grrgrgrrr aetcd has improper typing

            match event.kv.key.split(b"/")[2:]:
                case [b"meta"]:
                    self.meta = CachedEntry(
                        created_at=time.monotonic(),
                        inner=LandtableMeta(state=self, **json.loads(event.kv.value)),
                    )
                case [b"workspaces", workspace_id, b"meta"]:
                    self.workspace_cache[str(workspace_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=LandtableWorkspace(
                            state=self, **json.loads(event.kv.value)
                        ),
                    )
                case [b"databases", database_id]:
                    self.database_cache[str(database_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=BaseLandtableDatabase(
                            state=self, **json.loads(event.kv.value)
                        ),
                    )
                case [b"workspaces", _, b"tables", table_id]:
                    self.table_cache[str(table_id)] = CachedEntry(
                        created_at=time.monotonic(),
                        inner=LandtableTable(**json.loads(event.kv.value)),
                    )
                case _:
                    logger.warn(
                        f"Received unknown etcd update event: {event.kv.key.decode()}"
                    )

    async def connect(self):
        with debug_tracer("connect"):
            await self.db.connect()

        self.task_obj = asyncio.create_task(self.task())

    async def shutdown(self):
        self.task_obj.cancel()
        await self.db.close()

    async def fetch_workspace(self, workspace: str) -> LandtableWorkspace:
        """
        Fetch a workspace. Do not cache the result of this call.
        Raises an exception if the workspace could not be found.
        """

        if (
            entry := self.workspace_cache.get(workspace)
        ) is not None and time.monotonic() - entry.created_at < self.cache_expiry_time:
            Tracer.from_context().instant_event(
                "configFetch", f"cache hit on {workspace}"
            )
            return entry.inner

        with Tracer.from_context().trace("configFetch", f"cache miss on {workspace}"):
            if workspace[4:] != "lwk:":
                alias = await self.db.get(
                    f"/landtable/workspaceAliases/{workspace}".encode()
                )

                if alias is None:
                    raise APINotFoundException(
                        message=f"workspace {workspace} does not exist"
                    )

                workspace = alias.value.decode()

            workspace_bytes = await self.db.get(
                f"/landtable/workspaces/{workspace}".encode()
            )

            if workspace_bytes is None:
                raise APINotFoundException(
                    message=f"workspace {workspace} does not exist"
                )

            resolved_workspace = LandtableWorkspace(
                state=self, **json.loads(workspace_bytes.value)
            )

            cache_entry = CachedEntry(
                created_at=time.monotonic(), inner=resolved_workspace
            )

            self.workspace_cache[resolved_workspace.lt_id] = cache_entry
            self.workspace_cache[resolved_workspace.lt_name] = cache_entry

        return resolved_workspace

    async def fetch_table(self, workspace_id: str, table: str) -> LandtableTable:
        """
        Fetch a table. Do not cache the result of this call.
        Does not check for workspace aliases.
        """

        if (
            entry := self.table_cache.get(table)
        ) is not None and time.monotonic() - entry.created_at < self.cache_expiry_time:
            Tracer.from_context().instant_event(
                "configFetch", f"cache hit on {workspace_id}/{table}"
            )
            return entry.inner

        with Tracer.from_context().trace(
            "configFetch", f"cache miss on {workspace_id}/{table}"
        ):
            if table[4:] != "ltb:":
                alias = await self.db.get(
                    f"/landtable/workspaces/{workspace_id}/tableAliases/{table}".encode()
                )

                if alias is None:
                    raise APINotFoundException(
                        message=f"table {workspace_id}/{table} does not exist"
                    )

                table = alias.value.decode()

            table_bytes = await self.db.get(
                f"/landtable/workspaces/{workspace_id}/tables/{table}".encode()
            )

            if table_bytes is None:
                raise APINotFoundException(
                    message=f"table {workspace_id}/{table} does not exist"
                )

            resolved_table = LandtableTable(**json.loads(table_bytes.value))

            cache_entry = CachedEntry(created_at=time.monotonic(), inner=resolved_table)

            self.table_cache[resolved_table.lt_id] = cache_entry
            self.table_cache[resolved_table.lt_name] = cache_entry

        return resolved_table
