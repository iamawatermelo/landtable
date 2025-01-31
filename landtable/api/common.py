"""
Common parameters that the API uses.
"""

from __future__ import annotations

from logging import getLogger
from typing import Annotated
from typing import TypeAlias

from fastapi import Depends
from starlette.requests import Request

from landtable.auth.abstract import AuthenticationContext
from landtable.exceptions import APIUnauthorized

logger = getLogger(__name__)


async def authenticate(request: Request, state: State):
    meta = await state.fetch_meta()

    for name, _ in meta.auth_plugins.items():
        logger.debug(f"Trying {name} on {request}")
        plugin = state.auth.plugins.get(name)
        assert plugin, f"Auth plugin {name} doesn't exist"

        maybe_context = await plugin.create_context(request)

        if maybe_context is not None:
            return maybe_context

    raise APIUnauthorized()


Authentication: TypeAlias = Annotated[AuthenticationContext, Depends(authenticate)]


def state(request: Request):
    return request.app.state.landtable


State: TypeAlias = Annotated[LandtableState, Depends(state)]


async def workspace(request: Request, workspace_id: str, auth: Authentication):
    with auth.enter():
        return await request.app.state.landtable.fetch_workspace(workspace_id)


Workspace: TypeAlias = Annotated[LandtableWorkspace, Depends(workspace)]


async def table(
    request: Request, workspace: Workspace, table_id: str, auth: Authentication
):
    with auth.enter():
        return await workspace.fetch_table(table_id)


Table: TypeAlias = Annotated[LandtableTable, Depends(table)]
