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
from landtable.auth.unsafe import UnsafeAuthenticationPlugin
from landtable.core import Landtable
from landtable.core.models.workspaces import WorkspaceModel
from landtable.exceptions import APIUnauthorized

logger = getLogger(__name__)


async def authenticate(request: Request, core: Core):
    """
    For now, disable authentication.
    """
    
    return await UnsafeAuthenticationPlugin().create_context(request)


Authentication: TypeAlias = Annotated[AuthenticationContext, Depends(authenticate)]


def core(request: Request):
    return request.app.state.landtable


Core: TypeAlias = Annotated[Landtable, Depends(core)]


async def workspace(request: Request, workspace_id: str, auth: Authentication):
    with auth.enter():
        return await request.app.state.landtable.fetch_workspace(workspace_id)


Workspace: TypeAlias = Annotated[WorkspaceModel, Depends(workspace)]
