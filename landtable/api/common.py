"""
Common parameters that the API uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated
from typing import Any
from typing import TypeAlias

from fastapi import Depends
from fastapi import Response
from pydantic import BaseModel
from starlette.requests import Request

from landtable.state import LandtableState
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace


def state(request: Request):
    return request.app.state.landtable


State: TypeAlias = Annotated[LandtableState, Depends(state)]


async def workspace(request: Request, workspace_id: str):
    return await request.app.state.landtable.fetch_workspace(workspace_id)


Workspace: TypeAlias = Annotated[LandtableWorkspace, Depends(workspace)]


async def table(request: Request, workspace: Workspace, table_id: str):
    return await workspace.fetch_table(table_id)


Table: TypeAlias = Annotated[LandtableTable, Depends(table)]
