"""
Common parameters that the API uses.
"""

from typing import Annotated
from typing import TypeAlias

from fastapi import Depends
from starlette.requests import Request

from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace


async def workspace(request: Request, workspace_id: str):
    return await request.state.landtable.fetch_workspace(workspace_id)


Workspace: TypeAlias = Annotated[LandtableWorkspace, Depends(workspace)]


async def table(request: Request, workspace: Workspace, table_id: str):
    return await workspace.fetch_table(table_id)


Table: TypeAlias = Annotated[LandtableTable, Depends(table)]
