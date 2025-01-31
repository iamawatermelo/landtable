"""
Quick frontend to work with Landtable.
"""

from datetime import date
from pathlib import Path
from typing import Annotated, cast
from html import escape

from fastapi import APIRouter, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
import importlib.resources as resources

from landtable.api.common import Authentication, Core
from landtable.core.models.transactions import ReadOperation, RowResult, TransactionModel, WriteOperation
from landtable.identifiers import Identifier

app = APIRouter(prefix="/hs")

@app.get("/load-db")
async def htmx_load_db(core: Core, auth: Authentication):
    with auth.enter():
        workspace = await core.fetch_workspace("highseas")
        [result] = cast(list[list[RowResult]], await core.execute_txn(
            workspace,
            Identifier.parse_from("ltb:743d16834d574a11cd5d4425bf60c223"),
            TransactionModel(
                ops=[
                    ReadOperation(
                        type="read",
                        target=None
                    )
                ]
            )
        ))
        
        html = (
            "<table>"
            "<tr>"
            "<th>ID</th>"
            "<th>Created at</th>"
            "<th>Content</th>"
            "</tr>"
        )
        
        for row in result:
            html += (
                "<tr>"
                f"<td>{row.id}</td>"
                f"<td>{row.created_at}</td>"
                f"<td>{escape(str(row.row))}</td>"
                "</tr>"
            )
        
        html += "</table>"
    
    return HTMLResponse(html)

@app.post("/write-record")
async def htmx_write_record(
    core: Core,
    auth: Authentication,
    name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    comments: Annotated[str, Form()]
):
    with auth.enter():
        workspace = await core.fetch_workspace("highseas")
        await core.execute_txn(
            workspace,
            Identifier.parse_from("ltb:743d16834d574a11cd5d4425bf60c223"),
            TransactionModel(
                ops=[
                    WriteOperation(
                        type="write",
                        row={
                            "Name": name,
                            "Email": email,
                            "Comment": comments
                        }
                    )
                ]
            )
        )

    return Response(
        status_code=201,
        headers={
            "hx-refresh": "true"
        }
    )