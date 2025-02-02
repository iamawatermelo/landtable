"""
Quick frontend to work with Landtable.
"""

from datetime import date
import os
from pathlib import Path
from typing import Annotated, cast
from html import escape

from fastapi import APIRouter, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
import aiohttp
import importlib.resources as resources

from landtable.api.common import Authentication, Core
from landtable.core.models.transactions import DeleteOperation, ReadOperation, RowResult, RowTarget, TransactionModel, WriteOperation
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

async def verify_recaptcha(recaptcha: str):
    async with aiohttp.ClientSession() as c:
        async with c.post(
            "https://www.google.com/recaptcha/api/siteverify",
            params={
                "secret": os.environ["LT_HS_RECAPTCHA_TOKEN"],
                "response": recaptcha
            }
        ) as response:
            response.raise_for_status()
            result = await response.json()
            assert result.get("success")
            assert result.get("score") > 0.5

@app.post("/write-record")
async def htmx_write_record(
    core: Core,
    auth: Authentication,
    name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    comments: Annotated[str, Form()],
    recaptcha: Annotated[str, Form(alias="g-recaptcha-response")]
):
    print(name, email, comments)
    
    if len(name) + len(email) + len(comments) > 1024:
        return HTMLResponse(
            status_code=200,
            content="<p>While Landtable itself doesn't enforce any character limits, there is a limit for this demo.</p>"
        )
    
    try:
        name.encode("ASCII")
        email.encode("ASCII")
        comments.encode("ASCII")
    except UnicodeEncodeError:
        return HTMLResponse(
            status_code=200,
            content="<p>While Landtable itself allows any valid Unicode string, this demo only accepts ASCII for content moderation purposes.</p>"
        )
    
    try:
        await verify_recaptcha(recaptcha)
    except AssertionError:
        return HTMLResponse(
            status_code=200,
            content="<p>Sorry, Google thinks you're a robot.</p>"
        )
    
    async with aiohttp.ClientSession() as c:
        async with c.post(
            "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze",
            params={
                "key": os.environ["LT_HS_PERSPECTIVE_TOKEN"]
            },
            json={
                "comment": {
                    "text": f"Name: {name}\nEmail: {email}\nComment: {comments}",
                    "type": "PLAIN_TEXT"
                },
                "languages": ["en"],
                "doNotStore": True,
                
                # let's not have a repeat of that one High Seas notebook
                "requestedAttributes": {
                    "SEVERE_TOXICITY": {
                        "scoreThreshold": 0.5
                    },
                    "TOXICITY": {
                        "scoreThreshold": 0.5
                    },
                    "IDENTITY_ATTACK": {
                        "scoreThreshold": 0.5
                    },
                    "FLIRTATION": {
                        "scoreThreshold": 0.5
                    },
                    "SEXUALLY_EXPLICIT": {
                        "scoreThreshold": 0.5
                    }
                }
            }
        ) as response:
            response.raise_for_status()
            result = await response.json()
            if len(result.get("attributeScores", [])) != 0:
                # scores = result["attributeScores"]
                return HTMLResponse(
                    status_code=200,
                    #content=f"<p>Sorry, Google thinks that's: {", ".join(f"{k} ({int(v["summaryScore"]["value"]*100)}%)" for k, v in scores.items())}</p>"
                    content="<p>Sorry, Google thinks that's inappropriate. Try something else.</p>"
                )
    
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

@app.post("/delete-record")
async def htmx_delete_record(
    core: Core,
    auth: Authentication,
    row_id: Annotated[str, Form()],
    recaptcha: Annotated[str, Form(alias="g-recaptcha-response")]
):
    try:
        await verify_recaptcha(recaptcha)
    except AssertionError:
        return HTMLResponse(
            status_code=200,
            content="<p>Sorry, Google thinks you're a robot.</p>"
        )
    
    try:
        row_ident = Identifier.parse_from_ns("lrw", row_id.strip())
    except ValueError as e:
        return HTMLResponse(
            status_code=200,
            content=f"<p>Sorry, that row ID doesn't look right: {e}</p>"
        )
    
    with auth.enter():
        workspace = await core.fetch_workspace("highseas")
        await core.execute_txn(
            workspace,
            Identifier.parse_from("ltb:743d16834d574a11cd5d4425bf60c223"),
            TransactionModel(
                ops=[
                    DeleteOperation(
                        type="delete",
                        target=RowTarget(
                            type="row",
                            row=row_ident
                        )
                    )
                ]
            )
        )
        
    return Response(
        status_code=200,
        headers={
            "hx-refresh": "true"
        }
    )