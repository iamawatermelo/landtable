import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, Any, List, Literal, Self, TypeAlias
import asyncpg
from fastapi import Depends, FastAPI, APIRouter, Query
from itertools import chain
from fastapi.exceptions import HTTPException
import uvicorn
import pydantic
import aetcd
import unicodedata
from uuid import UUID

class LandtableMeta(pydantic.BaseModel):
    version: Literal[1] = 1

class LandtableWorkspace(pydantic.BaseModel):
    version: Literal[1] = 1
    type: Literal["postgresql"] = "postgresql"
    url: str

class LandtableTable(pydantic.BaseModel):
    version: Literal[1] = 1
    table: str
    exposed_fields: List[str]
    created_time_field: str
    id_field: str
    
meta: LandtableMeta
etcd: aetcd.Client

@asynccontextmanager
async def setup(app: FastAPI):
    global meta
    global etcd
    
    etcd = aetcd.Client(host="localhost")
    await etcd.connect()
    
    meta_get = await etcd.get(b"/landtable/meta")
    meta = LandtableMeta.model_validate_json(meta_get.value)
    
    yield
    
    await etcd.close()

app = FastAPI(lifespan=setup)

airtable = APIRouter(prefix="/compat")

async def workspace_parameter(workspace_id: str):
    workspace = await etcd.get(f"/landtable/workspaces/{workspace_id}/meta".encode())
    
    if workspace is None:
        # TODO: use proper Airtable exceptions
        raise HTTPException(404, f"Workspace {workspace_id} does not exist")
    
    return LandtableWorkspace.model_validate_json(workspace.value)

async def table_parameter(workspace_id: str, table_id: str):
    table = await etcd.get(f"/landtable/workspaces/{workspace_id}/table/{table_id}".encode())
        
    if table is None:
        # TODO: use proper Airtable exceptions
        raise HTTPException(404, f"Table {workspace_id}/{table_id} does not exist")
    
    return LandtableTable.model_validate_json(table.value)

def row_parameter(row_id: str):
    if (ident := row_id[:3]) == "rec":
        raise HTTPException(400, "Airtable record IDs are not supported by Landtable yet")
    
    if (ident := row_id[:4]) != "lrw:":
        raise HTTPException(400, f"Invalid row identifier {ident} (expected something like lrw:XXXX...)")
    
    try:
        row_key = UUID(bytes=bytes.fromhex(row_id[4:]))
    except ValueError:
        raise HTTPException(400, f"Invalid row ID {row_id}")
    
    return row_key

Workspace = Depends(workspace_parameter)
Table = Depends(table_parameter)
Row = Depends(row_parameter)

def fmt_row(row: UUID):
    return f"lrw:{row.hex}"

@app.get("/v0/{workspace_id}/{table_id}/{row_id}")
async def fetch_row(
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row_uuid: UUID = Row
):
    fields = list(set(chain(table.exposed_fields, (table.created_time_field,))))
    
    db = await asyncpg.connect(workspace.url)
    row = await db.fetchrow(f"SELECT ({','.join(fields)}) FROM {table.table} WHERE {table.id_field} = $1", row_uuid)
    await db.close()
    assert row
    
    columns = row.get("row")
    assert columns
    
    return {
        "createdTime": columns[fields.index(table.created_time_field)],
        "fields": {field: columns[fields.index(field)] for field in table.exposed_fields},
        "id": fmt_row(row_uuid)
    }

class AirtableUpdateRowBody(pydantic.BaseModel):
    fields: dict[str, Any]

@app.patch("/v0/{workspace_id}/{table_id}/{row_id}")
async def patch_row(
    row_id: str,
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row: UUID = Row
):
    db = await asyncpg.connect(workspace.url)
    fields = body.fields.keys()
    for field in fields:
        if field not in table.exposed_fields:
            raise HTTPException(400, f"field {field} does not exist in table {workspace_id}/{table_id} or field is not configured to be exposed in etcd")
    
    fields_parsed = ','.join(f"{field} = ${idx+2}" for idx, field in enumerate(fields))
    maybe_row = await db.fetchrow(f"UPDATE {table.table} SET {fields_parsed} WHERE {table.id_field}=$1 RETURNING *", row, *body.fields.values())
    
    if maybe_row is None:
        raise HTTPException(404, f"row {row_id} does not exist in table {workspace_id}/{table_id}")
    
    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": {k: v for k, v in maybe_row.items() if k in table.exposed_fields},
        "id": fmt_row(maybe_row.get(table.id_field))
    }

@app.put("/v0/{workspace_id}/{table_id}/{row_id}")
async def overwrite_row(
    row_id: str,
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row: UUID = Row
):
    db = await asyncpg.connect(workspace.url)
    fields = body.fields.keys()
    for field in fields:
        if field not in table.exposed_fields:
            raise HTTPException(400, f"field {field} does not exist in table {workspace_id}/{table_id} or field is not configured to be exposed in etcd")
    
    fields_parsed = ','.join(chain(
        (f"{field} = ${idx+2}" for idx, field in enumerate(fields)),
        (f"{field} = DEFAULT" for idx, field in enumerate(table.exposed_fields) if field not in fields)
    ))
    maybe_row = await db.fetchrow(f"UPDATE {table.table} SET {fields_parsed} WHERE {table.id_field}=$1 RETURNING *", row, *body.fields.values())
    
    if maybe_row is None:
        raise HTTPException(404, f"row {row_id} does not exist in table {workspace_id}/{table_id}")
    
    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": {k: v for k, v in maybe_row.items() if k in table.exposed_fields},
        "id": fmt_row(maybe_row.get(table.id_field))
    }

@app.delete("/v0/{workspace_id}/{table_id}/{row_id}")
async def delete_row(
    row_id: str,
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row: UUID = Row
):
    db = await asyncpg.connect(workspace.url)
    status = await db.execute(f"DELETE FROM {table.table} WHERE {table.id_field}=$1", row)
    if status != "DELETE 1":
        raise HTTPException(404, f"row {row_id} does not exist in table {workspace_id}/{table_id}")
        
    return {
        "deleted": True,
        "id": row_id
    }

@app.post("/v0/{workspace_id}/{table_id}/")
async def put_row(
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table
):
    db = await asyncpg.connect(workspace.url)
    fields = body.fields.keys()
    for field in fields:
        if field not in table.exposed_fields:
            raise HTTPException(400, f"field {field} does not exist in table {workspace_id}/{table_id} or field is not configured to be exposed in etcd")
    
    fields_parsed = ','.join(fields)
    
    maybe_row = await db.fetchrow(f"INSERT INTO {table.table} ({fields_parsed}) VALUES ({','.join(f"${n+1}" for n in range(len(body.fields)))}) ON CONFLICT DO NOTHING RETURNING *", *body.fields.values())
    
    if maybe_row is None:
        raise HTTPException(400, "record conflicts with another record")
    
    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": {k: v for k, v in maybe_row.items() if k in table.exposed_fields},
        "id": fmt_row(maybe_row.get(table.id_field))
    }

@app.delete("/v0/{workspace_id}/{table_id}/")
async def delete_rows(
    workspace_id: str,
    table_id: str,
    row_ids: Annotated[list[str], Query(
        alias="records[]",
        max_length=10,
        min_length=1
    )],
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table
):
    rows = [row_parameter(x) for x in row_ids]
    
    if len(set(rows)) != len(rows):
        # TODO: check if Airtable does the same validation
        raise HTTPException(400, "duplicate records in supplied row ids")
    
    db = await asyncpg.connect(workspace.url)
    async with db.transaction():
        status = await db.fetch(f"DELETE FROM {table.table} WHERE {table.id_field} IN ({','.join(f"${n+1}" for n in range(len(rows)))}) RETURNING {table.id_field}", *rows)
        if len(status) != len(rows):
            raise HTTPException(404, "not all records were deleted, maybe because some do not exist")
    
    return {
        "records": [{
            "deleted": True,
            "id": fmt_row(row.get("id"))
        } for row in status]
    }
    

if __name__ == "__main__":
    uvicorn.run(app)