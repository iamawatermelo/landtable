"""
Landtable's CLI.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from contextlib import asynccontextmanager
from functools import cached_property
from itertools import chain
from typing import Annotated
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Union
from uuid import UUID

import aetcd
import asyncpg
import pydantic
import uvicorn
from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Query
from fastapi.exceptions import HTTPException

from landtable.formula.exceptions import FormulaException
from landtable.formula.formula import Formula
from landtable.formula.parse import ASTConcreteType
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.sql import to_sql
from landtable.formula.sql.functions import SQL_FUNCTIONS


class LandtableMeta(pydantic.BaseModel):
    version: Literal[1] = 1


class LandtableWorkspace(pydantic.BaseModel):
    version: Literal[1] = 1

    type: Literal["postgresql"] = "postgresql"
    """
    The type of this database connection.
    """

    url: str
    """
    URI for this database connection (postgres://...)
    """

    lt_name: str
    """
    What Landtable will call this workspace.
    """

    lt_id: str
    """
    An immutable ID for this workspace.
    """


class LandtableField(pydantic.BaseModel, frozen=True):
    db_name: str
    """
    What this field is called in the database.
    """

    lt_name: str
    """
    What Landtable will call this field.
    """

    lt_id: str
    """
    An immutable ID for this field (lfd:...).
    """

    type: Union[
        Literal["attachment"],
        Literal["autonumber"],
        Literal["barcode"],
        Literal["string"],
        Literal["boolean"],
        Literal["count"],
        Literal["created_at"],
        Literal["created_by"],
        Literal["currency"],
        Literal["datetime"],
        Literal["duration"],
        Literal["email"],
        Literal["modified_by"],
        Literal["modified_time"],
        Literal["linked"],
        Literal["long_text"],
        Literal["lookup"],
        Literal["multi_select"],
        Literal["number"],
        Literal["percentage"],
        Literal["phone_number"],
        Literal["rating"],
        Literal["short_text"],
        Literal["select"],
        Literal["url"],
        Literal["user"],
    ]
    """
    The type of this field.
    """

    def type_to_ast_type(self):
        if typ := {
            "number": ASTConcreteType.NUMBER,
            "short_text": ASTConcreteType.STRING,
            "long_text": ASTConcreteType.STRING,
            "boolean": ASTConcreteType.BOOLEAN,
            "datetime": ASTConcreteType.DATETIME,
        }.get(self.type):
            return typ
        else:
            raise Exception(f"don't know how to handle type {self.type}")


class LandtableTable(pydantic.BaseModel):
    version: Literal[1] = 1

    table: str
    """
    What this table is called in the database.
    """

    lt_name: str
    """
    What Landtable will call this table.
    """

    lt_id: str
    """
    An immutable ID for this field.
    """

    exposed_fields: List[LandtableField]
    """
    A list of fields for this table.
    More fields are allowed to exist in the underlying database, but Landtable
    will never touch them. If any of those fields are NOT NULL and don't have
    a default, Landtable will never be able to write to the database.
    """

    created_time_field: str
    """
    The datatime field that represents the created time field. Note that
    Landtable does not insert a created time itself—it expects the database
    to do this.
    """

    id_field: str
    """
    The UUID field that represents the ID.
    """

    @cached_property
    def field_name_to_field_map(self):
        """
        Map of field names to field objects
        """

        return {field.lt_name: field for field in self.exposed_fields}

    @cached_property
    def field_id_to_field_map(self):
        """
        Map of field IDs to field objects
        """

        return {field.lt_id: field for field in self.exposed_fields}

    @cached_property
    def field_to_field_map(self):
        """
        Map of field names and IDs to field objects
        """

        return {**self.field_name_to_field_map, **self.field_id_to_field_map}

    @cached_property
    def db_column_to_field_map(self):
        """
        Map of database columns to field objects
        """

        return {field.db_name: field for field in self.exposed_fields}


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
    table = await etcd.get(
        f"/landtable/workspaces/{workspace_id}/table/{table_id}".encode()
    )

    if table is None:
        # TODO: use proper Airtable exceptions
        raise HTTPException(404, f"Table {workspace_id}/{table_id} does not exist")

    return LandtableTable.model_validate_json(table.value)


def row_parameter(row_id: str):
    if (ident := row_id[:3]) == "rec":
        raise HTTPException(
            400, "Airtable record IDs are not supported by Landtable yet"
        )

    if (ident := row_id[:4]) != "lrw:":
        raise HTTPException(
            400, f"Invalid row identifier {ident} (expected something like lrw:XXXX...)"
        )

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


def strip_record(record: Dict[str, Any]):
    # This is a terrible anti-feature of Airtable.
    # Unfortunately, we must be compatible
    return {k: v for k, v in record.items() if v not in (0, False, None, [])}


@app.get("/v0/{workspace_id}/{table_id}")
async def fetch_rows(
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    page_size: Annotated[int, Query(gt=0, le=100, alias="pageSize")] = 100,
    filter_by_formula: Annotated[str | None, Query(alias="filterByFormula")] = None,
    fields: Annotated[list[str], Query()] = [],
    use_id: Annotated[bool, Query(alias="returnFieldsByFieldId")] = False,
):
    db = await asyncpg.connect(workspace.url)

    if fields == []:
        columns = {field.db_name for field in table.field_id_to_field_map.values()}
        requested_fields = set(table.field_id_to_field_map.values())
    else:
        columns = {
            field.db_name
            for name, field in table.field_to_field_map.items()
            if name in fields
        }
        requested_fields = {
            field
            for name, field in table.field_id_to_field_map.items()
            if name in fields
        }

    columns |= {table.id_field, table.created_time_field}
    columns = list(columns)
    print(f"fetching {columns}")

    if filter_by_formula is None:
        rows = await db.fetch(
            f"SELECT ({','.join(columns)} FROM {table.table} LIMIT {page_size}"
        )
    else:
        env = ASTTypeEnvironment(
            variables={
                name: field.type_to_ast_type()
                for name, field in table.field_to_field_map.items()
            },
            functions=SQL_FUNCTIONS,
            id_field=table.id_field,
            created_time_field=table.created_time_field,
        )

        try:
            formula = Formula(filter_by_formula)
            sql, values = to_sql(formula, env)
            print(sql)

            rows = await db.fetch(
                f"SELECT ({','.join(columns)}) FROM {table.table} WHERE {sql} LIMIT {page_size}",
                *values,
            )
        except FormulaException as e:
            raise HTTPException(400, f"error while parsing or evaluating formula: {e}")
        except asyncpg.PostgresSyntaxError as e:
            raise HTTPException(
                500,
                f"internal error {e} while executing SQL (executed SQL: {sql}, values: {values})",
            )

    return {
        "records": [
            {
                "createdTime": x["row"][columns.index(table.created_time_field)],
                "fields": strip_record(
                    {
                        (field.lt_id if use_id else field.lt_name): x["row"][
                            columns.index(field.db_name)
                        ]
                        for field in requested_fields
                    }
                ),
                "id": fmt_row(x["row"][columns.index(table.id_field)]),
            }
            for x in rows
        ]
    }


@app.get("/v0/{workspace_id}/{table_id}/{row_id}")
async def fetch_row(
    workspace_id: str,
    table_id: str,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row_uuid: UUID = Row,
    use_id: Annotated[bool, Query(alias="returnFieldsByFieldId")] = False,
):
    fields = list(set(chain(table.exposed_fields, (table.created_time_field,))))

    db = await asyncpg.connect(workspace.url)
    fields, field_map = remap_fields(
        table.exposed_fields, [table.created_time_field, table.id_field], use_id, None
    )

    row = await db.fetchrow(
        f"SELECT ({','.join(fields)}) FROM {table.table} WHERE {table.id_field} = $1",
        row_uuid,
    )
    await db.close()

    if row is None:
        raise HTTPException(
            404, f"row {row_uuid} does not exist in table {workspace_id}/{table_id}"
        )

    columns = row.get("row")
    assert columns

    return {
        "createdTime": columns["row"][fields.index(table.created_time_field)],
        "fields": strip_record(
            {
                field_name: columns["row"][fields.index(field.db_name)]
                for field_name, field in field_map.items()
            }
        ),
        "id": fmt_row(columns["row"][fields.index(table.id_field)]),
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
    row: UUID = Row,
):
    db = await asyncpg.connect(workspace.url)
    fields, field_map = remap_fields(
        table.exposed_fields,
        [table.created_time_field, table.id_field],
        False,
        list(body.fields.keys()),
    )

    _, field_id_map = remap_fields(
        table.exposed_fields,
        [table.created_time_field, table.id_field],
        True,
        list(body.fields.keys()),
    )

    for field in body.fields.keys():
        if field not in field_map.keys() or field not in field_id_map.keys():
            raise HTTPException(
                400,
                f"field {field} does not exist in table {workspace_id}/{table_id}",
            )

    maybe_row = await db.fetchrow(
        f"UPDATE {table.table} SET ({','.join(fields)}) WHERE {table.id_field}=$1 RETURNING *",
        row,
        *body.fields.values(),
    )

    if maybe_row is None:
        raise HTTPException(
            404, f"row {row_id} does not exist in table {workspace_id}/{table_id}"
        )

    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": strip_record(
            {k: v for k, v in maybe_row.items() if k in table.exposed_fields}
        ),
        "id": fmt_row(maybe_row.get(table.id_field)),
    }


@app.put("/v0/{workspace_id}/{table_id}/{row_id}")
async def overwrite_row(
    row_id: str,
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row: UUID = Row,
):
    db = await asyncpg.connect(workspace.url)
    fields, field_map = remap_fields(
        table.exposed_fields, [table.created_time_field, table.id_field], False
    )

    _, field_id_map = remap_fields(
        table.exposed_fields,
        [table.created_time_field, table.id_field],
        True,
        list(body.fields.keys()),
    )

    for field in body.fields.keys():
        if field not in field_map.keys() or field not in field_id_map.keys():
            raise HTTPException(
                400,
                f"field {field} does not exist in table {workspace_id}/{table_id}",
            )

    fields_parsed = ",".join(
        chain(
            (f"{field} = ${idx+2}" for idx, field in enumerate(fields)),
            (
                f"{field} = DEFAULT"
                for idx, field in enumerate(table.exposed_fields)
                if field not in fields
            ),
        )
    )
    maybe_row = await db.fetchrow(
        f"UPDATE {table.table} SET {fields_parsed} WHERE {table.id_field}=$1 RETURNING *",
        row,
        *body.fields.values(),
    )

    if maybe_row is None:
        raise HTTPException(
            404, f"row {row_id} does not exist in table {workspace_id}/{table_id}"
        )

    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": strip_record(
            {k: v for k, v in maybe_row.items() if k in table.exposed_fields}
        ),
        "id": fmt_row(maybe_row.get(table.id_field)),
    }


@app.delete("/v0/{workspace_id}/{table_id}/{row_id}")
async def delete_row(
    row_id: str,
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
    row: UUID = Row,
):
    db = await asyncpg.connect(workspace.url)
    status = await db.execute(
        f"DELETE FROM {table.table} WHERE {table.id_field}=$1", row
    )
    if status != "DELETE 1":
        raise HTTPException(
            404, f"row {row_id} does not exist in table {workspace_id}/{table_id}"
        )

    return {"deleted": True, "id": row_id}


@app.post("/v0/{workspace_id}/{table_id}/")
async def put_row(
    workspace_id: str,
    table_id: str,
    body: AirtableUpdateRowBody,
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
):
    db = await asyncpg.connect(workspace.url)
    fields = body.fields.keys()
    for field in fields:
        if field not in table.exposed_fields:
            raise HTTPException(
                400,
                f"field {field} does not exist in table {workspace_id}/{table_id} or field is not configured to be exposed in etcd",
            )

    fields_parsed = ",".join(fields)

    maybe_row = await db.fetchrow(
        f"INSERT INTO {table.table} ({fields_parsed}) VALUES ({','.join(f"${n+1}" for n in range(len(body.fields)))}) ON CONFLICT DO NOTHING RETURNING *",
        *body.fields.values(),
    )

    if maybe_row is None:
        raise HTTPException(400, "record conflicts with another record")

    return {
        "createdTime": maybe_row.get(table.created_time_field),
        "fields": strip_record(
            {k: v for k, v in maybe_row.items() if k in table.exposed_fields}
        ),
        "id": fmt_row(maybe_row.get(table.id_field)),
    }


@app.delete("/v0/{workspace_id}/{table_id}/")
async def delete_rows(
    workspace_id: str,
    table_id: str,
    row_ids: Annotated[
        list[str], Query(alias="records[]", max_length=10, min_length=1)
    ],
    workspace: LandtableWorkspace = Workspace,
    table: LandtableTable = Table,
):
    rows = [row_parameter(x) for x in row_ids]

    if len(set(rows)) != len(rows):
        # TODO: check if Airtable does the same validation
        raise HTTPException(400, "duplicate records in supplied row ids")

    db = await asyncpg.connect(workspace.url)
    async with db.transaction():
        status = await db.fetch(
            f"DELETE FROM {table.table} WHERE {table.id_field} IN ({','.join(f"${n+1}" for n in range(len(rows)))}) RETURNING {table.id_field}",
            *rows,
        )
        if len(status) != len(rows):
            raise HTTPException(
                404, "not all records were deleted, maybe because some do not exist"
            )

    return {
        "records": [{"deleted": True, "id": fmt_row(row.get("id"))} for row in status]
    }


if __name__ == "__main__":
    uvicorn.run(app)
