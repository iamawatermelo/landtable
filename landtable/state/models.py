"""
Landtable's internal state, stored in etcd
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from collections.abc import Collection
from logging import getLogger
from typing import Annotated
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import Union

import pydantic

from landtable.exceptions import APIBadRequestException
from landtable.formula.parse import ASTConcreteType
from landtable.identifiers import DatabaseIdentifier
from landtable.identifiers import FieldIdentifier
from landtable.identifiers import Identifier
from landtable.identifiers import TableIdentifier
from landtable.identifiers import WorkspaceIdentifier

if TYPE_CHECKING:
    from landtable.state import LandtableState
else:
    LandtableState = None


logger = getLogger(__name__)


FieldType: TypeAlias = Union[
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
    Literal["user"]
]


class LandtableMeta(pydantic.BaseModel):
    """
    Configuration for Landtable.
    """

    state: Annotated[LandtableState, pydantic.SkipValidation]
    version: Literal[1]

    auth_plugins: dict[str, dict[str, Any]]
    """
    Which authentication plugins to use.
    """


class LandtableFieldReplicaConfig(pydantic.BaseModel, frozen=True):
    """
    Database configuration for a Landtable field.
    """

    model_config = pydantic.ConfigDict(extra="allow")

    column_name: str
    """
    The name of the underlying database column.
    """


class LandtableField(pydantic.BaseModel):
    """
    A field in a table. A field has a certain type, like "attachment",
    and can have configurable database options.
    """

    name: str
    """
    What Landtable will call this field.
    """

    id: FieldIdentifier
    """
    An immutable ID for this field (lfd:...).
    """

    type: FieldType
    """
    The type of this field.
    """

    replica_config: Dict[DatabaseIdentifier, LandtableFieldReplicaConfig]

    def type_to_ast_type(self):
        if typ := {
            "number": ASTConcreteType.NUMBER,
            "short_text": ASTConcreteType.STRING,
            "long_text": ASTConcreteType.STRING,
            "boolean": ASTConcreteType.BOOLEAN,
            "datetime": ASTConcreteType.DATETIME,
            "email": ASTConcreteType.STRING,
        }.get(self.type):
            return typ
        else:
            raise Exception(f"don't know how to handle type {self.type}")

    def fetch_replica_config(self, replica: Identifier):
        """
        Fetch the LandtableFieldReplicaConfig for this replica.
        """

        if config := self.replica_config.get(replica):
            return config

        raise Exception(f"no replica config for replica {replica} on field {self.id}")


class LandtableTable(pydantic.BaseModel):
    """
    Configuration for a Landtable table.
    """

    version: Literal[1] = 1
    state: Annotated[LandtableState, pydantic.SkipValidation]

    read_only: bool
    """
    Whether this table is read only. Writes to this table will be rejected.
    """

    name: str
    """
    What Landtable will call this table.
    """

    id: TableIdentifier
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

    replica_config: Dict[DatabaseIdentifier, dict]

    def fetch_replica_config(self, replica: Identifier):
        """
        Fetch the replica config dict for this replica. 
        """

        if (config := self.replica_config.get(replica)) is not None:
            return config
        
        raise Exception(f"no replica configuration for replica {replica} on table {self.id}")

    def create_field_map(
        self, fields: Collection[str | FieldIdentifier]
    ) -> dict[str | FieldIdentifier, LandtableField]:
        """
        From a list of either field IDs or field names, create a dict mapping
        the name to the field.
        """

        ret_names = {
            field.name: field for field in self.exposed_fields if field.name in fields
        }

        ret_ids = {
            field.id: field for field in self.exposed_fields if field.id in fields
        }

        return {**ret_names, **ret_ids}

    def resolve_fields(self, fields: Collection[str | FieldIdentifier] | None):
        """
        From a list of either field IDs or field names, get their fields.
        Duplicate fields and missing fields are checked.
        """

        if fields is None:
            return self.exposed_fields
        else:
            non_unique_fields = [
                field.id
                for field in self.exposed_fields
                if field.name in fields or field.id in fields
            ]
            
            if len(non_unique_fields) != len(fields):
                raise APIBadRequestException(message=f"some fields specified don't exist (fields: {fields})")
            
            unique_fields = set(non_unique_fields)
            
            if len(unique_fields) != len(fields):
                raise APIBadRequestException(message=f"duplicate fields specified (fields: {fields})")
            
            return [
                field
                for field in self.exposed_fields
                if field.id in unique_fields
            ]


class BaseLandtableDatabase(pydantic.BaseModel):
    """
    A database.

    Note that all database classes are BaseLandtableDatabase. An instance of
    a more specific database class will never exist.
    """

    model_config = pydantic.ConfigDict(extra="allow")

    state: Annotated[LandtableState, pydantic.SkipValidation]
    version: Literal[1] = 1
    id: DatabaseIdentifier
    name: str
    type: str


class LandtablePostgresV0Database(BaseLandtableDatabase):
    type: str = "postgres_v0"
    connection_url: str


class LandtableAirtableV0Database(BaseLandtableDatabase):
    type: str = "airtable_v0"
    api_url: str = "https://api.airtable.com/v0/"
    base_id: str
    table_id: str


LandtableDatabase: TypeAlias = Union[
    LandtablePostgresV0Database, LandtableAirtableV0Database
]


class LandtableWorkspace(pydantic.BaseModel):
    state: Annotated[LandtableState, pydantic.SkipValidation]
    version: Literal[1] = 1

    primary_replica: DatabaseIdentifier
    """
    The primary replica for this workspace.
    """

    name: str
    """
    What Landtable will call this workspace.
    """

    id: WorkspaceIdentifier
    """
    An immutable ID for this workspace.
    """

    async def fetch_table(self, table: str) -> LandtableTable:
        """
        Fetch a table from this workspace. Do not cache the result of this call.
        """

        return await self.state.fetch_table(self.id, table)
