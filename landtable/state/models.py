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
from typing import Dict
from typing import List
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import Union

import pydantic

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


class LandtableMeta(pydantic.BaseModel):
    """
    Configuration for Landtable.
    """

    state: Annotated[LandtableState, pydantic.SkipValidation]
    version: Literal[1]

    auth_modules: List[str]
    """
    Which authentication modules to use.
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


class LandtableField(pydantic.BaseModel, frozen=True):
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

        return LandtableFieldReplicaConfig(column_name=self.name)


class LandtableTableReplicaConfig(pydantic.BaseModel):
    """
    Replica configuration for a Landtable table.
    """

    model_config = pydantic.ConfigDict(extra="allow")

    table_name: str
    """
    The name of the underlying database table.
    """

    id_column: str | None
    """
    The ID column for this database table.
    """

    created_at_column: str | None
    """
    The created at column for this database table.
    """


class LandtableTable(pydantic.BaseModel):
    """
    Configuration for a Landtable table.
    """

    version: Literal[1] = 1

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

    replica_config: Dict[DatabaseIdentifier, LandtableTableReplicaConfig]

    def fetch_replica_config(self, replica: Identifier):
        """
        Fetch the LandtableTableReplicaConfig for this replica.
        """

        if (config := self.replica_config.get(replica)) is not None:
            return config

        return LandtableTableReplicaConfig(
            table_name=self.name, id_column=None, created_at_column=None
        )

    def resolve_columns(self, fields: Collection[str] | None):
        if fields is None:
            return self.exposed_fields
        else:
            return [
                field
                for field in self.exposed_fields
                if field.name in fields or field.id in fields
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
