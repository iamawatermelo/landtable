"""
Landtable's internal state, stored in etcd
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from typing import Dict
from typing import List
from typing import Literal
from typing import TypeAlias
from typing import Union

import pydantic

import landtable.state as state
from landtable.formula.parse import ASTConcreteType
from landtable.identifiers import DatabaseIdentifier
from landtable.identifiers import Identifier


class LandtableMeta(pydantic.BaseModel):
    state: state.LandtableState
    version: Literal[1] = 1


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

    state: state.LandtableState
    version: Literal[1]

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

    replica_config: Dict[str, LandtableFieldReplicaConfig]

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

    def fetch_replica_config(self, replica: Identifier):
        """
        Fetch the LandtableFieldReplicaConfig for this replica.
        """

        if config := self.replica_config.get(str(replica)):
            return config

        return LandtableFieldReplicaConfig(column_name=self.lt_name)


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
    The datetime field that represents the created time field.
    """

    id_field: str
    """
    The UUID field that represents the ID.
    """

    replica_config: Dict[str, LandtableTableReplicaConfig]

    def fetch_replica_config(self, replica: Identifier):
        """
        Fetch the LandtableTableReplicaConfig for this replica.
        """

        if config := self.replica_config.get(str(replica)):
            return config

        return LandtableTableReplicaConfig(
            table_name=self.lt_name, id_column=None, created_at_column=None
        )


class BaseLandtableDatabase(pydantic.BaseModel):
    """
    A database.

    Note that all database classes are BaseLandtableDatabase. An instance of
    a more specific database class will never exist.
    """

    model_config = pydantic.ConfigDict(extra="allow")

    state: state.LandtableState
    version: Literal[1] = 1
    id: DatabaseIdentifier
    name: str


class LandtablePostgresV0Database(BaseLandtableDatabase):
    type = "postgres_v0"
    connection_url: str


class LandtableAirtableV0Database(BaseLandtableDatabase):
    type = "airtable_v0"
    api_url: str = "https://api.airtable.com/v0/"
    base_id: str
    table_id: str


LandtableDatabase: TypeAlias = Union[
    LandtablePostgresV0Database, LandtableAirtableV0Database
]


class LandtableWorkspace(pydantic.BaseModel):
    state: state.LandtableState
    version: Literal[1] = 1

    primary_replica: DatabaseIdentifier
    """
    The primary replica for this workspace.
    """

    lt_name: str
    """
    What Landtable will call this workspace.
    """

    lt_id: str
    """
    An immutable ID for this workspace.
    """

    async def fetch_table(self, table: str) -> LandtableTable:
        """
        Fetch a table from this workspace. Do not cache the result of this call.
        """

        return await self.state.fetch_table(self.lt_id, table)
