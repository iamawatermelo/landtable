"""
Workspace models
"""

from typing import Annotated, Literal, TypeAlias, Union
from pydantic import BaseModel, Field, PrivateAttr

from landtable.identifiers import DatabaseIdentifier, FieldIdentifier, TableIdentifier, WorkspaceIdentifier


FieldType: TypeAlias = Union[
    Literal["text"],
    Literal["integer"],
    Literal["number"],
    Literal["date"],
    Literal["time"],
    Literal["datetime"]
]


class FieldModel(BaseModel):
    id: FieldIdentifier = PrivateAttr()
    """
    Set when resolving a transaction's fields.
    """
    
    name: str
    """
    The name of this field.
    """
    
    type: FieldType
    """
    The type of this field.
    """
    
    replica_config: dict[DatabaseIdentifier, dict]
    """
    Per-replica configuration for this field.
    """
    
    metadata: dict
    """
    Arbitrary metadata for this field.
    """


class TableModel(BaseModel):
    """
    A table in Landtable contains your data.
    """
    
    id: FieldIdentifier = PrivateAttr()
    """
    Set when resolving a transaction's table.
    """
    
    read_only: bool
    """
    Whether to allow reads from this table.
    """
    
    name: str
    """
    A human-readable name for this table.
    """
    
    description: str
    """
    A human-readable description for this table.
    """
    
    fields: dict[FieldIdentifier, FieldModel]
    """
    Fields in this table (like columns).
    """
    
    replica_config: dict[DatabaseIdentifier, dict]
    """
    Per-table replica configuration.
    """
    
    metadata: dict
    """
    Extra metadata for this table.
    """


class SecondaryImmediateReplication(BaseModel):    
    """
    replicate_immediate replication will block requests from
    completing until replication has succeeded.
    """
    
    action: Literal["replicate_immediate"]


class SecondaryDeferReplication(BaseModel):
    """
    defer replication asynchronously replicates to a database
    without blocking a request from completing.
    """
    
    action: Literal["defer"]
    batch_ms: int


class SecondaryDeferElectReplication(BaseModel):
    """
    defer_elect replication batches some requests locally
    (local_batch_ms) before sending them to an elected leader,
    which also batches some requests (remote_batch_ms) before
    writing them all at once.
    """
    
    action: Literal["defer_elect"]
    local_batch_ms: int
    remote_batch_ms: int


SecondaryDatabaseConfig: TypeAlias = Annotated[
    Union[
        SecondaryImmediateReplication,
        SecondaryDeferReplication,
        SecondaryDeferElectReplication
    ],
    Field(discriminator='action')
]


class WorkspaceModel(BaseModel):
    """
    A workspace in Landtable is a collection of tables and views.
    """
    version: Literal[1]
    
    name: str
    """
    The name of this workspace.
    """
    
    id: WorkspaceIdentifier
    """
    This workspace's unique identifier.
    """
    
    primary: DatabaseIdentifier
    """
    This workspace's primary database. All reads are against this
    database.
    """
    
    secondary: dict[DatabaseIdentifier, SecondaryDatabaseConfig]
    """
    This workspace's secondary database replicas. Writes are copied to
    secondary database replicas.
    """
    
    tables: dict[TableIdentifier, TableModel]
    """
    Tables in this workspace.
    """
    
    replica_config: dict[DatabaseIdentifier, dict]
    """
    Per-workspace replica configuration.
    """
