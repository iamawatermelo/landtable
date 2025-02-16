"""
Configuration objects for Landtable.
"""
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from landtable.identifiers import TableIdentifier


class DatabaseSpecV1(BaseModel):
    """
    A database. Extra fields will be in model_extra.
    """
    
    model_config = ConfigDict()
    
    name: str
    using: str
    replicate: bool


class DatabaseRefSpecV1(BaseModel):
    """
    A reference to an existing database.
    """
    
    model_config = ConfigDict()
    
    ref: str


class BaseStrategySpecV1(BaseModel):
    """
    A provisioning strategy. Strategy providers should get their
    configuration with model_extra.
    """
    model_config = ConfigDict(extra="allow")
    
    extends: str | None
    """
    A strategy template to take this from.
    """
    
    using: str
    """
    The strategy provider to use for provisioning.
    """
    
    replicate: bool = True
    """
    Whether to allow replication by default.
    """


class BaseFieldReplicaConfigV1(BaseModel):
    """
    Replica configuration for a field. Strategy providers should get
    their configuration with model_extra.
    """
    model_config = ConfigDict(extra="allow")
    
    disallow_replication: bool


class FieldSpecV1(BaseModel):
    """
    A field specification.
    """
    
    type: str
    alias: str | list[str] = []
    default: Any | None = None
    
    enum: list[str] | None = None
    
    primary_config: BaseFieldReplicaConfigV1 | None = None
    secondary_config: dict[str, BaseFieldReplicaConfigV1] = {}


# class ViewSpec(BaseModel):
#     """
#     A view specification, defining what a view should look like.
#     """
#     
#     filter: Formula | None


class TableSpecV1(BaseModel):
    """
    A table specification, defining what a table should look like.
    """
    
    alias: str | list[str] = []
    field: dict[str, FieldSpecV1] = {}
    # view: dict[str, ViewSpec]


class WorkspaceDocumentV1(BaseModel):
    """
    A workspace configuration file.
    """
    type: Literal["workspace"]
    version: Literal[1]
    
    name: str
    alias: str | list[str] = []
    id: TableIdentifier | None = None
    
    # TODO implement an actual pydantic validator 
    primary_database: DatabaseSpecV1 | DatabaseRefSpecV1
    secondary_database: list[DatabaseSpecV1 | DatabaseRefSpecV1] | DatabaseSpecV1 | DatabaseRefSpecV1 = []
    
    table: dict[str, TableSpecV1] = {}