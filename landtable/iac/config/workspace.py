"""
Configuration objects for Landtable.
"""
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from landtable.identifiers import TableIdentifier
from landtable.state.models import FieldType
from landtable.formula.formula import Formula


class BaseStrategySpec(BaseModel):
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


class BaseFieldReplicaConfig(BaseModel):
    """
    Replica configuration for a field. Strategy providers should get
    their configuration with model_extra.
    """
    model_config = ConfigDict(extra="allow")
    
    disallow_replication: bool


class FieldSpec(BaseModel):
    """
    A field specification.
    """
    
    type: FieldType
    default: Any | None
    
    enum: list[str] | None
    
    primary_config: BaseFieldReplicaConfig | None
    secondary_config: dict[str, BaseFieldReplicaConfig] = {}


class ViewSpec(BaseModel):
    """
    A view specification, defining what a view should look like.
    """
    
    filter: Formula | None


class TableSpec(BaseModel):
    """
    A table specification, defining what a table should look like.
    """
    
    field: dict[str, FieldSpec]
    view: dict[str, ViewSpec]


class WorkspaceDocument(BaseModel):
    """
    A workspace configuration file.
    """
    type: Literal["workspace"]
    version: Literal[1]
    
    alias: str | list[str]
    id: TableIdentifier | None
    
    primary_strategy: BaseStrategySpec | None
    secondary_strategy: dict[str, BaseStrategySpec] = {}
    
    table: dict[str, TableSpec] = {}