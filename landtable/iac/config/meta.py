"""
Configuration objects for Landtable.
"""
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from landtable.identifiers import TableIdentifier
from landtable.state.models import FieldType
from landtable.formula.formula import Formula


class DatabaseSpec(BaseModel):
    """
    A database. Extra fields will be 
    """
    
    model_config = ConfigDict()
    
    using: str
    replicate: bool


class MetaDocument(BaseModel):
    """
    A meta configuration file.
    """
    
    type: Literal["meta"]
    version: Literal[1]
    
    auth: dict[str, dict] = {}
    allow_runtime_provisioning: bool = False
    
    database: dict[str, DatabaseSpec]