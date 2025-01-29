"""
Database models
"""

from typing import Literal
from pydantic import BaseModel

from landtable.identifiers import DatabaseIdentifier

class DatabaseModel(BaseModel):
    """
    A database in Landtable is something that can be written to or
    read from.
    """
    version: Literal[1]
    revision: int
    
    id: DatabaseIdentifier
    """
    A unique identifier for this database.
    """
    
    name: str
    """
    A human-readable name for this database.
    """
    
    description: str
    """
    A human-readable description for this database.
    """
    
    comment: str
    """
    A human-readable comment about this database left by automated
    tooling.
    """
    
    plugin: str
    """
    Which database plugin to use.
    """
    
    config: dict
    """
    Configuration for this database plugin.
    """
    
    management: dict | None
    """
    Management plugin for this database.
    """