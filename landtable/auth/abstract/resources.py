"""
Types of resources.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.

from dataclasses import dataclass

from landtable.auth.abstract import Resource
from landtable.identifiers import DatabaseIdentifier, TableIdentifier, WorkspaceIdentifier


@dataclass(frozen=True)
class TableRowsResource(Resource):
    """
    Whether the caller can access the rows of a table.
    """

    id = "lt.table_rows"

    table: TableIdentifier
    
    @property
    def resource_name(self) -> str:
        return f"rows of {self.table}"


@dataclass(frozen=True)
class TableAliasesResource(Resource):
    """
    Whether the caller can access the aliases of a table.
    """

    id = "lt.tables.aliases"

    table: TableIdentifier


@dataclass(frozen=True)
class TableConfigurationResource(Resource):
    """
    Whether the caller can access the configuration of a table.
    """

    id = "lt.tables.configuration"

    table: TableIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.table}"


@dataclass(frozen=True)
class WorkspaceResource(Resource):
    """
    Whether the caller can access this workspace at all.
    
    Supports READ only.
    """

    id = "lt.workspace"

    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"{self.workspace}"


@dataclass(frozen=True)
class WorkspaceConfigurationResource(Resource):
    """
    Whether the caller can access non-sensitive configuration of a workspace,
    like its name.
    
    Supports READ/UPDATE/DELETE.
    """

    id = "lt.workspace_config"

    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.workspace}"


@dataclass(frozen=True)
class WorkspaceAliasesResource(Resource):
    """
    Whether the caller can find a workspace by alias or change aliases.
    
    Supports READ/WRITE/UPDATE/DELETE.
    """

    id = "lt.workspace_aliases"

    @property
    def resource_name(self) -> str:
        return "aliases"


@dataclass(frozen=True)
class DatabaseConfigResource(Resource):
    """
    Whether the caller can access a database's configuration.
    
    Supports READ/UPDATE/DELETE.
    """
    
    id = "lt.databases"
    
    database: DatabaseIdentifier
    
    @property
    def resource_name(self) -> str:
        return "databases"


@dataclass(frozen=True)
class LandtableAPIResource(Resource):
    """
    Whether the caller can use the Extended Landtable API.
    
    Supports EXECUTE only.
    """
    
    id = "lt.ext_api"
    
    @property
    def resource_name(self) -> str:
        return "landtable API"


@dataclass(frozen=True)
class CompatibilityAPIResource(Resource):
    """
    Whether the caller can use the Airtable compatibility layer.
    
    Supports EXECUTE only.
    """
    
    id = "lt.legacy_api"
    
    @property
    def resource_name(self) -> str:
        return "compatibility API"
