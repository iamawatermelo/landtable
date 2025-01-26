"""
Types of resources.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.

from dataclasses import dataclass

from landtable.auth.abstract import Resource
from landtable.identifiers import TableIdentifier, WorkspaceIdentifier


@dataclass
class TableRowsResource(Resource):
    """
    Whether the caller can access the rows of a table.
    """

    id = "lt.table_rows"

    table: TableIdentifier
    
    @property
    def resource_name(self) -> str:
        return f"rows of {self.table}"


@dataclass
class TableAliasesResource(Resource):
    """
    Whether the caller can access the aliases of a table.
    """

    id = "lt.tables.aliases"

    table: TableIdentifier


@dataclass
class TableConfigurationResource(Resource):
    """
    Whether the caller can access the configuration of a table.
    """

    id = "lt.tables.configuration"

    table: TableIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.table}"


@dataclass
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


@dataclass
class WorkspaceConfigurationResource(Resource):
    """
    Whether the caller can access non-sensitive configuration of a workspace,
    like its name.
    
    Supports READ/WRITE/UPDATE/DELETE.
    """

    id = "lt.workspace_config"

    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.workspace}"


class WorkspaceAliasesResource(Resource):
    """
    Whether the caller can find a workspace by alias or change aliases.
    
    Supports READ/WRITE/UPDATE/DELETE.
    """

    id = "lt.workspace_aliases"

    @property
    def resource_name(self) -> str:
        return "aliases"


class LandtableAPIResource(Resource):
    """
    Whether the caller can use the Extended Landtable API.
    
    The only operation that makes sense for this resource is EXECUTE.
    """
    
    id = "lt.ext_api"
    
    @property
    def resource_name(self) -> str:
        return "landtable API"


class CompatibilityAPIResource(Resource):
    """
    Whether the caller can use the Airtable compatibility layer.
    
    The only operation that makes sense for this resource is EXECUTE.
    """
    
    id = "lt.legacy_api"
    
    @property
    def resource_name(self) -> str:
        return "compatibility API"
