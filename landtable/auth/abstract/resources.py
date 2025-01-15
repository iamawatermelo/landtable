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

    id = "table_rows"

    table: TableIdentifier
    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"rows of {self.workspace}/{self.table}"


@dataclass
class TableAliasesResource(Resource):
    """
    Whether the caller can access the aliases of a table.
    """

    id = "table_aliases"

    table: TableIdentifier
    workspace: WorkspaceIdentifier


@dataclass
class TableConfigurationResource(Resource):
    """
    Whether the caller can access the configuration of a table.
    """

    id = "table_config"

    table: TableIdentifier
    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.workspace}/{self.table}"


@dataclass
class WorkspaceResource(Resource):
    """
    Whether the caller can access this workspace at all.
    """

    id = "workspace"

    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"{self.workspace}"


@dataclass
class WorkspaceConfigurationResource(Resource):
    """
    Whether the caller can access non-sensitive configuration of a workspace,
    like its name.
    """

    id = "workspace_config"

    workspace: WorkspaceIdentifier

    @property
    def resource_name(self) -> str:
        return f"configuration of {self.workspace}"


class WorkspaceAliasesResource(Resource):
    """
    Whether the caller can find a workspace by alias or write aliases.
    """

    id = "workspace_aliases"

    @property
    def resource_name(self) -> str:
        return "aliases"
