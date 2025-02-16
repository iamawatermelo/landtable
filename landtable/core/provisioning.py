"""
Landtable provisioning plugins. A workspace becomes 'owned' by a
provisioning plugin when it is used to create a workspace, or when
it is imported from an existing workspace.
"""

from importlib.metadata import entry_points
import logging
from typing import Any, Awaitable, Callable, Coroutine, Literal, Protocol

from pydantic import BaseModel

from landtable.identifiers import DatabaseIdentifier, WorkspaceIdentifier

logger = logging.getLogger(__name__)


def find_all_backends() -> dict[str, type[ProvisioningPlugin]]:
    discovered_plugins = entry_points(group="landtable.backends")
    logger.info(f"Discovered {len(discovered_plugins)} database plugins")
    
    return {
        plugin.name: plugin.load() for plugin in discovered_plugins
    }


class ModifyDatabase(BaseModel):
    """
    Create or modify an existing database.
    """
    
    id: DatabaseIdentifier | None
    """
    The existing database, if it exists.
    """
    
    ref: str
    """
    What this database will be referred to in future operations, since
    IDs can't be used on databases that don't exist yet.
    """
    
    args: dict[str, Any]
    """
    Database connection arguments.
    """


class ModifyWorkspace(BaseModel):
    """
    Create or modify a new workspace.
    """
    
    id: WorkspaceIdentifier | None
    """
    The existing workspace, if it exists.
    """
    
    ref: str
    """
    What this workspace will be referred as in future operations, since
    IDs can't be used on workspaces that don't exist yet.
    """
    
    args: dict[str, Any]
    """
    Workspace creation settings that the user has specified.
    """
    
    database: DatabaseIdentifier | str
    """
    An existing database to use, or a reference to a database created
    in this transaction.
    """


class ModifyTable(BaseModel):
    """
    Create a new table.
    """
    
    ref: str
    """
    What this table will be referred as in future operations, since IDs
    can't be used on tables that don't exist yet.
    """
    
    on: WorkspaceIdentifier | str
    """
    Create a table on:
        - an existing workspace
        - a reference to a workspace created inside this transaction
    """
    
    args: dict[str, Any]
    """
    Table creation settings that the user has specified.
    """
    
    fields: dict[ProvisioningField]


class ProvisioningOutput(BaseModel):
    """
    Something to be output.
    """
    
    severity: Literal["INFO"] | Literal["WARNING"] | Literal["ERROR"]
    """
    Whether this is an informational message, a warning message or
    an error message.
    """
    
    message: str
    """
    The actual message. May contain newlines.
    """


ProvisioningOperation = Annotated[
    Union[
        ModifyDatabase,
        ModifyWorkspace,
        ModifyTable
    ]
]


class Action(BaseModel):
    """
    An action.
    """
    
    message: str


class ChangeSummary(BaseModel):
    """
    A change summary.
    """
    
    summary: list[Action]
    etag: str


class ProvisioningPlugin(Protocol):
    """
    A provisioning plugin tells Landtable how to create, update and delete
    tables and fields in a workspace.
    """
    
    def dryrun_execute_changeset(
        self,
        changeset: list[ProvisioningOperation],
        emit: Callable[[ProvisioningOutput], Awaitable]
    ) -> str:
        """
        Show what would happen if a changeset were to be executed.
        `emit` is an awaitable because your log messages may be being
        streamed live to a client.
        """
        
        