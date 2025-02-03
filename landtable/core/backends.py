"""
Landtable database backends.
"""
from __future__ import annotations
from importlib.metadata import entry_points
import logging
from typing import ClassVar, Protocol

from landtable.core.models.transactions import RowResult, TransactionModel
from landtable.core.models.workspaces import TableModel, WorkspaceModel

logger = logging.getLogger(__name__)


def find_all_backends() -> dict[str, type[DatabaseBackend]]:
    discovered_plugins = entry_points(group="landtable.backends")
    logger.info(f"Discovered {len(discovered_plugins)} plugins")
    
    return {
        plugin.name: plugin.load() for plugin in discovered_plugins
    }


class DatabaseBackend(Protocol):
    """
    A database backend tells Landtable how to connect to a database.
    """
    metadata: ClassVar[dict] = {}
    
    def __init__(self, configuration: dict):
        """
        Validate the database's configuration here.
        """
        
        raise NotImplementedError
    
    async def connect(self):
        """
        Do setup tasks. Called before any transactions are executed.
        """
        ...
    
    async def close(self):
        """
        Do shutdown tasks. Called before the backend is deleted.
        """
        ...
    
    async def execute_txn(
        self,
        workspace: WorkspaceModel,
        table: TableModel,
        transaction: TransactionModel
    ) -> list[RowResult | list[RowResult]]:
        """
        Execute a transaction. You do not need to validate caller
        permissions in this function.
        """
        
        raise NotImplementedError
