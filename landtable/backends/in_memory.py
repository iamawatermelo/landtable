"""
in_memory_v0 is an in-memory database for Landtable. Note that data
is not persisted. For development use only.
"""

from datetime import datetime
from typing import Any, ClassVar
from uuid import uuid4

from pydantic.dataclasses import dataclass
from landtable.core.backends import DatabaseBackend
from landtable.core.models.transactions import ReadOperation, RowResult, RowTarget, Target, TransactionModel, TransactionOperation, WriteOperation
from landtable.core.models.workspaces import FieldModel, TableModel, WorkspaceModel
from landtable.exceptions import APIPredicateFailed
from landtable.identifiers import FieldIdentifier, Identifier, RowIdentifier, TableIdentifier, WorkspaceIdentifier


@dataclass
class Row:
    created_at: datetime
    row: dict[FieldIdentifier, Any]


class InMemoryDatabaseBackend(DatabaseBackend):
    metadata: ClassVar = {}
    table_data: dict[WorkspaceIdentifier, dict[TableIdentifier, dict[RowIdentifier, Row]]]
    
    def __init__(self, config: dict):
        assert len(config) == 0, "in_memory_v0 expects no database configuration"
        
        self.table_data = {}
    
    @staticmethod
    def resolve_targeted_fields(
        target: Target | None,
        filter: set[FieldModel],
        use_ids: bool,
        table: dict[RowIdentifier, Row]
    ):
        filtered_ids = {x._id for x in filter}
        if use_ids:
            id_map = {
                x._id: x._id for x in filter
            }
        else:
            id_map = {
                x._id: x.name for x in filter
            }
        
        if isinstance(target, RowTarget):
            row = table.get(target.row)
            
            if row is None:
                return []
            
            return [RowResult(
                id=target.row,
                row={
                    (id_map[k]): v
                    for k, v in row.row.items()
                    if k in filtered_ids
                },
                created_at=row.created_at
            )]
        elif target is None:
            return [RowResult(
                id=row_id,
                row={
                    (id_map[k]): v
                    for k, v in row.row.items()
                    if k in filtered_ids
                },
                created_at=row.created_at
            ) for row_id, row in table.items()]
        
        raise NotImplementedError()
    
    async def _execute_op(
        self,
        table: dict[RowIdentifier, Row],
        operation: TransactionOperation,
        use_ids: bool
    ) -> RowResult | list[RowResult]:
        if isinstance(operation, ReadOperation):
            result = self.resolve_targeted_fields(
                operation.target,
                operation._resolved_returned_fields,
                use_ids,
                table
            )
            
            if operation.fail is not None and not operation.fail.evaluate(len(result)):
                raise APIPredicateFailed(message="read operation has failed a predicate")
            
            return result
        
        if isinstance(operation, WriteOperation):
            new_id = Identifier("lrw", uuid4())
            table[new_id] = Row(
                created_at=datetime.now(),
                row={k._id: v for k, v in operation._resolved_row.items()}
            )
            
            return RowResult(
                id=new_id,
                row=table[new_id].row,
                created_at=table[new_id].created_at
            )
        
        raise NotImplementedError()
    
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
        
        workspace_tab = self.table_data.get(workspace.id, {})
        self.table_data[workspace.id] = workspace_tab
        table_tab = workspace_tab.get(table._id, {})
        workspace_tab[table._id] = table_tab
        
        ret = [await self._execute_op(table_tab, op, transaction.use_ids) for op in transaction.ops]
        
        return ret