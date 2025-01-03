"""
The Landtable transaction API. This is the primary way of interacting with
Landtable.
"""

from fastapi import APIRouter

from landtable.api.common import State
from landtable.api.common import Table
from landtable.api.common import Workspace
from landtable.backends.abstract import LandtableTransaction
from landtable.backends.abstract import TransactionConsistency

transaction_router = APIRouter()


@transaction_router.post("/execute")
async def execute_transaction(
    state: State,
    transaction: LandtableTransaction,
    table: Table,
    workspace: Workspace,
    consistency: TransactionConsistency,
):
    database, backend = await state.fetch_database(workspace.primary_replica)

    return await backend.exec_transaction(
        transaction, table, database, consistency=consistency
    )
