"""
The Landtable transaction API. This is the primary way of interacting with
Landtable.
"""

from __future__ import annotations

from fastapi import APIRouter

from landtable.api.common import Authentication
from landtable.api.common import State
from landtable.api.common import Table
from landtable.api.common import Workspace
from landtable.auth.abstract import AccessType
from landtable.auth.abstract.resources import LandtableAPIResource
from landtable.backends.abstract import LandtableTransaction
from landtable.backends.abstract import TransactionConsistency

transaction_router = APIRouter(prefix="/api")


@transaction_router.post("/execute")
async def execute_transaction(
    state: State,
    transaction: LandtableTransaction,
    table: Table,
    workspace: Workspace,
    consistency: TransactionConsistency,
    context: Authentication,
):
    # Ensure the caller can use this API
    await context.evaluate({AccessType.EXECUTE}, LandtableAPIResource())
    
    with context.enter():
        return await transaction.execute_and_validate(
            state, table, workspace, consistency
        )