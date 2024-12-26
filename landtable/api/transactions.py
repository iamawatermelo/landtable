"""
The Landtable transaction API. This is the primary way of interacting with
Landtable.
"""

from fastapi import APIRouter

from landtable.backends.abstract import LandtableTransaction
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace

transaction_router = APIRouter()


@transaction_router.post("/execute")
async def execute_transaction(
    transaction: LandtableTransaction,
    table: LandtableTable,
    workspace: LandtableWorkspace,
):
    workspace.primary_replica
