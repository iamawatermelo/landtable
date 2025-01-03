"""
Base classes and models for Landtable backends.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from collections.abc import Collection
from enum import Enum
from typing import Any, Literal, LiteralString
from typing import ClassVar
from typing import Dict
from typing import TypeAlias
from typing import Union

from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from landtable.exceptions import BaseAPIException
from landtable.exceptions import LandtableExceptionCode
from landtable.formula.formula import Formula
from landtable.identifiers import FieldIdentifier
from landtable.identifiers import RowIdentifier
from landtable.state.models import BaseLandtableDatabase, LandtableField
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace

@dataclass
class LandtableTransactionException(BaseAPIException):
    code: int = Field(default=400)
    type: LandtableExceptionCode = Field(default="BAD_REQUEST")

class TransactionConsistencyEmulation(Enum):
    STRONG = "STRONG"
    """
    This backend natively supports transaction consistency.
    """

    EMULATED = "EMULATED"
    """
    This backend emulates transaction consistency.
    """

class TransactionConsistency(Enum):
    STRICT = "STRICT"
    """
    Ensures that all STRICT transactions will behave as if they were executed
    one after another, in some order. In SQL, this is equivalent to a
    SERIALIZABLE isolation level.
    """

    RELAXED = "RELAXED"
    """
    All operations in the transaction will see the same data. However,
    UpdateByFormula operations may write stale data. In SQL, this is
    equivalent to a REPEATABLE READ isolation level.
    """

    NONE = "NONE"
    """
    Operations in the transaction may see data in the process of being written.
    UpdateByFormula operations may write stale data. In SQL, this is equivalent
    to a READ UNCOMMITTED isolation level. Default for the Airtable API
    compatibility layer.
    """

class BaseResult:
    """
    Something that can be returned by a transaction.
    This class only exists for typing reasons.
    """

class BaseTransactionOperation[Result: BaseResult, T: LiteralString](BaseModel):
    """
    A transaction.
    """

    type: T

class FormulaTarget(BaseModel):
    formula: Formula

class RowTarget(BaseModel):
    id: FieldIdentifier

Target: TypeAlias = Union[FormulaTarget, RowTarget]

class Row(BaseModel, BaseResult):
    id: RowIdentifier
    created_at: str
    contents: Dict[str, Any]

class RowResult(BaseModel, BaseResult):
    """
    The result of a fetch/delete operation.
    """

    rows: list[Row]

class Fetch[Result: RowResult](BaseTransactionOperation):
    """
    Fetch columns.
    Returns a FetchResult.
    """

    type: Literal["fetch"] = "fetch"
    target: Target
    limit: int
    fields: set[str] | None = None
    execTarget: int | None = None
    orderBy: Formula
    failType: (
        Union[
            Literal["eq"],
            Literal["neq"],
            Literal["gt"],
            Literal["ge"],
            Literal["lt"],
            Literal["le"],
        ]
        | None
    ) = None

class Delete[Result: RowResult](BaseTransactionOperation):
    type: Literal["delete"] = "delete"
    target: Target
    limit: int
    fields: set[str] | None = None
    execTarget: int | None = None
    orderBy: Formula
    failType: (
        Union[
            Literal["eq"],
            Literal["neq"],
            Literal["gt"],
            Literal["ge"],
            Literal["lt"],
            Literal["le"],
        ]
        | None
    ) = None

class Create[Result: Row](BaseTransactionOperation):
    type: Literal["create"] = "create"
    row: Dict[str, Any]

class UpdateByFormula[Result: RowResult](BaseTransactionOperation):
    type: Literal["updateByFormula"] = "updateByFormula"
    target: Target
    exec_formula: Dict[str, Formula]

class Update[Result: Row](BaseTransactionOperation):
    type: Literal["update"]
    target: Target
    row: Dict[str, Any]

TransactionOperation: TypeAlias = Union[Fetch, Delete, Create, UpdateByFormula, Update]

class LandtableTransaction(BaseModel):
    ops: list[TransactionOperation]
    use_id: bool = True

    @property
    def read_only(self) -> bool: ...

class BackendInformation(BaseModel):
    transaction_type: TransactionConsistencyEmulation
    config_types: set[str]

class ChangesetRow(BaseModel):
    id: RowIdentifier
    updated_at: str
    contents: Dict[str, Any]

class DatabaseBackend:
    BACKEND_INFORMATION: ClassVar[BackendInformation]

    async def setup(self):
        """
        Initialise this database backend.
        """

    async def shutdown(self):
        """
        Perform shutdown tasks for this database backend.
        """

    async def exec_transaction(
        self,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: BaseLandtableDatabase,
        *,
        consistency: TransactionConsistency = TransactionConsistency.STRICT,
    ) -> Collection[BaseResult]:
        """
        Execute a transaction, and return all of the rows that have been
        changed. If an exception is raised, it is assumed that changes have
        not been applied.
        """

    async def exec_one[T: BaseResult](
        self,
        op: BaseTransactionOperation[T, LiteralString],
        table: LandtableTable,
        config: LandtableDatabase,
        *,
        consistency: TransactionConsistency = TransactionConsistency.STRICT,
        use_id: bool = False,
    ) -> T:
        """
        Execute one operation.
        """

    async def batch_update_row(
        self,
        table: LandtableTable,
        workspace: LandtableWorkspace,
        config: LandtableDatabase,
        changeset: Collection[ChangesetRow],
    ):
        """
        Overwrite or create some rows.
        """
