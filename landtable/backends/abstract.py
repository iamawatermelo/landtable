from collections.abc import Collection
from enum import auto
from enum import Enum
from typing import Any
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import TypeAlias
from typing import Union

from pydantic import BaseModel
from pydantic.fields import Field

from landtable.exceptions import BaseAPIException
from landtable.exceptions import LandtableExceptionCode
from landtable.formula.formula import Formula
from landtable.identifiers import FieldIdentifier
from landtable.identifiers import RowIdentifier
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace


class LandtableTransactionException(BaseAPIException):
    code: int = Field(default=400)
    type: LandtableExceptionCode = Field(default="BAD_REQUEST")


class BackendTransactionGuarantee(Enum):
    STRONG = auto()
    """
    This backend supports atomic, isolated and durable transactions.
    """

    EMULATED = auto()
    """
    This backend emulates atomic transactions.
    """


class EmulatedTransactionType(Enum):
    SEQCST = auto()
    """
    Sequentially consistent, atomic, isolated, batched writes. Atomic
    transactions are emulated when there are no other conflicting NONE writes.
    """

    RELAXED = auto()
    """
    Do not attempt to emulate atomicity.
    """

    NONE = auto()
    """
    Do not provide any guarantees at all.
    """


class BaseResult:
    """
    Something that can be returned by a transaction.
    This class only exists for typing reasons.
    """


class BaseTransactionOperation[Result: BaseResult]:
    """
    A transaction.
    """

    type: str


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


class Fetch[Result: RowResult](BaseModel, BaseTransactionOperation):
    """
    Fetch columns.
    Returns a FetchResult.
    """

    type = "fetch"
    target: Target
    limit: int
    columns: set[str] | None

    def resolve_columns(self, table: LandtableTable):
        if self.columns is None:
            return table.exposed_fields
        else:
            return [
                field
                for field in table.exposed_fields
                if field.lt_name in self.columns or field.lt_id in self.columns
            ]


class Delete[Result: RowResult](BaseModel, BaseTransactionOperation):
    type = "delete"
    target: Target


class Create[Result: Row](BaseModel, BaseTransactionOperation):
    type = "create"
    row: Dict[str, Any]


class UpdateByFormula[Result: RowResult](BaseModel, BaseTransactionOperation):
    type = "updateByFormula"
    target: Target
    exec_formula: Dict[str, Formula]


class Update[Result: Row](BaseModel, BaseTransactionOperation):
    type = "update"
    target: Target
    row: Dict[str, Any]


TransactionOperation: TypeAlias = Union[Fetch, Delete, Create, UpdateByFormula, Update]


class LandtableTransaction(BaseModel):
    ops: list[BaseTransactionOperation[BaseResult]]
    use_id: bool = True

    @property
    def read_only(self):
        for op in self.ops:
            if op.type != "fetch":
                return False

        return True


class BackendInformation(BaseModel):
    transaction_type: BackendTransactionGuarantee
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

        raise NotImplementedError

    async def exec_transaction(
        self,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: LandtableDatabase,
        *,
        consistency: EmulatedTransactionType | None,
    ) -> Collection[BaseResult]:
        """
        Execute a transaction, and return all of the rows that have been
        changed. If an exception is raised, it is assumed that changes have
        not been applied.
        """

        raise NotImplementedError

    async def exec_one[T: BaseResult](
        self,
        op: BaseTransactionOperation[T],
        table: LandtableTable,
        config: LandtableDatabase,
        *,
        consistency: EmulatedTransactionType | None,
        use_id: bool = False,
    ) -> T:
        [result] = await self.exec_transaction(
            LandtableTransaction(ops=[op], use_id=use_id),
            table,
            config,
            consistency=consistency,
        )

        return cast(T, result)

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

        raise NotImplementedError
