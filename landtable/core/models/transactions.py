"""
Transaction models
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, ClassVar, Literal, TypeAlias, Union
from pydantic import BaseModel, Field, PrivateAttr

from landtable.auth.abstract import AccessType
from landtable.core.models.workspaces import FieldModel
from landtable.formula.formula import Formula
from landtable.identifiers import FieldIdentifier, RowIdentifier


class Consistency(Enum):
    SERIALIZABLE = "serializable"
    """
    Serializable transactions ensure that transactions execute as if
    they were executed all at once, in some order, in some point in time.
    """
    
    RELAXED = "relaxed"
    """
    Relaxed transactions may overlap with other transactions, but they
    will only ever operate on committed data.
    """
    
    NONE = "none"
    """
    Transactions may read dirty / stale / uncommitted data.
    """


class FormulaWithEnvironment(BaseModel):
    """
    A formula that contains an environment.
    """
    formula: Formula
    env: dict[str, str | int | float]


class RowTarget(BaseModel):
    """
    Target a specific row.
    """
    type: Literal["row"]
    row: RowIdentifier


class FormulaTarget(BaseModel):
    type: Literal["formula"]
    row: RowIdentifier


Target: TypeAlias = Annotated[
    RowTarget | FormulaTarget,
    Field(discriminator="type")
]


class FailCondition(BaseModel):
    """
    Fail when the amount of returned rows is above, below or equal to
    some number.
    """
    target: int
    op: Union[
        Literal["eq"],
        Literal["neq"],
        Literal["gt"],
        Literal["geq"],
        Literal["lt"],
        Literal["leq"]
    ]
    
    def evaluate(self, rows: int):
        """
        Evaluate this fail condition, returning True if it has succeeded
        and False if it has failed.
        """
        match self.op:
            case "eq":
                return self.target == rows
            case "neq":
                return self.target != rows
            case "gt": 
                return self.target > rows
            case "geq":
                return self.target >= rows
            case "lt":
                return self.target < rows
            case "leq":
                return self.target <= rows


Row: TypeAlias = dict[FieldIdentifier | str, Any]


class RowResult(BaseModel):
    id: RowIdentifier
    created_at: datetime
    row: Row


class BaseOperation(BaseModel):
    access_types: ClassVar[set[AccessType]]


class ReadOperation(BaseOperation):
    """
    Read some files. Returns a list of RowResult.
    """
    type: Literal["read"]
    access_types: ClassVar = {AccessType.READ}
    
    target: Target | None
    """
    The rows to be read. If None, return all rows.
    """
    
    sort: Formula | FieldIdentifier | None = None
    """
    Which order to sort the rows in before reading them.
    """
    
    limit: int = 1
    """
    How many rows to return.
    """
    
    fail: FailCondition | None = None
    """
    When this operation should fail.
    """
    
    fields: set[FieldIdentifier | str] | None = None
    """
    The fields to return. If None, return all fields.
    """
    
    _resolved_returned_fields: set[FieldModel] = PrivateAttr()
    """
    Cannot be passed through deserialization of a transaction.
    Resolved set of returned fields.
    """


class WriteOperation(BaseOperation):
    """
    Write a row to the table. Returns the ID of the written row.
    """
    type: Literal["write"]
    access_types: ClassVar = {AccessType.WRITE}
    
    row: Row
    """
    The row to be written.
    """
    
    _resolved_row: dict[FieldModel, Any] = PrivateAttr()
    """
    Cannot be passed through deserialization of a transaction.
    Resolved set of rows.
    """


class UpdateByFormulaOperation(BaseOperation):
    """
    Update some rows according to a formula. Returns a list of RowResult
    with the rows updated.
    """
    type: Literal["update_by_formula"]
    access_types: ClassVar = {AccessType.MODIFY, AccessType.READ}
    
    target: Target
    """
    The rows to modify.
    """
    
    update: dict[RowIdentifier, Formula | FormulaWithEnvironment]
    """
    How to update each row.
    """
    
    reset_unspecified: bool = False
    """
    Whether to reset unspecified fields to their default value.
    """
    
    fail: FailCondition | None = None
    """
    When this operation should fail.
    """


class UpdateOperation(BaseOperation):
    """
    Update some rows. Returns a list of RowResult with the rows updated.
    """
    type: Literal["update"]
    access_types: ClassVar = {AccessType.MODIFY, AccessType.READ}
    
    target: Target
    """
    The rows to modify.
    """
    
    update: dict[RowIdentifier, str | int | float]
    """
    How to update each row.
    """
    
    reset_unspecified: bool = False
    """
    Whether to reset unspecified fields to their default value.
    """
    
    fail: FailCondition | None = None
    """
    When this operation should fail.
    """


class DeleteOperation(BaseOperation):
    """
    Delete some rows. Returns a list of RowResult with the rows deleted.
    """
    type: Literal["delete"]
    access_types: ClassVar = {AccessType.DELETE, AccessType.READ}
    
    target: Target
    """
    The rows to be deleted.
    """

    fail: FailCondition | None = None
    """
    When this operation should fail.
    """
    
    fields: set[FieldIdentifier | str] | None = None
    """
    The fields to return. If None, return all fields.
    """
    
    _resolved_returned_fields: set[FieldModel] = PrivateAttr()
    """
    Cannot be passed through deserialization of a transaction.
    Resolved set of returned fields.
    """


TransactionOperation = Annotated[
    Union[
        ReadOperation,
        WriteOperation,
        UpdateByFormulaOperation,
        UpdateOperation,
        DeleteOperation
    ],
    Field(discriminator="type")
]


class TransactionModel(BaseModel):
    """
    A transaction is how you interact with Landtable.
    """
    
    ops: list[TransactionOperation]
    consistency: Consistency = Consistency.RELAXED
    use_ids: bool = False
