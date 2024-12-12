from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeAlias

from landtable.formula.exceptions import FormulaTypeException
from landtable.formula.parse import ASTConcreteType
from landtable.formula.parse import ASTNode
from landtable.formula.parse import ASTType
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.parse import Cast
from landtable.formula.parse import FunctionCall
from landtable.formula.parse import String


FunctionValidator: TypeAlias = Callable[[FunctionCall, List[ASTType]], ASTType]
FunctionImplementation: TypeAlias = Callable[
    [ASTTypeEnvironment, Callable[[ASTNode], str], *Tuple[ASTNode, ...]], str
]


SQL_FUNCTIONS: Dict[str, FunctionValidator] = dict()
SQL_FUNCTION_IMPLS: Dict[str, FunctionImplementation] = dict()


def type_validator(name: str):
    def inner(fn: FunctionValidator):
        SQL_FUNCTIONS[name] = fn

    return inner


def implementation(name: str):
    def inner(fn: FunctionImplementation):
        SQL_FUNCTION_IMPLS[name] = fn

    return inner


def cast(self: FunctionCall, args: List[ASTType], expected_types: List[ASTType]):
    if len(args) != len(expected_types):
        raise FormulaTypeException(f"{self.name} expected 0 arguments, got {len(args)}")

    new_nodes = []

    for i, (arg, expected, node) in enumerate(zip(args, expected_types, self.args)):
        if arg.is_subset(expected):
            new_nodes.append(node)
        else:
            new_nodes.append(Cast(node, expected))


@type_validator("CREATED_TIME")
def created_time_validator(self: FunctionCall, args: List[ASTType]):
    """
    Returns the row's creation timestamp.
    """

    cast(self, args, [])

    return ASTConcreteType.DATETIME


@implementation("CREATED_TIME")
def created_time_implementation(
    env: ASTTypeEnvironment, recurse: Callable[[ASTNode], str], *_
):
    return f"({env.created_time_field})"


@type_validator("DATETIME_DIFF")
def datetime_diff_validator(self: FunctionCall, args: List[ASTType]):
    """
    Compute the difference in dates according to the third parameter
    """

    cast(
        self,
        args,
        [ASTConcreteType.DATETIME, ASTConcreteType.DATETIME, ASTConcreteType.STRING],
    )

    return ASTConcreteType.NUMBER


@implementation("DATETIME_DIFF")
def datetime_diff_implementation(
    env: ASTTypeEnvironment,
    recurse: Callable[[ASTNode], str],
    first: ASTNode,
    second: ASTNode,
    unit: ASTNode,
    *_,
):
    if type(unit) is not String:
        raise FormulaTypeException(
            "DATETIME_DIFF only supports literals as a third argument"
        )

    if unit.value not in (
        "years",
        "months",
        "days",
        "hours",
        "minutes",
        "seconds",
        "milliseconds",
        "quarters",
        "ms",
        "s",
        "m",
        "h",
        "w",
        "M",
        "Q",
        "y",
    ):
        raise FormulaTypeException(f"invalid unit {unit.value}")

    return f"EXTRACT({unit.value} FROM AGE({recurse(first)}, {recurse(second)}))"


@type_validator("NOW")
def now_validator(self: FunctionCall, args: List[ASTType]):
    """
    Get the timestamp of this moment in time.
    """

    cast(self, args, [])

    return ASTConcreteType.DATETIME


@implementation("NOW")
def now_implementation(env: ASTTypeEnvironment, recurse: Callable[[ASTNode], str], *_):
    return "now()"
