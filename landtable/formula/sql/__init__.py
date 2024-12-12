"""
Turn a Landtable formula into an SQL statement suitable for use with SELECT.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from typing import Any
from typing import List
from typing import Tuple

from landtable.formula.exceptions import FormulaTypeException
from landtable.formula.formula import Formula
from landtable.formula.lexer import TokenType
from landtable.formula.parse import ASTConcreteType
from landtable.formula.parse import ASTNode
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.parse import BinOp
from landtable.formula.parse import Cast
from landtable.formula.parse import FunctionCall
from landtable.formula.parse import Number
from landtable.formula.parse import String
from landtable.formula.parse import UnOp
from landtable.formula.parse import Variable
from landtable.formula.sql.functions import SQL_FUNCTION_IMPLS


def to_sql(formula: Formula, env: ASTTypeEnvironment) -> Tuple[str, List[Any]]:
    """
    Parse a formula into an SQL statement suitable for use with SELECT.

    Returns a tuple of (statement, values).
    """

    typ = formula.ast.resolve_type(env)
    if type(typ) is not ASTConcreteType:
        raise FormulaTypeException(
            f"only formulae returning concrete types, like number or string, are supported (got {typ})"
        )

    values: List[Any] = list()

    def recurse(node: ASTNode):
        if type(node) is Cast:
            match node.type:
                case ASTConcreteType.STRING:
                    return f"cast({recurse(node.inner)} as text)"
                case ASTConcreteType.NUMBER:
                    return f"cast({recurse(node.inner)} as double precision)"
                case ASTConcreteType.BOOLEAN:
                    return f"cast({recurse(node.inner)} as boolean)"
                case ASTConcreteType.DATETIME:
                    return f"cast({recurse(node.inner)} as timestamp)"
                case _:
                    raise FormulaTypeException(f"unsupported cast to {node.type}")
        elif type(node) is BinOp:
            token_map = {
                TokenType.MUL: "*",
                TokenType.DIV: "/",
                TokenType.PLUS: "+",
                TokenType.MINUS: "-",
                TokenType.EQ: "=",
                TokenType.LT: "<",
                TokenType.GT: ">",
                TokenType.LE: ">=",
                TokenType.GE: "<=",
            }

            if op := token_map.get(node.op):
                return f"({recurse(node.left)} {op} {recurse(node.right)})"

            raise FormulaTypeException(f"unsupported binop {node.op}")
        elif type(node) is UnOp:
            match node.op:
                case TokenType.MINUS:
                    return f"(-{recurse(node.right)})"
                case _:
                    raise FormulaTypeException(f"unsupported unop {node.op}")
        elif type(node) is Number:
            values.append(node.value)
            return f"${len(values)}"
        elif type(node) is String:
            values.append(node.value)
            return f"${len(values)}"
        elif type(node) is Variable:
            # !! Danger!
            # While it is true that letting people put arbitrary names in SQL is
            # a bad idea, the name should be in one of a predetermined list of
            # names. So it's probably okay.
            return node.name
        elif type(node) is FunctionCall:
            fn_impl = SQL_FUNCTION_IMPLS.get(node.name)

            if fn_impl is None:
                raise FormulaTypeException(
                    f"internal error: no function implementation associated with {node.name}"
                )

            return fn_impl(env, recurse, *node.args)
        else:
            raise FormulaTypeException(f"unsupported node type {node}")

    expr = recurse(formula.ast)

    match typ:
        case ASTConcreteType.NUMBER:
            return f"{expr} <> 0", values
        case ASTConcreteType.STRING:
            return f'{expr} <> ""', values
        case ASTConcreteType.BOOLEAN:
            return expr, values
        case _:
            raise FormulaTypeException(f"don't know how to handle return type {typ}")
