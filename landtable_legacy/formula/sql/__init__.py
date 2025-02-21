"""
Turn a Landtable formula into an SQL statement suitable for use with SELECT.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from typing import Any

from landtable_legacy.formula.exceptions import FormulaTypeException
from landtable_legacy.formula.formula import Formula
from landtable_legacy.formula.lexer import TokenType
from landtable_legacy.formula.parse import ASTConcreteType, ASTType
from landtable_legacy.formula.parse import ASTNode
from landtable_legacy.formula.parse import ASTTypeEnvironment
from landtable_legacy.formula.parse import BinOp
from landtable_legacy.formula.parse import Cast
from landtable_legacy.formula.parse import FunctionCall
from landtable_legacy.formula.parse import Number
from landtable_legacy.formula.parse import String
from landtable_legacy.formula.parse import UnOp
from landtable_legacy.formula.parse import Variable
from landtable_legacy.formula.sql.functions import SQL_FUNCTION_IMPLS


def _build_recurse(env: ASTTypeEnvironment, values: list[Any]):
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
                    raise FormulaTypeException(
                        message=f"unsupported cast to {node.type}"
                    )
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

            raise FormulaTypeException(message=f"unsupported binop {node.op}")
        elif type(node) is UnOp:
            match node.op:
                case TokenType.MINUS:
                    return f"(-{recurse(node.right)})"
                case _:
                    raise FormulaTypeException(message=f"unsupported unop {node.op}")
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
                    message=f"internal error: no function implementation associated with {node.name}"
                )

            return fn_impl(env, recurse, *node.args)
        else:
            raise FormulaTypeException(message=f"unsupported node type {node}")

    return recurse


def to_sql_expr(
    formula: Formula, env: ASTTypeEnvironment, values: list[Any]
) -> tuple[str, ASTType]:
    typ = formula.ast.resolve_type(env)

    return _build_recurse(env, values)(formula.ast), typ


def to_sql_predicate(
    formula: Formula, env: ASTTypeEnvironment, values: list[Any]
) -> str:
    """
    Parse a formula into an SQL statement suitable for use with SELECT.

    Returns a tuple of (statement, values).
    """

    expr, typ = to_sql_expr(formula, env, values)

    match typ:
        case ASTConcreteType.NUMBER:
            return f"{expr} <> 0"
        case ASTConcreteType.STRING:
            return f'{expr} <> ""'
        case ASTConcreteType.BOOLEAN:
            return expr
        case _:
            raise FormulaTypeException(
                message=f"don't know how to handle return type {typ}"
            )
