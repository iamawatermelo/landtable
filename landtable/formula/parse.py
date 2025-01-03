"""
Turn a list of Landtable formula tokens into an AST tree.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import List
from typing import Optional
from typing import Self
from typing import Set

from landtable.formula.exceptions import FormulaTypeException
from landtable.formula.lexer import Token
from landtable.formula.lexer import TokenType


class ASTType:
    def is_subset(self, rhs: Self):
        raise NotImplementedError


class ASTConcreteType(ASTType, Enum):
    """
    A concrete type, like "number".
    """

    NUMBER = 1
    STRING = 2
    DATETIME = 3
    BOOLEAN = 4

    def is_subset(self, rhs: ASTType):
        if type(rhs) is ASTTypeUnion:
            return self in rhs.members

        return self == rhs


@dataclass(unsafe_hash=True, frozen=True)
class ASTListType(ASTType):
    """
    A list type, like "[number | string]".
    """

    inner: ASTType


@dataclass(unsafe_hash=True)
class ASTTypeUnion(ASTType):
    """
    A union type, like "number | string".
    """

    members: Set[ASTConcreteType]

    def __init__(self, *members: ASTType):
        member_set = set()

        for member in members:
            if type(member) is ASTTypeUnion:
                member_set |= member.members
            else:
                member_set.add(member)

        self.members = member_set

    def is_subset(self, rhs: ASTType):
        if type(rhs) is ASTTypeUnion:
            return self.members.issubset(rhs.members)

        # otherwise, it is a concrete type
        # this cannot be a subset of a concrete type unless it is a union of one
        if len(self.members) != 1:
            return False

        return next(iter(self.members)) == rhs


@dataclass
class ASTTypeEnvironment:
    variables: dict[str, ASTType]
    functions: dict[str, Callable[["FunctionCall", List[ASTType]], ASTType]]
    id_field: str
    created_time_field: str


class ASTNode:
    resolved_type: ASTType | None = None

    def output(self, indent: int = 0) -> None:
        raise NotImplementedError

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        raise NotImplementedError

    def recurse[T](self, fn: Callable[["ASTNode"], T]) -> Generator[T, None, None]:
        raise NotImplementedError


@dataclass
class Cast(ASTNode):
    """
    Inserted during type checking.
    Represents a cast from the inner node's type to its own type.
    """

    inner: ASTNode
    type: ASTType

    @property
    def resolved_type(self) -> ASTType | None:
        return self.type

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        return self.type

    def output(self, indent: int = 0):
        print(" " * indent + f"Cast({self.type}")
        self.inner.output(indent + 4)
        print(" " * indent + f") -> {self.resolved_type or 'unknown'}")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        yield fn(self.inner)


@dataclass
class BinOp(ASTNode):
    """
    An operation between two nodes.
    """

    left: ASTNode
    op: TokenType
    right: ASTNode

    def __repr__(self) -> str:
        return f"BinOp({self.left} {self.op} {self.right})"

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        resultant_type: ASTType

        match self.op:
            case x if x in (
                TokenType.MUL,
                TokenType.DIV,
                TokenType.PLUS,
                TokenType.MINUS,
                TokenType.LE,
                TokenType.GE,
                TokenType.NE,
                TokenType.LT,
                TokenType.GT,
            ):
                resultant_type = ASTConcreteType.NUMBER
            case TokenType.AMPERSAND:
                resultant_type = ASTConcreteType.STRING
            case TokenType.EQ:
                resultant_type = self.right.resolve_type(env)
            case _:
                raise

        if not self.left.resolve_type(env).is_subset(resultant_type):
            self.left = Cast(self.left, resultant_type)

        if not self.right.resolve_type(env).is_subset(resultant_type):
            self.right = Cast(self.right, resultant_type)

        if self.op in (
            TokenType.EQ,
            TokenType.LE,
            TokenType.GE,
            TokenType.NE,
            TokenType.LT,
            TokenType.GT,
        ):
            self.resolved_type = ASTConcreteType.BOOLEAN
            return ASTConcreteType.BOOLEAN

        self.resolved_type = resultant_type
        return resultant_type

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f"BinOp({self.op}")
        self.left.output(indent + 4)
        self.right.output(indent + 4)
        print(" " * indent + f") -> {self.resolved_type or 'unknown'}")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        yield fn(self.left)
        yield fn(self.right)


class UnOp(ASTNode):
    """
    An operation on one node.

    Currently, the only unary operation is -
    """

    def __init__(self, op: TokenType, right: ASTNode) -> None:
        assert op == TokenType.MINUS
        self.op = op
        self.right = right

    def __repr__(self) -> str:
        return f"UnOp({self.op} {self.right})"

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        # the only unary op is -
        if not self.right.resolve_type(env).is_subset(ASTConcreteType.NUMBER):
            self.right = Cast(self.right, ASTConcreteType.NUMBER)

        self.resolved_type = ASTConcreteType.NUMBER
        return ASTConcreteType.NUMBER

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f"UnOp({self.op}")
        self.right.output(indent + 4)
        print(" " * indent + f") -> {self.resolved_type or 'unknown'}")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        yield fn(self.right)


class Number(ASTNode):
    def __init__(self, value: float) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Number({self.value})"

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        self.resolved_type = ASTConcreteType.NUMBER
        return ASTConcreteType.NUMBER

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f"Number({self.value})")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        return
        yield


class String(ASTNode):
    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f'String("{self.value}")'

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        self.resolved_type = ASTConcreteType.STRING
        return ASTConcreteType.STRING

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f'String("{self.value}")')

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        return
        yield


class Variable(ASTNode):
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f'Variable("{self.name}")'

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        if typ := env.variables.get(self.name):
            self.resolved_type = typ
            return typ

        raise FormulaTypeException(message=f"variable {self.name} does not exist")

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f'Variable("{self.name}") -> {self.resolved_type}')

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        return
        yield


class FunctionCall(ASTNode):
    def __init__(self, name: str, args: List[ASTNode]) -> None:
        self.name = name
        self.args = args

    def __repr__(self) -> str:
        return f'FunctionCall("{self.name}", {self.args})'

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        if validator := env.functions.get(self.name):
            typ = validator(self, [x.resolve_type(env) for x in self.args])
            self.resolved_type = typ
            return typ

        raise FormulaTypeException(message=f"function {self.name} does not exist")

    def output(self, indent: int = 0) -> None:
        print(" " * indent + f'FunctionCall("{self.name}"')
        for arg in self.args:
            arg.output(indent + 4)
        print(" " * indent + f") -> {self.resolved_type or 'unknown'}")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        for node in self.args:
            yield fn(node)


class Array(ASTNode):
    def __init__(self, elements: List[ASTNode]) -> None:
        self.elements = elements

    def __repr__(self) -> str:
        return f"Array({self.elements})"

    def resolve_type(self, env: ASTTypeEnvironment) -> ASTType:
        typ = ASTListType(ASTTypeUnion(*(x.resolve_type(env) for x in self.elements)))
        self.resolved_type = typ
        new_elements = []

        for element in self.elements:
            assert element.resolved_type

            if element.resolved_type.is_subset(typ):
                new_elements.append(element)
            else:
                new_elements.append(Cast(element, typ))

        return typ

    def output(self, indent: int = 0) -> None:
        print(" " * indent + "Array(")
        for element in self.elements:
            element.output(indent + 4)
        print(" " * indent + f") -> {self.resolved_type or 'unknown'}")

    def recurse[T](self, fn: Callable[[ASTNode], T]) -> Generator[T, None, None]:
        for node in self.elements:
            yield fn(node)


class Parser:
    def __init__(self) -> None:
        self.tokens: List[Token] = []
        self.pos: int = 0

    def current_token(self) -> Token:
        return (
            self.tokens[self.pos]
            if self.pos < len(self.tokens)
            else Token(TokenType.EOF, "")
        )

    def eat(self, token_type: TokenType) -> None:
        if self.current_token().kind == token_type:
            self.pos += 1
        else:
            raise Exception(
                f"Unexpected token {self.current_token().kind}, expected {token_type}"
            )

    def parse(self, tokens: List[Token]) -> Optional[ASTNode]:
        self.tokens = tokens
        self.pos = 0
        if not self.tokens:
            return None

        ret = self.expression()

        if (token := self.current_token().kind) != TokenType.EOF:
            raise Exception(f"parser didn't consume all tokens, found {token}")

        return ret

    def expression(self, precedence: int = 0) -> ASTNode:
        left = self.primary()
        while True:
            token = self.current_token()
            token_precedence = self.get_precedence(token.kind)

            if token_precedence <= precedence:
                break

            op = token.kind
            self.eat(op)
            right = self.expression(token_precedence)
            left = BinOp(left, op, right)

        return left

    def primary(self) -> ASTNode:
        token = self.current_token()

        if token.kind == TokenType.NUMBER:
            self.eat(TokenType.NUMBER)
            return Number(float(token.value))

        elif token.kind == TokenType.ID:
            # Check if it's a function call
            if self.peek() == TokenType.LPAREN:
                func_name = token.value
                self.eat(TokenType.ID)
                self.eat(TokenType.LPAREN)
                args = []
                while self.current_token().kind != TokenType.RPAREN:
                    args.append(self.expression())
                    if self.current_token().kind == TokenType.COMMA:
                        self.eat(TokenType.COMMA)
                self.eat(TokenType.RPAREN)
                return FunctionCall(func_name, args)
            else:
                # Just a variable
                var_name = token.value
                self.eat(TokenType.ID)
                return Variable(var_name)
        elif token.kind == TokenType.STRING:
            self.eat(TokenType.STRING)
            return String(token.value[1:-1])

        elif token.kind == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            node = self.expression()
            self.eat(TokenType.RPAREN)
            return node

        elif token.kind == TokenType.MINUS:
            self.eat(TokenType.MINUS)
            return UnOp(TokenType.MINUS, self.primary())

        elif token.kind == TokenType.LBRACK:
            self.eat(TokenType.LBRACK)
            e = []
            while self.current_token().kind != TokenType.RBRACK:
                e.append(self.expression())
                if self.current_token().kind == TokenType.COMMA:
                    self.eat(TokenType.COMMA)
            self.eat(TokenType.RBRACK)
            return Array(e)

        elif token.kind == TokenType.VARIABLE_NAME:
            name = token.value
            self.eat(TokenType.VARIABLE_NAME)
            return Variable(name)

        else:
            raise Exception(f"Unexpected token {token.kind}")

    def peek(self) -> TokenType:
        return (
            self.tokens[self.pos + 1].kind
            if self.pos + 1 < len(self.tokens)
            else TokenType.EOF
        )

    def get_precedence(self, token_type: TokenType) -> int:
        precedences = {
            TokenType.PLUS: 10,
            TokenType.AMPERSAND: 10,
            TokenType.MINUS: 10,
            TokenType.MUL: 20,
            TokenType.DIV: 20,
            TokenType.EQ: 7,
            TokenType.NE: 7,
            TokenType.LT: 7,
            TokenType.GT: 7,
            TokenType.LE: 7,
            TokenType.GE: 7,
        }

        return precedences.get(token_type, 0)
