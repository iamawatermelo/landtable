"""
Tokenize Landtable formulae.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
import re
from enum import auto
from enum import Enum
from re import Pattern
from typing import List
from typing import Tuple
from typing import Union


class TokenType(Enum):
    SKIP = auto()
    NUMBER = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACE = auto()
    RBRACE = auto()
    LBRACK = auto()
    RBRACK = auto()
    MUL = auto()
    DIV = auto()
    PLUS = auto()
    MINUS = auto()
    COMMA = auto()
    DOT = auto()
    LE = auto()
    GE = auto()
    EQ = auto()
    NE = auto()
    LT = auto()
    GT = auto()
    AMPERSAND = auto()
    ID = auto()
    STRING = auto()
    MISMATCH = auto()
    EOF = auto()
    VARIABLE_NAME = auto()


# Define token specifications
token_specification: List[Tuple[TokenType, Pattern[str]]] = [
    (i[0], re.compile(i[1]))
    for i in [
        (TokenType.SKIP, r"[ \t\n]+"),  # Skip over spaces, newlines, and tabs
        (TokenType.NUMBER, r"((\d+)\.\d+)(?!\w)"),  # Float
        (TokenType.NUMBER, r"\d+(?!\w)"),  # Integer
        (TokenType.LPAREN, r"\("),  # Left parenthesis
        (TokenType.RPAREN, r"\)"),  # Right parenthesis
        (TokenType.LBRACE, r"\{"),  # Left brace
        (TokenType.RBRACE, r"\}"),  # Right brace
        (TokenType.LBRACK, r"\["),  # Left brack
        (TokenType.RBRACK, r"\]"),  # Right brack
        (TokenType.MUL, r"\*"),  # Multiplication operator
        (TokenType.DIV, r"/"),  # Division operator
        (TokenType.PLUS, r"\+"),  # Addition operator
        (TokenType.MINUS, r"-"),  # Subtraction operator
        (TokenType.COMMA, r","),  # Comma
        (TokenType.DOT, r"\."),  # Dot
        (TokenType.LE, r"<="),  # Less than or equal to
        (TokenType.GE, r">="),  # Greater than or equal to
        (TokenType.EQ, r"="),  # Equal to
        (TokenType.NE, r"!="),  # Not equal to
        (TokenType.LT, r"<"),  # Less than
        (TokenType.GT, r">"),  # Greater than
        (TokenType.AMPERSAND, r"&"),  # String concatenations
        (TokenType.ID, r"[A-Za-z_]\w*"),  # Identifiers
        (TokenType.STRING, r'"(?:\\.|[^"\\])*"'),  # String literals
        (TokenType.MISMATCH, r"."),  # Any other character
    ]
]


class Token:
    """
    A token.
    """

    def __init__(self, kind: TokenType, value: str) -> None:
        self.kind = kind
        self.value = value

    def __str__(self) -> str:
        return f"{self.kind}: {self.value}"

    def __repr__(self) -> str:
        return str(self)


def lex(code: str) -> List[Token]:
    """
    Turn code into a list of tokens.
    """

    pos = 0
    tokens: List[Token] = []
    while pos < len(code):
        match: Union[re.Match[str], None] = None
        if code[pos] == "{":
            pos += 1
            inner = ""
            while code[pos] != "}":
                if code[pos] == "\\":
                    pos += 1
                inner += code[pos]
                pos += 1
            tokens.append(Token(TokenType.VARIABLE_NAME, inner))
            pos += 1
            continue
        for token_kind, regex in token_specification:
            match = regex.match(code, pos)
            if match:
                text = match.group(0)
                if token_kind != TokenType.SKIP:
                    tokens.append(Token(token_kind, text))
                break
        if not match:
            raise Exception(f"Invalid character at position {pos}")
        pos = match.end(0)
    return tokens
