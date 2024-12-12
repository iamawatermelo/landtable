"""
Tools for parsing a Landtable formula.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from landtable.formula.lexer import lex
from landtable.formula.parse import Parser


class Formula:
    def __init__(self, code: str):
        self.code = code
        self.parser = Parser()

        ast = self.parse()

        if ast is None:
            raise Exception("code is empty")

        self.ast = ast

    def parse(self):
        tokens = lex(self.code)
        return self.parser.parse(tokens)
