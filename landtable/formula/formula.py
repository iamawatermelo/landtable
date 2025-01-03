"""
Tools for parsing a Landtable formula.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from typing import Any
from typing import TypeAlias

from pydantic import GetCoreSchemaHandler
from pydantic import ValidationInfo
from pydantic import ValidatorFunctionWrapHandler
from pydantic_core import core_schema
from pydantic_core.core_schema import CoreSchema

from landtable.formula.exceptions import FormulaParseException
from landtable.formula.lexer import lex
from landtable.formula.parse import Parser


class Formula:
    def __init__(self, code: str):
        self.code = code
        self.parser = Parser()

        ast = self.parse()

        if ast is None:
            raise FormulaParseException(message="empty formula")

        self.ast = ast

    def parse(self):
        tokens = lex(self.code)
        return self.parser.parse(tokens)

    @classmethod
    def validate(cls, value: str, info: ValidationInfo):
        return cls(value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_after_validator_function(
            cls.validate, handler(str)
        )
