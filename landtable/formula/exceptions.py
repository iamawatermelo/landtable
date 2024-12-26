"""
Landtable formula parsing exceptions.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from pydantic import Field

from landtable.exceptions import BaseAPIException
from landtable.exceptions import LandtableExceptionCode


class FormulaException(BaseAPIException):
    code: int = Field(default=400)
    type: LandtableExceptionCode = Field(default="BAD_REQUEST")


class FormulaTypeException(FormulaException):
    pass


class FormulaParseException(FormulaException):
    pass


class FormulaInternalException(FormulaException):
    code: int = Field(default=500)
    type: LandtableExceptionCode = Field(default="INTERNAL_ERROR")
