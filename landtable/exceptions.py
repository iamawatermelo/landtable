"""
Landtable API exceptions.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from dataclasses import dataclass
from typing import Any
from typing import Literal
from typing import TypeAlias
from typing import Union

from pydantic import BaseModel
from pydantic import Field

from landtable.formula.exceptions import FormulaException


LandtableExceptionCode: TypeAlias = Union[
    Literal["NOT_FOUND"],
    Literal["NOT_ALLOWED"],
    Literal["BAD_REQUEST"],
    Literal["RATE_LIMITED"],
    Literal["INTERNAL_ERROR"],
    Literal["TEMPORARILY_UNAVAILABLE"],
]


class BaseAPIException(Exception, BaseModel):
    code: int
    type: LandtableExceptionCode
    message: str
    detail: Any = Field(default=None)


class APINotFoundException(BaseAPIException):
    code: int = Field(default=404)
    type: LandtableExceptionCode = Field(default="NOT_FOUND")


class APIBadRequestException(BaseAPIException):
    code: int = Field(default=400)
    type: LandtableExceptionCode = Field(default="BAD_REQUEST")
