"""
Landtable API exceptions.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from typing import Any
from typing import Literal
from typing import TypeAlias
from typing import Union

from pydantic import Field
from pydantic.dataclasses import dataclass


LandtableExceptionCode: TypeAlias = Union[
    Literal["NOT_FOUND"],
    Literal["NOT_ALLOWED"],
    Literal["BAD_REQUEST"],
    Literal["RATE_LIMITED"],
    Literal["INTERNAL_ERROR"],
    Literal["TEMPORARILY_UNAVAILABLE"],
    Literal["UNAUTHORIZED"],
    Literal["PREDICATE_FAILED"]
]


@dataclass
class BaseAPIException(Exception):
    code: int = Field(default=500)
    type: LandtableExceptionCode = Field(default="INTERNAL_ERROR")
    message: str = Field(default="Unknown exception")
    detail: Any = Field(default=None)
    
    def __str__(self):
        return f"{self.code} {self.type}: {self.message}"


@dataclass
class APIUnavailable(BaseAPIException):
    code: int = Field(default=503)
    type: LandtableExceptionCode = Field(default="TEMPORARILY_UNAVAILABLE")


@dataclass
class APINotFoundException(BaseAPIException):
    code: int = Field(default=404)
    type: LandtableExceptionCode = Field(default="NOT_FOUND")


@dataclass
class APIBadRequestException(BaseAPIException):
    code: int = Field(default=400)
    type: LandtableExceptionCode = Field(default="BAD_REQUEST")


@dataclass
class APIUnauthorized(BaseAPIException):
    code: int = Field(default=401)
    type: LandtableExceptionCode = Field(default="UNAUTHORIZED")
    message: str = Field(default="No authentication credentials found")


@dataclass
class APIForbidden(BaseAPIException):
    code: int = Field(default=403)
    type: LandtableExceptionCode = Field(default="NOT_ALLOWED")
    message: str = Field(default="You can't do that")

@dataclass
class APIPredicateFailed(BaseAPIException):
    code: int = Field(default=412)
    type: LandtableExceptionCode = Field(default="PREDICATE_FAILED")
