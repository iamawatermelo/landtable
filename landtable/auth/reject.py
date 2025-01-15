"""
Reject all requests.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from contextlib import asynccontextmanager

from starlette.requests import Request

from landtable.auth import AuthenticationPlugin
from landtable.auth.abstract import AccessType
from landtable.auth.abstract import AuthenticationContext
from landtable.auth.abstract import Identity
from landtable.auth.abstract import Resource
from landtable.exceptions import APIForbidden


class RejectAuthenticationContext(AuthenticationContext):
    """
    An AuthenticationContext represents a user's request.
    """

    identity: Identity

    @asynccontextmanager
    async def evaluate(self, actions: set[AccessType], on: Resource):
        """
        Reject all requests.
        """

        raise APIForbidden()
        yield


class RejectAuthenticationPlugin(AuthenticationPlugin):
    """
    Reject all requests.
    """

    async def create_context(self, request: Request) -> RejectAuthenticationContext:
        """
        Attempt to authenticate this request.

        On success, return an AuthenticationContext object.
        """

        return RejectAuthenticationContext()
