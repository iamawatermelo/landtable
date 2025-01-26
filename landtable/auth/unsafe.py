"""
UNSAFE Landtable authentication module: simply accepts all requests.
Do not use this.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from starlette.requests import Request

from landtable.auth import AuthenticationPlugin
from landtable.auth.abstract import AccessType
from landtable.auth.abstract import AuthenticationContext
from landtable.auth.abstract import Identity
from landtable.auth.abstract import Resource


class UnsafeAuthenticationContext(AuthenticationContext):
    """
    An AuthenticationContext represents a user's request.
    """

    identity: Identity = Identity(
        name="anonymous",
        metadata={}
    )

    async def _evaluate(self, actions: set[AccessType], on: Resource):
        """
        Accept all requests.
        """
        
        pass


class UnsafeAuthenticationPlugin(AuthenticationPlugin):
    """
    This UNSAFE authentication plugin simply allows all requests.
    Do not use this.
    """

    async def create_context(self, request: Request) -> UnsafeAuthenticationContext:
        """
        Attempt to authenticate this request.

        On success, return an AuthenticationContext object.
        """

        return UnsafeAuthenticationContext()
