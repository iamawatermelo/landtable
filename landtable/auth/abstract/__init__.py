"""
Authentication module protocols.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, ClassVar
from typing import AsyncContextManager
from typing import Callable
from typing import Protocol

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

if TYPE_CHECKING:
    from landtable.state import LandtableState
    from landtable.auth import AuthenticationPluginResolver


@dataclass
class Identity:
    """
    An "identity" is something like a user, or a service, or otherwise.
    Authentication modules can provide extra metadata.
    """

    name: str
    metadata: dict[str, Any]


class AccessType(Enum):
    """
    An operation. No operation implies permission to run another operation, e.g
    WRITE access does not imply READ access too.
    """

    READ = auto()
    """
    Read from this resource.
    """

    WRITE = auto()
    """
    Write something to this resource.
    """

    MODIFY = auto()
    """
    Modify this resource.
    """

    DELETE = auto()
    """
    Delete this resource.
    """


class Resource:
    """
    Something that an Identity can perform an AccessType on.
    Should be subclassed.

    **Unknown resources to an access provider must raise an exception.**
    """

    resource_id: ClassVar[str]

    @property
    def resource_name(self) -> str:
        """
        A human-readable resource name shown in error messages.
        """

        raise NotImplementedError


class AuthenticationContext(Protocol):
    """
    An AuthenticationContext represents a user's request.
    """

    identity: Identity
    context: ClassVar = ContextVar("authentication_context")

    @contextmanager
    def enter(self):
        """
        Enter this AuthenticationContext.
        """

        token = self.context.set(self)

        yield

        self.context.reset(token)

    @classmethod
    def from_context(cls) -> AuthenticationContext:
        """
        Get the current AuthenticationContext. Raises LookupError if there is
        no current AuthenticationContext.
        """

        return cls.context.get()

    def evaluate(self, actions: set[AccessType], on: Resource) -> AsyncContextManager:
        """
        Answer the question: can this context perform [actions] on [identifier]?
        On success, returns a ContextManager.
        On failure, throw a subclass of BaseAPIException if the user is
        unauthorized and Exception if something wrong has happened.
        """

        ...


class AuthenticationPlugin[C: AuthenticationContext](Protocol):
    """
    A Landtable authentication plugin.
    """

    async def create_context(self, request: Request) -> C | None:
        """
        Attempt to authenticate this request.

        On success, return an AuthenticationContext object.
        If this plugin can't authenticate this request, it should return None to
        let another plugin handle this request.
        """

        ...

    async def transform_response(self, response: Response, ctx: C):
        """
        Transform an API response to, for example, add CORS headers.
        This is only called when this authentication plugin was the plugin that
        provided the authentication context for this request.
        """

        return response

    async def setup(
        self,
        plugins: AuthenticationPluginResolver,
        state: LandtableState,
        mount: Callable[[str, ASGIApp], None],
    ):
        """
        Do any setup tasks.
        Optionally, mount an ASGI application with the provided callback.
        The first argument is the base path of your app (e.g "/auth/oauth2").
        The second argument is the ASGI application.
        """

        pass

    async def shutdown(self):
        """
        Do any shutdown tasks.
        """

        pass
