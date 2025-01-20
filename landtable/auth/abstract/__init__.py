"""
Authentication module protocols.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
from __future__ import annotations

from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, ClassVar
from typing import AsyncContextManager
from typing import Callable
from typing import Protocol
from landtable.exceptions import APIForbidden

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

    READ = "read"
    """
    Read from this resource.
    """

    WRITE = "write"
    """
    Write something to this resource.
    """

    MODIFY = "modify"
    """
    Modify this resource.
    """

    DELETE = "delete"
    """
    Delete this resource.
    """
    
    EXECUTE = "execute"
    """
    Execute something on this resource.
    """


class Resource:
    """
    Something that an Identity can perform an AccessType on.
    Should be subclassed.
    """

    id: ClassVar[str]
    """
    Something unique identifying this resource, like lt.workspace.
    
    Do not use the `lt.` namespace unless you are a first-party
    Landtable plugin to avoid confusion and conflicts.
    """
    
    depset: ClassVar[list[str]]
    """
    A suggestion of what access to this resource depends on to be useful.
    
    For example, lt.workspace.aliases may require lt.workspace:READ.
    
    This is not and should not be enforced and is purely a suggestion
    for users.
    """

    @property
    def resource_name(self) -> str:
        """
        A human-readable resource name shown in error messages.
        """

        raise NotImplementedError


class ContextFailedException(Exception):
    """
    Thrown when an authentication context successfully validated that the
    current context is not able to do this action.
    """


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

    @asynccontextmanager
    async def evaluate(self, actions: set[AccessType], on: Resource):
        """
        Answer the question: can this context perform [actions] on [identifier]?
        On success, returns an AsyncContextManager.
        On failure, throws an exception.
        """

        try:
            async with self._evaluate(actions, on):
                yield
        except ContextFailedException:
            pass

        raise APIForbidden(
            message=f"current caller identity cannot perform {', '.join(x.value for x in actions)} on {on.resource_name}"
        )

    def _evaluate(self, actions: set[AccessType], on: Resource) -> AsyncContextManager:
        """
        Inner function for the evaluate function. Throw a ContextFailedException
        when the user may not perform this set of actions on that resource.
        """

        raise ContextFailedException


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
