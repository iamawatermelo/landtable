"""
Fetch authentication plugins.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from logging import getLogger
from typing import TYPE_CHECKING

from landtable.auth.abstract import AuthenticationPlugin
from landtable.tracing import Tracer

if TYPE_CHECKING:
    from landtable.core import Landtable

logger = getLogger(__name__)


class AuthenticationPluginResolver:
    plugins: dict[str, AuthenticationPlugin]
    initialised = False

    def __init__(self):
        discovered_plugins = entry_points(group="landtable.auth")

        with Tracer.from_context().trace("auth", "Discover all authentication plugins"):
            self.plugins = {
                plugin.name: plugin.load()() for plugin in discovered_plugins
            }

            logger.debug(f"Found {len(self.plugins)} authentication plugins:")

            for name, plugin in self.plugins.items():
                logger.debug(f"- {type(plugin).__qualname__} ({name})")

    async def initialise(self, core: Landtable, mount_callback):
        with Tracer.from_context().trace(
            "auth", "Initialise all authentication plugins"
        ):
            # TODO: use asyncio taskgroup
            for name, plugin in self.plugins.items():
                logger.debug(f"Initializing plugin {name}")
                await plugin.setup(self, core, mount_callback)

        self.initialised = True

    async def shutdown(self):
        with Tracer.from_context().trace("auth", "Shutdown all authentication plugins"):
            # TODO: use asyncio taskgroup
            for name, plugin in self.plugins.items():
                logger.debug(f"Shutting down plugin {name}")
                await plugin.shutdown()
