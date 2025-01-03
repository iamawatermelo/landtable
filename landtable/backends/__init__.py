"""
Fetch database backends.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from logging import getLogger

from landtable.backends.abstract import DatabaseBackend
from landtable.tracing import Tracer

logger = getLogger(__name__)


class BackendResolver:
    backends: list[DatabaseBackend]
    config_to_backend_map: dict[str, DatabaseBackend]
    initialised = False

    def __init__(self):
        discovered_plugins = entry_points(group="landtable.backends")

        with Tracer.from_context().trace("backend", "Discover all backends"):
            self.backends = [plugin.load()() for plugin in discovered_plugins]
            self.config_to_backend_map = dict()

            logger.debug(f"Found {len(self.backends)} backends:")

            for backend in self.backends:
                logger.debug(
                    f"- {type(backend).__qualname__} ({len(type(backend).BACKEND_INFORMATION.config_types)} associated types)"
                )

                self.config_to_backend_map.update(
                    (type, backend) for type in backend.BACKEND_INFORMATION.config_types
                )

    async def initialise(self):
        with Tracer.from_context().trace("backend", "Initialise all backends"):
            # TODO: use asyncio taskgroup
            for backend in self.backends:
                logger.debug(f"Initializing {backend}")
                await backend.setup()

        self.initialised = True

    def fetch_backend_for_config_type(self, config_type: str) -> DatabaseBackend:
        if not self.initialised:
            raise RuntimeError("not initialised")

        return self.config_to_backend_map[config_type]
