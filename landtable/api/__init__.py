"""
Landtable's API, which is an ASGI app.
"""

from __future__ import annotations

import dataclasses
import traceback
from contextlib import asynccontextmanager
from logging import basicConfig
from logging import getLogger
from typing import Any

from fastapi import FastAPI
from fastapi import Request
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from landtable.core.models.config import ConfigurationModel

# from .legacy import legacy_router
# from .transactions import transaction_router
from landtable.exceptions import BaseAPIException
from landtable.core import Landtable
from landtable.tracing import Tracer

import importlib.resources as resources

logger = getLogger(__name__)


class TracingResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        tracer = Tracer().from_context()

        if not isinstance(tracer, Tracer):
            return super().render(content)

        tracer.finish()

        if not isinstance(content, dict):
            return super().render(content)

        content["_trace"] = tracer.compute_trace()

        return super().render(content)


def LandtableASGI():
    basicConfig(level="DEBUG")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tracer = Tracer()

        app.state.landtable = Landtable(ConfigurationModel())

        async with app.state.landtable.enter():
            yield

    app = FastAPI(lifespan=lifespan, default_response_class=TracingResponse)
    
    @app.middleware("http")
    async def middleware(request: Request, call_next):
        tracer = Tracer()

        with tracer.trace("request"):
            try:
                return await call_next(request)
            except BaseAPIException as e:
                content = dataclasses.asdict(e)
                content["_stack"] = traceback.format_tb(e.__traceback__)
                status = e.code
                headers = None
            except Exception as e:
                tracer.finish()
                logger.debug(f"Exception trace: {tracer.compute_json_trace()}")
                raise e

        tracer.finish()
        content["_trace"] = tracer.compute_trace()

        response = JSONResponse(content=content, status_code=status, headers=headers)

        return response

    return app
