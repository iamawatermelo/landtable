"""
Landtable's API, which is an ASGI app.
"""

from __future__ import annotations

import dataclasses
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass
from logging import basicConfig
from logging import getLogger
from typing import Any

from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.datastructures import Headers
from pydantic import BaseModel
from starlette.responses import JSONResponse

from .legacy import legacy_router
from .transactions import transaction_router
from landtable.exceptions import BaseAPIException
from landtable.state import LandtableState
from landtable.tracing import Tracer

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


def Landtable():
    basicConfig(level="DEBUG")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tracer = Tracer()

        with tracer.trace("app startup"):
            app.state.landtable = LandtableState("etcd://localhost:2379")
            await app.state.landtable.connect()

        tracer.finish()
        logger.debug(f"Trace for app startup: {tracer.compute_json_trace()}")

        yield

        await app.state.landtable.shutdown()

    app = FastAPI(lifespan=lifespan, default_response_class=TracingResponse)

    app.include_router(legacy_router)
    app.include_router(transaction_router)

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
