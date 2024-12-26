"""
Landtable's API, which is an ASGI app.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .legacy import legacy_router
from .transactions import transaction_router
from landtable.state import LandtableState


def Landtable():
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.landtable = LandtableState("lcoalhost:2379")
        await app.state.landtable.connect()

        yield

        await app.state.landtable.shutdown()

    app = FastAPI()

    app.include_router(legacy_router)
    app.include_router(transaction_router)

    return app
