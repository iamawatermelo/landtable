"""
Landtable's CLI.
"""

from __future__ import annotations

import os

import uvicorn

from landtable.api import Landtable


def main():
    try:
        import uvloop as asyncio
    except ImportError:
        import asyncio

    server = uvicorn.Server(
        uvicorn.Config(app=Landtable(), port=int(os.environ.get("PORT", 8080)))
    )

    asyncio.run(server.serve())


if __name__ == "__main__":
    main()
