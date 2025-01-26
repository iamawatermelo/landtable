"""
Landtable's CLI.
"""

from __future__ import annotations

from logging import getLogger
import os
from pathlib import Path
from typing import Annotated

import uvicorn
import typer

from landtable.iac import iac

logger = getLogger(__name__)
app = typer.Typer()
app.add_typer(iac, name="iac")


@app.command()
def serve():
    # move API import here to reduce start times
    from landtable.api import Landtable
    
    try:
        import uvloop as asyncio
    except ImportError:
        logger.warning("uvloop isn't available!")
        import asyncio

    server = uvicorn.Server(
        uvicorn.Config(app=Landtable(), port=int(os.environ.get("PORT", 8080)))
    )

    asyncio.run(server.serve())


# for support with `python3 -m landtable`:
if __name__ == "__main__":
    app()
