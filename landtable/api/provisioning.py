"""
Landtable provisioning API
"""

from __future__ import annotations

from fastapi import APIRouter

provisioning_router = APIRouter(prefix="/api/provisioning")

@provisioning_router.get("/execute")
async def execute():
    