"""
A cache utility.

TODO: Delete old cache entries
"""


import asyncio
from typing import Callable, Coroutine
from pydantic.dataclasses import dataclass
from time import monotonic

from landtable.tracing import Tracer


@dataclass
class CacheEntry[T, C]:
    cache_token: C
    value: T
    issued_at: float


class Cache[T, C, K]:
    expiry_time: float
    """
    How long (in seconds) that cache entries should live for.
    """
    
    refresh: Callable[[C], Coroutine[None, None, None]]
    """
    Refresh cache entries based on their cache token.
    Use put to add cache entries. Raise LookupError if it no longer
    exists.
    """
    
    callback: Callable[[K], Coroutine[None, None, T]]
    """
    Get a value by its key. Raise LookupError if it could not be found.
    """
    
    name: str
    """
    Name to use in tracing and exceptions.
    """
    
    cache: dict[K, CacheEntry[T, C]]
    
    callback_tasks: dict[K, asyncio.Task]
    refresh_tasks: dict[K, asyncio.Task]
    
    def __init__(
        self,
        expiry_time_ms: float,
        callback: Callable[[K], Coroutine[None, None, T]],
        refresh: Callable[[C], Coroutine[None, None, None]],
        name: str = ""
    ):
        self.expiry_time_ms = expiry_time_ms
        self.callback = callback
        self.refresh = refresh
        self.cache = dict()
        self.name = name
    
    def put(
        self,
        key: K,
        cache_token: C,
        value: T
    ):
        self.cache[key] = CacheEntry(
            cache_token=cache_token,
            value=value,
            issued_at=monotonic()
        )
    
    async def get(
        self,
        key: K
    ):
        """
        Get a cache entry or raise LookupError if it does not exist.
        """
        
        if (task := self.refresh_tasks.get(key)) is not None:
            await task
        
        if (value := self.cache.get(key)) is not None:
            if monotonic() - value.issued_at > self.expiry_time:
                with Tracer.from_context().trace(
                    "cache",
                    f"cache {self.name} hit for {key} but it was expired"
                ):
                    task = asyncio.create_task(self.callback(key))
                    self.refresh_tasks[key] = task
                    
                    await task
                    value = self.cache.get(key)
                    
                    if value is None:
                        raise LookupError
            
            Tracer.from_context().instant_event(
                "cache",
                f"cache {self.name} hit for {key}"
            )
            return value.value
        
        with Tracer.from_context().trace(
            "cache",
            f"cache {self.name} miss for {key}"
        ):
            value = await self.callback(key)
            