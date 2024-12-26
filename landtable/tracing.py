"""
Utilities for creating Server-Timing traces.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
import json
import time
from contextlib import asynccontextmanager
from contextlib import contextmanager
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from itertools import chain
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Self

from landtable import __version__

logger = getLogger(__name__)


@dataclass
class TraceEvent:
    """
    A tracing event that happened over a period of time.
    """

    start: int
    """
    The start time, in nanoseconds, of this trace event.
    The only valid comparison is to compare against other trace objects.
    """

    end: int
    """
    The end time, in nanoseconds, of this trace event.
    The only valid comparison is to compare against other trace objects.
    """

    identifier: str
    """
    A very short identifier naming this trace event (like "db").
    """

    description: str | None
    """
    A short description of this trace event.
    """

    detail: Dict[str, Any] | None = None
    """
    Any extra information that should be included in the trace.
    """


@dataclass
class InstantEvent:
    """
    A tracing event that happened instantaneously.
    """

    start: int
    """
    The time this event occured, in nanoseconds, of this trace event.
    The only valid comparison is to compare against other trace objects.
    """

    identifier: str
    """
    A very short identifier naming this trace event (like "db")
    """

    description: str | None
    """
    A short description of this trace event.
    """

    detail: Dict[str, Any] | None = None
    """
    Any extra information that should be included in the trace.
    """


class DummyTracer:
    """
    Tracer that does nothing.
    """

    @contextmanager
    def trace(self, identifier: str, description: str | None = None):
        yield

    @asynccontextmanager
    async def async_trace(self, identifier: str, description: str | None = None):
        yield

    def instant_event(self, identifier: str, description: str | None = None):
        pass


class Tracer(DummyTracer):
    """
    Creates Server-Timing information and traces compatible with the Chrome
    tracing format.
    """

    start: int
    end: int | None
    trace_events: List[TraceEvent]
    instant_events: List[InstantEvent]
    context_token: Token

    def __init__(self):
        self.start = time.perf_counter_ns()
        self.trace_events = list()
        self.end = None
        self.context_token = ContextVar("tracer").set(self)

    @staticmethod
    def from_context() -> DummyTracer:
        return ContextVar("tracer").get()

    def finish(self):
        self.end = time.perf_counter_ns()
        ContextVar("tracer").reset(self.context_token)

    def instant_event(self, identifier: str, description: str | None = None):
        self.instant_events.append(
            InstantEvent(
                start=time.perf_counter_ns(),
                identifier=identifier,
                description=description,
            )
        )

    @contextmanager
    def trace(self, identifier: str, description: str | None = None):
        start = time.perf_counter_ns()
        yield
        self.trace_events.append(
            TraceEvent(
                start=start,
                end=time.perf_counter_ns(),
                identifier=identifier,
                description=description,
            )
        )

    @asynccontextmanager
    async def async_trace(self, identifier: str, description: str | None = None):
        start = time.perf_counter_ns()
        yield
        self.trace_events.append(
            TraceEvent(
                start=start,
                end=time.perf_counter_ns(),
                identifier=identifier,
                description=description,
            )
        )

    def compute_server_timing(self) -> str:
        if self.end is None:
            raise Exception("trace is not finished")

        timing_events = list()

        for event in self.trace_events:
            event_str = (
                f"{event.identifier};dur={(self.end - self.start) / 1000000:.4f}"
            )

            if event.description is not None:
                event_str += f';desc="{event.description}"'

            timing_events.append(event_str)

        return ";".join(timing_events)

    def compute_trace(self) -> Dict[str, Any]:
        return {
            "traceEvents": list(
                chain(
                    (
                        {
                            "name": f"{event.identifier}: {event.description}"
                            if event.description
                            else event.identifier,
                            "cat": event.identifier,
                            "ph": "X",
                            "ts": event.start / 1000,
                            "dur": (event.end - event.start) / 1000,
                            "pid": 0,
                            "tid": 0,
                            "args": event.detail,
                        }
                        for event in self.trace_events
                    ),
                    (
                        {
                            "name": f"{event.identifier}: {event.description}"
                            if event.description
                            else event.identifier,
                            "cat": event.identifier,
                            "ph": "i",
                            "ts": event.start / 1000,
                            "pid": 0,
                            "tid": 0,
                            "args": event.detail,
                            "s": "g",
                        }
                        for event in self.instant_events
                    ),
                )
            ),
            "otherData": {"version": f"landtable v{__version__}"},
        }


@contextmanager
def debug_tracer(what: str):
    tracer = Tracer()
    with tracer.trace(what):
        yield
    tracer.finish()
    trace = json.dumps(tracer.compute_trace())
    logger.debug(f"trace finished for {what}: {trace}")
