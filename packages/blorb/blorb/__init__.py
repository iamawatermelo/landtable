"""
Wrapper functions for Blorb's internals.
"""
from dataclasses import dataclass
import blorb._blorb as rs


@dataclass
class ParseError(Exception):
    inner: list[rs.WrappedCompilationError]
    
    def __str__(self) -> str:
        return f"Error while parsing:\n  - {"\n  - ".join(str(x) for x in self.inner)}"


def parse(source: str) -> rs.WrappedFormula:
    result = rs.compile(source)
    
    if isinstance(result, rs.WrappedFormula):
        return result
    
    raise ParseError(result)
