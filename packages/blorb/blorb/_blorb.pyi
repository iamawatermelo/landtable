"""
Rust internals of Blorb. Do not interact with this module directly.
"""


class UserSpan:
    line: int
    column: int
    line_index_start: int
    line_index_end: int


class WrappedCompilationError:
    span: tuple[int, int]
    message: str
    associated_hints: list[WrappedCompilationError]
    line_col: None | UserSpan
    
    def __str__(self): ...
    
    def __repr__(self): ...


class WrappedFormula:
    def __str__(self): ...


def compile(source: str) -> WrappedFormula | list[WrappedCompilationError]: ...