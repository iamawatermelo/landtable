"""
Rust internals of Blorb. Do not interact with this module directly.
"""


class WrappedCompilationError:
    span: tuple[int, int]
    message: str


class WrappedAST:
    def __str__(self): ...


def compile(source: str) -> WrappedAST | list[WrappedCompilationError]: ...