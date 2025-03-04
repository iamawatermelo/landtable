"""
Benchmarks for Blorb
"""

import sys
import blorb
from time import monotonic_ns


EXAMPLE = """
let
    {Neko's rating} := 1520,
    {Elliot's rating} := 1867,
    {K-factor} := 32,
    
    # Functions are values in Blorb.
    {Expected score function} := |{Player's rating}, {Opponent's rating}|
        1 / (1 + 10 ^ (({Opponent's rating} - {Player's rating}) / 400)),
    
    # Let bindings can reference previous let bindings inside of itself.
    {Neko's expected score} := {Expected score function}(
        {Neko's rating},
        {Elliot's rating}
    ),
    
    {Neko's actual scores} := [1, 0, 0, 0.5, 1],
    {Neko's total actual score} := SUM({Neko's actual scores}),
    {Neko's total expected score} := {Neko's expected score} * LEN({Neko's actual scores}),
    
    {Neko's new rating} := FLOOR(
        {Neko's rating} + (
            {K-factor} * (
                {Neko's total actual score} - {Neko's total expected score}
            )
        )
    )
of
    # When expressions allow a more ergonomic alternative to nested IF
    # calls.
    when {Neko's new rating}
        {Neko's rating} => "Neko's rating hasn't changed",
        {Neko's ratkng}!.. => "Neko's rating increased to " & {Neko's new rating},
        ..{Neko's rating} => "Neko's rating decreased to " & {Neko's new rating}
"""

def main():
    iterations = 100_000
    
    try:
        file = open(sys.argv[1]).read()
    except IndexError:
        file = EXAMPLE
    
    print(f"Parsing this file {iterations} times:\n{file}")
    
    start = monotonic_ns()
    for _ in range(iterations):
        blorb.parse(file)
    end = monotonic_ns()
    
    print(f"blorb: took {(end - start) / 1_000_000 :.2f}ms ({((end - start) / iterations) / 1_000 :.2f} us/it)")

if __name__ == "__main__":
    main()