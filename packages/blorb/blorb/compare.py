"""
Benchmarks for Blorb
"""
import sys
from time import monotonic_ns

# To get this to import correctly, we must do shenanigans
sys.path.append("./benchmark/captainexpo_fp/src")
import main as ce_formula
sys.path.pop()

import landtable_legacy.formula.formula as lt_formula

import blorb

def main():
    formula = input(">>> ")
    iterations = 1000000
    
    print(f"Running {iterations} iterations")

    start = monotonic_ns()
    for i in range(iterations):
        blorb.parse(formula)
    end = monotonic_ns()

    print(f"blorb: took {(end - start) / 1_000_000 :.2f}ms ({((end - start) / iterations) / 1_000 :.2f} us/it)")

    start = monotonic_ns()
    for i in range(iterations):
        ce_formula.Formula(formula)
    end = monotonic_ns()

    print(f"Captainexpo-1/FormulaParser: took {(end - start) / 1_000_000 :.2f}ms ({((end - start) / iterations) / 1000:.2f} us/it)")

    start = monotonic_ns()
    for i in range(iterations):
        lt_formula.Formula(formula)
    end = monotonic_ns()

    print(f"landtable: took {(end - start) / 1_000_000 :.2f}ms ({((end - start) / iterations) / 1000 :.2f} us/it)")

if __name__ == "__main__":
    main()