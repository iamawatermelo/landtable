"""
REPL for Blorb.
"""
from time import monotonic_ns
import blorb


def main():
    while True:
        formula = input(">>> ")
        
        start = monotonic_ns()
        try:
            compiled_formula = blorb.parse(formula)
        except blorb.ParseError as e:
            print(e)
            continue
        end = monotonic_ns()
        
        print(f"Took {(end - start) * 0.001 :.2f} microseconds: {compiled_formula}")


if __name__ == "__main__":
    main()
