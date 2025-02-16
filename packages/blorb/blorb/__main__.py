"""
REPL for Blorb.
"""
import blorb


def main():
    while True:
        formula = input(">>> ")
        
        try:
            compiled_formula = blorb.parse(formula)
        except blorb.ParseError as e:
            print(e)
            continue
        
        print(f"Parsed formula: {compiled_formula}")


if __name__ == "__main__":
    main()
