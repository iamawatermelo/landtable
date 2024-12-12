"""
Landtable formula parsing exceptions.
"""
# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.


class FormulaException(Exception):
    pass


class FormulaTypeException(FormulaException):
    pass


class FormulaParseException(FormulaException):
    pass
