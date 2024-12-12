#!/usr/bin/env sh
reorder-python-imports landtable/**/*.py --py312-plus
ruff format
