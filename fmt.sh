#!/usr/bin/env sh
reorder-python-imports landtable/**/*.py --py312-plus --add-import='from __future__ import annotations'
ruff format
