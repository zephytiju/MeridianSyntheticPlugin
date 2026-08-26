#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

python scripts/check_boundaries.py
ruff check .
ruff format --check .
mypy src
PYTHONPATH=src pytest --cov=meridian_storage.plugins.synthetic --cov-report=term
python -m build
python -m twine check dist/*.whl dist/*.tar.gz
