#!/bin/bash
set -euo pipefail

rm -f ./dist/*.whl ./dist/*.tar.gz
uv build
uv run twine upload dist/*.whl --verbose
