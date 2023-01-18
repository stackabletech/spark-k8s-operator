#!/usr/bin/env bash
set -euo pipefail

# Check if jinja2 is there
if ! command -v jinja2 &> /dev/null
then
  echo "jinja2 could not be found. Use 'pip install jinja2-cli' to install it."
  exit 1
fi

SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.readme"
jinja2 README.md.j2 -o ../README.md
cd ..

python3 scripts/ensure_one_trailing_newline.py README.md
