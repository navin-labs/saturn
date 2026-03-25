#!/bin/bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  exit 0
fi

case "$1" in
  *.py) python3 -m py_compile "$1" ;;
esac
