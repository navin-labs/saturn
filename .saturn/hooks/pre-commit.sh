#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if git diff --cached --name-only | grep -E '(^|/)(\\.env|telegram\\.env|secrets?/)' >/dev/null; then
  echo "Blocked: secret material staged." >&2
  exit 1
fi

py_files="$(git diff --cached --name-only -- '*.py')"
if [ -n "$py_files" ]; then
  for file in $py_files; do
    python3 -m py_compile "$file"
  done
fi
