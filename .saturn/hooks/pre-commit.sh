#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if git diff --cached --name-only | grep -E '(^|/)(\\.env|telegram\\.env|secrets?/|.*\\.pem$|.*\\.key$)' >/dev/null; then
  echo "Blocked: secret material staged." >&2
  exit 1
fi

if find "$ROOT" -mindepth 2 -type d -name .git | grep -q .; then
  echo "Blocked: nested .git directory detected." >&2
  exit 1
fi

if git diff --cached --name-only | grep -E '(^|/)(__pycache__/|.*\\.(pyc|pyo|log|tmp))$' >/dev/null; then
  echo "Blocked: generated garbage files staged." >&2
  exit 1
fi

py_files="$(git diff --cached --name-only -- '*.py')"
if [ -n "$py_files" ]; then
  for file in $py_files; do
    python3 -m py_compile "$file"
  done
fi
