#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/../../configs/base_path.env"

ensure_saturn_dirs() {
  mkdir -p "$LOGS_DIR" "$DATABASE_DIR" "$DOCS_DIR" "$SKILLS_DIR" "$CONTROL_DIR"
}

require_base_path() {
  ensure_saturn_dirs
  local raw="${1:?path required}"
  local purpose="${2:-write}"
  local base target
  base="$(realpath -m "$BASE_PATH")"
  target="$(realpath -m "$raw")"

  case "$target" in
    "$base"|"$base"/*) return 0 ;;
    *)
      printf '%s DENY purpose=%s path=%s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$purpose" "$target" >> "$SECURITY_LOG"
      echo "Write blocked outside BASE_PATH: $target" >&2
      return 1
      ;;
  esac
}
