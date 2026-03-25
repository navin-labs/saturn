#!/bin/bash
# 10pm daily summary

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/../configs/base_path.env"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/path_guard.sh"

# Source credentials from the secure file
source ~/.config/openclaw-secrets/telegram.env
SQLITE_TIMEOUT_MS="${SATURN_SQLITE_BUSY_TIMEOUT_MS:-30000}"

send_telegram() {
  curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d chat_id="${GROUP_ID}" \
    -d message_thread_id="${THREAD_PULSE}" \
    -d parse_mode="Markdown" \
    -d text="$1" > /dev/null
}

sqlite_query() {
  local sql="$1"
  local fallback="${2:-0}"
  local output
  if ! output=$(sqlite3 -cmd ".timeout ${SQLITE_TIMEOUT_MS}" "$DB_PATH" "$sql" 2>&1); then
    send_telegram "⚠️ SATURN report sqlite3 query failed: ${output:0:180}"
    echo "$fallback"
    return 1
  fi
  if [ -z "$output" ]; then
    output="$fallback"
  fi
  echo "$output"
}

require_base_path "$DB_PATH" "sqlite-db-write"
DONE=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="done" AND date(updated_at)=date("now")' 0)
OPEN=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="pending"' 0)
LEADS_TODAY=$(sqlite_query 'SELECT COUNT(*) FROM leads WHERE date(created_at)=date("now")' 0)
REVENUE=$(sqlite_query 'SELECT COALESCE(SUM(amount),0) FROM revenue WHERE status="paid"' 0)
MONTH_REV=$(sqlite_query 'SELECT COALESCE(SUM(amount),0) FROM revenue WHERE status="paid" AND strftime("%Y-%m",paid_date)=strftime("%Y-%m","now")' 0)

MSG="🌙 *SATURN — Daily Report*

✅ Tasks completed today: ${DONE}
📋 Still open: ${OPEN}
🎯 New leads today: ${LEADS_TODAY}
💰 Total revenue: \$${REVENUE}
📈 This month: \$${MONTH_REV}

System stable. Agents resting.
Tomorrow planning starts at 8am. 🛰️"

send_telegram "$MSG"
