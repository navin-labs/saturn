#!/bin/bash
# SATURN Progress Review — runs at 18:00 IST via cron

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
    send_telegram "⚠️ SATURN progress sqlite3 query failed: ${output:0:180}"
    echo "$fallback"
    return 1
  fi
  if [ -z "$output" ]; then
    output="$fallback"
  fi
  echo "$output"
}

sqlite_exec() {
  local sql="$1"
  local output
  if ! output=$(sqlite3 -cmd ".timeout ${SQLITE_TIMEOUT_MS}" "$DB_PATH" "$sql" 2>&1); then
    send_telegram "⚠️ SATURN progress sqlite3 exec failed: ${output:0:180}"
    return 1
  fi
}

require_base_path "$DB_PATH" "sqlite-db-write"

# Gather metrics for today
DONE_TODAY=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="done" AND date(updated_at)=date("now")' 0)
LEADS_TODAY=$(sqlite_query 'SELECT COUNT(*) FROM leads WHERE date(created_at)=date("now")' 0)
CONTACTED_TODAY=$(sqlite_query 'SELECT COUNT(*) FROM leads WHERE status="contacted" AND date(last_contact)=date("now")' 0)
REVENUE_TODAY=$(sqlite_query 'SELECT COALESCE(SUM(amount),0) FROM revenue WHERE date(paid_date)=date("now")' 0)
OPEN_TASKS=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="pending"' 0)

MSG="📊 *SATURN — 18:00 Progress Review*

*Activity Today:*
✅ Tasks completed: ${DONE_TODAY}
🎯 New leads added: ${LEADS_TODAY}
📞 Leads contacted: ${CONTACTED_TODAY}
💰 Revenue logged: \$${REVENUE_TODAY}

*Current Status:*
📋 Open tasks remaining: ${OPEN_TASKS}

Evening report will follow at 22:00."

send_telegram "$MSG"

# Log to DB for audit trail
sqlite_exec "INSERT INTO agent_log (agent, action, detail) VALUES ('Pulse', 'progress_review', 'Sent 18:00 review')" || true
