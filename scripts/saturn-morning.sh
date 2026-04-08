#!/bin/bash
# SATURN Morning Plan — runs at 8am IST via cron
# Pulse agent generates the day plan and sends to Telegram

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
    send_telegram "⚠️ SATURN morning sqlite3 query failed: ${output:0:180}"
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
    send_telegram "⚠️ SATURN morning sqlite3 exec failed: ${output:0:180}"
    return 1
  fi
}

DATE=$(date '+%A, %d %B %Y')
require_base_path "$DB_PATH" "sqlite-db-write"
OPEN_TASKS=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="pending"' 0)
HIGH_TASKS=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="pending" AND priority="high"' 0)
LEADS=$(sqlite_query 'SELECT COUNT(*) FROM leads WHERE status IN ("new","contacted")' 0)
REVENUE=$(sqlite_query 'SELECT COALESCE(SUM(amount),0) FROM revenue WHERE status="paid"' 0)

MSG="🌅 *SATURN — Morning Briefing*
📅 ${DATE}

📋 *Tasks:* ${OPEN_TASKS} open | ${HIGH_TASKS} high priority
🎯 *Leads in pipeline:* ${LEADS} active
💰 *Revenue earned:* \$${REVENUE}

*Agents standing by:*
• Forge — ready to build
• Hunter — scanning for leads
• Echo — content queued
• Sentinel — watching system
• Pulse — plan active

Reply /tasks to see today's work."

send_telegram "$MSG"

# Log to DB
sqlite_exec "INSERT INTO agent_log (agent, action, detail) VALUES ('Pulse', 'morning_plan', 'Sent morning briefing')"
