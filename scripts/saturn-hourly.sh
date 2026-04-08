#!/bin/bash
# Runs every hour. Checks for stuck tasks, alerts if needed.

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
    -d message_thread_id="${THREAD_ALERTS}" \
    -d parse_mode="Markdown" \
    -d text="$1" > /dev/null
}

sqlite_query() {
  local sql="$1"
  local fallback="${2:-0}"
  local output
  if ! output=$(sqlite3 -cmd ".timeout ${SQLITE_TIMEOUT_MS}" "$DB_PATH" "$sql" 2>&1); then
    send_telegram "⚠️ SATURN hourly sqlite3 query failed: ${output:0:180}"
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
    send_telegram "⚠️ SATURN hourly sqlite3 exec failed: ${output:0:180}"
    return 1
  fi
}

sqlite_exec_allow_duplicate() {
  local sql="$1"
  local output
  if ! output=$(sqlite3 -cmd ".timeout ${SQLITE_TIMEOUT_MS}" "$DB_PATH" "$sql" 2>&1); then
    if [[ "$output" == *"duplicate column name"* ]]; then
      return 0
    fi
    send_telegram "⚠️ SATURN hourly sqlite3 schema update failed: ${output:0:180}"
    return 1
  fi
}

require_base_path "$DB_PATH" "sqlite-db-write"

# Ensure quota-monitor columns exist (safe no-op if already present)
sqlite_exec_allow_duplicate "ALTER TABLE api_usage_log ADD COLUMN service TEXT;"
sqlite_exec_allow_duplicate "ALTER TABLE api_usage_log ADD COLUMN usage_date TEXT;"
sqlite_exec_allow_duplicate "ALTER TABLE api_usage_log ADD COLUMN call_count INTEGER DEFAULT 0;"
sqlite_exec_allow_duplicate "ALTER TABLE api_usage_log ADD COLUMN quota_limit INTEGER DEFAULT 0;"
sqlite_exec_allow_duplicate "ALTER TABLE api_usage_log ADD COLUMN paused INTEGER DEFAULT 0;"

HUNTER_QUOTA=${SATURN_HUNTER_API_DAILY_LIMIT:-10}
SMTP_QUOTA=${SATURN_SMTP_DAILY_LIMIT:-10}
GEMINI_QUOTA=${SATURN_GEMINI_API_DAILY_LIMIT:-1000}

# New day auto-resume: clear pause flags, keep call_count for audit.
sqlite_exec \
  "UPDATE api_usage_log SET paused=0 WHERE COALESCE(paused,0)=1 AND usage_date IS NOT NULL AND usage_date < date('now');"

check_service_quota() {
  local service="$1"
  local quota="$2"
  local details
  local call_count
  local quota_limit
  local paused
  local pct

  sqlite_exec \
    "INSERT OR IGNORE INTO api_usage_log (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_count, quota_limit, paused) \
     VALUES ('system','$service','daily_counter','success','','',CURRENT_TIMESTAMP,'$service',date('now'),0,$quota,0);"
  sqlite_exec \
    "UPDATE api_usage_log SET quota_limit=$quota WHERE service='$service' AND usage_date=date('now') AND endpoint='daily_counter' AND COALESCE(quota_limit,0)<=0;"

  details=$(sqlite_query \
    "SELECT COALESCE(call_count,0) || '|' || COALESCE(quota_limit,$quota) || '|' || COALESCE(paused,0) \
     FROM api_usage_log WHERE service='$service' AND usage_date=date('now') AND endpoint='daily_counter' ORDER BY id DESC LIMIT 1;" \
    "0|$quota|0")
  call_count="${details%%|*}"
  details="${details#*|}"
  quota_limit="${details%%|*}"
  paused="${details##*|}"
  if [ -z "$call_count" ]; then call_count=0; fi
  if [ -z "$quota_limit" ]; then quota_limit="$quota"; fi
  if [ -z "$paused" ]; then paused=0; fi

  if [ "$quota_limit" -le 0 ]; then
    return
  fi
  pct=$(( (call_count * 100) / quota_limit ))
  if [ "$pct" -ge 80 ] && [ "$paused" -eq 0 ]; then
    sqlite_exec \
      "UPDATE api_usage_log SET paused=1 WHERE service='$service' AND usage_date=date('now') AND endpoint='daily_counter';"
    send_telegram "⚠️ ${service} quota at ${pct}%. Pausing until tomorrow."
  fi
}

check_service_quota "hunter" "$HUNTER_QUOTA"
check_service_quota "smtp" "$SMTP_QUOTA"
check_service_quota "gemini" "$GEMINI_QUOTA"

# Check n8n
N8N=$(curl -s http://localhost:5678/healthz 2>/dev/null)
if [ -z "$N8N" ]; then
  send_telegram "⚠️ *Sentinel Alert*: n8n is offline. Restarting..."
  systemctl --user restart n8n
fi

# Check high-priority tasks
HIGH=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE priority="high" AND status="pending"' 0)

if [ "$HIGH" -gt 2 ]; then
  TASK_LIST=$(sqlite_query 'SELECT title FROM tasks WHERE priority="high" AND status="pending" LIMIT 3' "")
  send_telegram "🔴 *Pulse:* ${HIGH} high-priority tasks need attention:\n${TASK_LIST}"
fi

# Log hourly check
DONE=$(sqlite_query 'SELECT COUNT(*) FROM tasks WHERE status="done" AND date(updated_at)=date("now")' 0)
sqlite_exec "INSERT INTO hourly_checks (summary, open_tasks, completed_today) VALUES ('hourly', $HIGH, $DONE)"
