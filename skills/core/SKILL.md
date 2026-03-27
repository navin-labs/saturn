---
name: saturn-core
version: 2
---

# SATURN CORE

## Execution Steps (ordered, mandatory)
1. Read current state from SQLite before any other action.
2. Validate all tool inputs before dispatch. Return blocked if invalid.
3. Call registered tool via ToolRegistry only. Never call provider or external API directly.
4. Write results to SQLite immediately after tool returns.
5. Sync external systems (Notion, alerts) only after local write succeeds.
6. Return structured JSON at every exit point — success and failure both.

## Exit Contracts
- Success:  {"status": "ok", "output": {...}}
- Error:    {"status": "error", "error": "<type>", "detail": "<msg>", "agent": "<n>"}
- Blocked:  {"status": "blocked", "reason": "<condition>"}
- Quota:    {"status": "quota_blocked", "service": "<n>", "agent": "<n>"}
- Fallback: {"status": "fallback", "output": {...}, "reason": "<why>"}

## Hard Rules
- No agent executes work outside the tool registry.
- No silent failure. Every error path returns a structured dict.
- No state held in memory across cycles. All cycle state lives in SQLite.
- If the same logic exists in two files, one must be deleted.
- If local state is ambiguous, return blocked. Never guess.
- Log every action to agent_log. Log every error to error_log.
