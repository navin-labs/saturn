---
name: saturn-coordination
version: 2
---

# SATURN COORDINATION

## Dispatch Contract
Saturn dispatches exactly one task to exactly one agent per cycle.
Required dispatch payload:
{
  "run_id": "<uuid>",
  "agent": "<hunter|echo|forge|pulse|sentinel>",
  "task": "<task_type>",
  "input": {},
  "context_ids": []
}

Required response envelope:
{
  "run_id": "<uuid>",
  "status": "ok | error | blocked | fallback | quota_blocked",
  "output": {},
  "error": null
}

## Ownership Map (exclusive, no overlap)
| Task Type                              | Owner    |
|----------------------------------------|----------|
| Lead search, scoring, dedup, insert    | hunter   |
| Outreach and follow-up draft writing   | echo     |
| n8n workflow build, deploy, manage     | forge    |
| Reports, metrics, cost summaries       | pulse    |
| Health checks, alerts, recovery        | sentinel |
| Cycle scheduling, task routing         | saturn   |

## Pre-Dispatch Checks
1. Read agent_lock. If target agent is locked, skip this cycle.
2. Read agent_runs for active runs from target agent. If running, skip.
3. Check api_usage_log for critical service paused. If paused, skip LLM-dependent tasks.

## Stale Lock Protocol
A lock older than 10 minutes is stale.
Only Sentinel may clear a stale lock via clear_stale_agent_lock tool.
Saturn must not clear locks. Saturn skips the cycle and signals Sentinel.
Sentinel clears the lock, logs the action to agent_log, and writes a warn alert to system_alerts.

## Conflict Resolution
- Ownership ambiguity: return {"status": "blocked", "reason": "ownership_unclear"}. Do not dispatch.
- Stale lock: Saturn returns {"status": "skipped", "reason": "stale_lock_detected"}. Sentinel resolves.
- No agent reads or modifies another agent's active records.
