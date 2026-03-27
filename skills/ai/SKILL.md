---
name: saturn-ai
version: 2
---

# SATURN AI

## Permitted LLM Uses
- Lead scoring when no deterministic score is computable
- Outreach and follow-up email generation (echo only)
- Report summary prose after metrics are computed without LLM (pulse only)
- Reply classification (email_reader only)

## Forbidden LLM Uses
- Notion sync operations
- SQLite reads, writes, or queries
- Alert dispatch or formatting
- Status transition logic
- Routing or scheduling decisions
- Any function in backend/tools/notion_sync.py

## Call Protocol
1. Confirm no deterministic path resolves the task.
2. Check agent daily token total in token_usage_log. If >= 50000, return quota_blocked immediately.
3. Read required context from SQLite. Include only what the task needs.
4. Build prompt: role (1-2 lines) + local context + output schema. No padding phrases.
5. Call call_llm(prompt, agent=<agent>, action=<action>). No other entrypoint.
6. Parse response. Validate against output schema before using result.
7. Write token count to token_usage_log: agent, action, tokens_used, log_date (IST).
8. On parse failure: return fallback. Do not retry with a broken prompt.

## Prompt Constraints
- Every prompt defines a JSON output schema.
- Max retries: 2. On second failure, return fallback and stop.
- Never pass raw LLM output to an external API.
- Prompts must not include data not needed for the specific output.
