---
name: saturn-cost
version: 2
---

# SATURN COST

## Enforcement Points

Before every LLM call:
- Query token_usage_log for today's IST agent total.
- If SUM(tokens_used) >= 50000: return {"status": "quota_blocked", "service": "llm", "agent": "<n>"}
- Log the block to error_log: error_type="QUOTA_EXCEEDED", agent=<caller>, detail="llm daily limit"
- Log the block to agent_log: action="quota_blocked", detail="service=llm"

Before every external API call:
- Query api_usage_log for paused=1 on this service for today.
- If paused: return {"status": "quota_blocked", "service": "<n>"}
- Log the block to error_log: error_type="QUOTA_EXCEEDED", detail=<service>

After every successful LLM call:
- Write agent, action, tokens_used, log_date (IST) to token_usage_log.

After every successful external API call:
- Increment call_count in api_usage_log.

## Default Daily Thresholds
| Service     | Daily Limit              |
|-------------|--------------------------|
| LLM (any)   | 50,000 tokens per agent  |
| SerpAPI     | 100 calls                |
| Hunter.io   | 50 calls                 |
| Email send  | 30 sends                 |
| LinkedIn    | 20 searches              |

## Cost Reduction Rules
1. Reuse existing SQLite state before calling any paid API.
2. Never retry a quota error. Quota errors are terminal for the current cycle.
3. Max 2 retries on non-quota LLM errors. Stop after second failure.
4. Use minimum model tier that can complete the task.
5. Prompts must not include data not needed for the specific output.
