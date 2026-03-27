---
name: saturn-n8n
version: 2
---

# SATURN N8N

## Workflow Requirements (all required before deploy)
- Exactly one trigger node
- Exactly one linear success path
- Exactly one error path routing to Saturn DB log node or explicit stop
- All node names are stable strings with no spaces
- All payload field names are stable across activations

## Build Protocol
1. Define trigger type and schedule expression.
2. Define success path as a linear chain.
3. Add error handling at every node calling an external service.
4. Validate full JSON schema locally before calling n8n API.
5. Deploy via tool registry only. Never make direct HTTP calls to n8n.
6. After deploy, re-read workflow from n8n. Confirm it matches spec before activating.
7. Activate only after re-validation passes.
8. After every state change (built, deployed, activated, failed), write workflow name, status, and IST timestamp to agent_log.

## Idempotency Rule
All scheduler-triggered workflows must produce the same result when run twice on the same data.
Use INSERT OR IGNORE or explicit dedup checks at any write node.

## Failure Contracts
- Invalid spec before deploy: return {"status": "blocked", "reason": "<validation_error>"}. Do not call n8n.
- n8n API error on deploy: return {"status": "error", "step": "deploy", "detail": "..."}. Do not activate.
- Workflow run failure: route to Saturn DB log node. Never silently discard.
