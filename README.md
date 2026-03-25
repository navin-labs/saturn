# Saturn

Local-first operations engine for lead handling, outreach drafting, workflow delivery, reporting, monitoring, and strict tool execution.

## Source Of Truth

Read [docs/SATURN_CANONICAL_SPEC.md](/home/navin/Workspace/Saturn/docs/SATURN_CANONICAL_SPEC.md) first. If any other Saturn markdown file disagrees, the canonical spec wins.

## Runtime

- API: `backend/saturn-api.py`
- MCP/tools: `configs/saturn-server.py`
- Database: `database/saturn.db`
- Workflows: `configs/workflows/`
- Control layer: `.saturn/`
- Skills: `skills/n8n/`, `skills/core/`, `skills/ai/`

## Core Rules

- Notion sync is direct API only and must not use the LLM.
- LLM use is reserved for generation, reasoning, and content tasks.
- Gemini rate limits must be handled centrally.
- Agent roles stay isolated and actions route through tools.
- Historical or stale docs must not control behavior.
- No UI is part of the runtime repo.

## Local Start

```bash
cd ~/Workspace/Saturn
source ~/mcp-env/bin/activate
systemctl --user start saturn-api
curl http://localhost:8787/api/saturn/health/full
```
