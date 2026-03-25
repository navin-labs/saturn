# Saturn

Local-first operations and automation system for lead handling, outreach drafting, workflow delivery, reporting, and monitoring.

## Source Of Truth

Read [docs/SATURN_CANONICAL_SPEC.md](/home/navin/Workspace/Saturn/docs/SATURN_CANONICAL_SPEC.md) first. If any other Saturn markdown file disagrees, the canonical spec wins.

## Runtime

- API: `backend/saturn-api.py`
- MCP/tools: `configs/saturn-server.py`
- Database: `database/saturn.db`
- Workflows: `configs/workflows/`

## Core Rules

- Notion sync is direct API only and must not use the LLM.
- LLM use is reserved for generation, reasoning, and content tasks.
- Gemini rate limits must be handled centrally.
- Historical or audit docs must not control behavior.

## Local Start

```bash
cd ~/Workspace/Saturn
source ~/mcp-env/bin/activate
systemctl --user start saturn-api
curl http://localhost:8787/api/saturn/health/full
```
