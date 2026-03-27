# SATURN CANONICAL SPEC

## Role
This document defines how Saturn works internally.
If it conflicts with `.saturn/SATURN.md`, the control contract wins for execution behavior.

## Runtime Map
- API surface: `backend/saturn-api.py`
- Tool and MCP surface: `configs/saturn-server.py`
- Tool governance: `backend/modules/tool_registry.py`
- Governed LLM path: `backend/modules/llm_queue.py`
- Direct integrations: `backend/tools/`
- Local state: `database/saturn.db`
- Control layer: `.saturn/`
- Reusable skill layer: `skills/`

## Internal Execution Model
- Agent intent is descriptive only until it reaches the centralized tool layer.
- Tool execution is the only approved runtime action path.
- Local state is read from SQLite first when relevant.
- Deterministic writes happen before optional external sync.
- LLM work is allowed only through the governed queue and only for reasoning, scoring, summarization, or writing tasks that need it.

## Agent To Runtime Boundaries
- Saturn routes and schedules but does not perform specialist logic.
- Forge owns workflow build, validation, and deployment behavior.
- Echo owns outreach generation behavior.
- Hunter owns search, extraction, dedupe, scoring, and structured lead persistence.
- Pulse owns reporting, planning, and analytic summaries.
- Sentinel owns health checks, alerts, and safe recovery actions.

## Tool Governance
- The tool registry is the centralized execution gateway.
- Tool outputs must be structured at the API boundary.
- Empty tool results must degrade safely instead of returning `None`.
- No duplicate control path may bypass the registry for agent work.

## Database Policy
- SQLite is the system source of truth.
- Lead, task, alert, approval, and activity state must persist locally first.
- Duplicate inserts must be prevented with deterministic checks or safe insert guards.
- Daily dedupe and run guards belong in runtime logic, not only in scheduler timing.

## LLM Policy
- `llm_queue` is the only approved LLM entrypoint.
- Centralized rate limiting, backoff, and fallback remain shared.
- Agents must not call provider SDKs directly.
- Notion, logging, alerts, and simple database helpers must not consume LLM tokens.

## Notion Policy
- Notion is direct API only.
- Notion sync must not call the LLM or create token usage records.
- Notion sync is fail-open and must not block local runtime success.
- Notion content should mirror approved local records rather than invent new state.

## Skill Layer Policy
- Skills must stay short, role-tied, and non-overlapping.
- Skills define reusable operating rules, not new runtime architecture.
- Weak or duplicate skill text should be removed instead of expanded.
- Canonical skill themes are `core`, `ai`, `n8n`, `notion`, `web`, `outreach`, `cost`, and `coordination`.

## Command And Hook Policy
- Commands are deterministic operator contracts.
- Hooks enforce hygiene only.
- Hooks must not add competing runtime logic.
- Control docs, commands, and hooks must not contradict one another.

## Repo Safety Policy
- No UI runtime.
- No duplicate control layers.
- No secrets in repo content.
- No schema changes without verified need and explicit migration handling.
- No broad runtime rewrites when a minimal patch preserves a working system.
