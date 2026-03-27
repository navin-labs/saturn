# SATURN CONTROL CONTRACT

## Authority
This file defines Saturn execution behavior.
If this file conflicts with any other markdown, this file wins for execution behavior.
`docs/SATURN_CANONICAL_SPEC.md` explains implementation behavior and runtime internals.

## Objective
Run Saturn as a strict local-first operations system with one control layer, one tool layer, one database truth, and one governed LLM path.

## Execution Order
1. Read current state from SQLite or validated tool output.
2. Route the task to the single owning agent.
3. Execute through the centralized tool layer.
4. Use the governed LLM path only when the selected tool requires reasoning or writing.
5. Write deterministic results back to SQLite first.
6. Sync outward systems fail-open after local state is safe.

## Non-Negotiable Rules
- Tool-first.
- DB-first where local state exists.
- No direct agent-to-LLM bypass.
- No direct agent-to-external-API bypass.
- No duplicate control files or competing execution rules.
- No Notion sync through LLM.
- No agent may own another agent's responsibility.
- No UI runtime in the repo.

## Agent Ownership
- Saturn: orchestration only.
- Forge: n8n workflow build, validation, and deployment only.
- Echo: outreach writing only.
- Hunter: lead discovery, scoring, and structured saving only.
- Pulse: reporting and analysis only.
- Sentinel: monitoring and safe recovery only.

## Shared Runtime Contract
- Tool registry is the only execution gateway for agent work.
- SQLite is the system source of truth.
- `llm_queue` is the only approved LLM path.
- Notion is a direct API integration and visualization layer only.
- External sync must never block core local execution.

## Skills, Commands, And Hooks
- Skills define reusable operating rules, not runtime ownership.
- Commands define safe operator actions, not new control logic.
- Hooks enforce hygiene only and must not change runtime behavior.

## Safe Failure Behavior
- Fail open when external integrations break.
- Return structured error output instead of silent drift.
- Stop the current cycle when ownership, state, or tool routing is unclear.
- Never escalate to destructive action automatically.
