THIS FILE IS THE CONTROL-LAYER SOURCE OF TRUTH.
Runtime behavior still defers first to `docs/SATURN_CANONICAL_SPEC.md`.

# Saturn Control Layer

## System Purpose
Saturn is API + agents + database + control layer.
It runs as a local-first production operations engine for leads, outreach, workflows, reports, and monitoring.

## Strict Architecture
- Runtime lives in `backend/`, `configs/`, `database/`, `scripts/`, and `skills/`.
- Control instructions live in `.saturn/`.
- OpenClaw-style isolated agents are required.
- Each agent has one strict role and one instruction file.
- Tools are centralized and are the only execution path.
- No UI exists in the runtime repo.

## LLM Rules
- LLM is for reasoning and output only.
- All LLM calls go through the shared queue.
- No direct Gemini calls outside the central queue path.

## Notion Rules
- Notion sync is direct API only. No LLM tokens are consumed here.
- Never route Notion through `call_llm()`.
- Notion writes must stay fail-open and token-free.

## Database-First Strategy
- Prefer deterministic SQLite reads and writes before LLM work.
- State guards, alerts, logs, and counters stay local and direct.

## Control Override
- This file overrides conflicting control markdown.
- Any remaining markdown must align with this file and the canonical spec.
