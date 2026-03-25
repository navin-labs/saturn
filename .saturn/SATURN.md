THIS FILE IS THE ONLY SOURCE OF TRUTH for the `.saturn` control layer.
Runtime behavior still defers first to `docs/SATURN_CANONICAL_SPEC.md`.

# Saturn Control Layer

## Purpose
Saturn is a local-first production operations engine for leads, outreach, reporting, monitoring, and workflow delivery.

## Architecture
- Core runtime lives in `backend/`, `configs/`, `database/`, and `scripts/`.
- SQLite is the local source of operational state.
- n8n and Notion are direct integrations, not alternate control planes.
- UI, stale archives, demos, and duplicate docs are non-core and must stay out.

## LLM Rules
- All LLM calls must go through the shared queue.
- LLM use is allowed only for generation, reasoning, scoring, and content work.
- No direct Gemini calls outside the central queue path.

## Notion Rules
- Never use the LLM for Notion work.
- Use direct API calls only.
- Notion sync must not write token usage rows.

## Database-First Strategy
- Prefer deterministic SQLite reads and writes before LLM work.
- State updates, dedup guards, alerts, and logs must stay local and direct.

## Repo Cleanliness
- Keep only active runtime assets and the canonical spec.
- Remove generated weight, duplicate scripts, UI, and stale docs.
- If a path is uncertain, move it outside the repo instead of keeping clutter inside.
