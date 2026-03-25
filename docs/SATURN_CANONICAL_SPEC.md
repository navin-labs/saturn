# SATURN Canonical Spec

## Source Of Truth
All Saturn behavior instructions must be taken from this file first.
This file overrides any conflicting markdown file in the repo.
Duplicated, stale, historical, or vendor markdown must not control Saturn behavior.

## 1. What Saturn Is
Saturn is a local-first automation and operations system that runs on a Linux laptop.
It coordinates lead handling, outreach drafting, workflow delivery, reporting, monitoring, and operator visibility through a small set of Python services, SQLite, n8n, and direct API integrations.

## 2. What Saturn Does
Saturn:
- stores operational state in SQLite
- exposes operational tools through the Saturn API and MCP surface
- generates outreach and other content only where reasoning or writing is required
- syncs operational records to Notion through direct API calls
- runs scheduled checks, reports, and planning through local timers and scripts
- keeps a lightweight repo with clear operational boundaries and low token waste

## 3. What Saturn Must Never Do
Saturn must never:
- expose secrets, tokens, passwords, API keys, or private environment values in markdown or source control
- treat stale docs, audit reports, or historical notes as runtime instructions
- route Notion sync through `call_llm()`, agent prompt templates, or any LLM-only workflow
- spend LLM tokens for direct Notion writes, logging, alert writes, or simple database helpers
- bypass centralized rate limiting for Gemini calls
- redesign the architecture, add features, or change schema without a verified bug and explicit need

## 4. Canonical Execution Rules
- Runtime behavior is defined by current code plus this spec.
- Markdown files outside this spec are descriptive only unless they explicitly defer here.
- Saturn uses an OpenClaw-style split between runtime code and the `.saturn/` control layer.
- Agent instructions live in `.saturn/agents/` and must stay role-isolated.
- One agent owns one role. No agent may call another agent's private logic directly.
- Actions must be exposed and executed through centralized tools.
- Tool execution must return structured JSON at the API boundary.
- Local timers and scripts must fail safely, log clearly, and avoid duplicate daily execution.
- Daily report deduplication must be enforced in runtime logic, not only by scheduler timing.
- LLM usage is allowed only for generation, reasoning, summarization, scoring, or content tasks that need it.
- Direct operational writes must remain deterministic where possible.

## 5. Canonical Rate-Limit Policy
- All Gemini traffic must flow through the shared central queue and shared rate-limit state.
- Rate limits must be handled centrally, not by independent timers or isolated retry loops.
- Burst smoothing is required so simultaneous timer wake-ups become a queue instead of a spike.
- Fallback models may be used only from the centralized LLM path.
- Non-LLM integrations manage their own API behavior and must not be forced through the LLM queue.

## 6. Canonical Notion Policy
- Notion sync is direct API only and must not consume LLM tokens.
- Notion sync must not call `call_llm()`.
- Notion sync must not be routed through any agent prompt or content-generation path.
- Notion sync must not create SQLite `token_log` or `token_usage_log` rows.
- `progress_report()`, `sync_lead()`, `sync_revenue()`, `sync_task()`, `sync_draft()`, `log_agent_activity()`, `raise_alert()`, and `notion_update_*()` remain direct integration methods.
- If Notion is unavailable, Saturn must fail open for sync work and preserve core local runtime behavior.

## 7. Canonical GitHub / Repo Policy
- The repo must be Git-ready, lean, and professional.
- Secrets, env files, databases, logs, caches, generated UI artifacts, and workspace state must stay ignored.
- The live repo keeps no UI runtime, no nested repo wrappers, and no duplicate control layers.
- Vendor or third-party docs do not define Saturn behavior.
- Historical audit docs may be archived outside the repo when they are stale, conflicting, or contain sensitive information.
- README stays short and accurate and points to this canonical spec.

## 8. Canonical Cleanup Policy
- Keep only the markdown docs needed for current operation and onboarding.
- Archive stale or uncertain Saturn-specific docs outside the repo rather than deleting them blindly.
- Remove generated weight that is safe to recreate: caches, compiled files, logs, local build output, local dependency trees, and transient workspace state.
- Keep the skill layer minimal and flat under `skills/core/`, `skills/n8n/`, and `skills/ai/`.
- Do not remove runtime Python files, core configs, real workflows in use, database files, or this canonical spec.

## 9. Canonical V1 Lock Policy
- Saturn remains on the current V1 architecture.
- No architecture redesign.
- No new feature surface.
- No schema changes unless a verified bug requires one.
- Runtime changes are allowed only when needed to match this spec, preserve reliability, or prevent duplication and token waste.

## 10. Canonical Module Responsibilities

### Saturn
- Orchestrates shared state, core tools, summaries, and operator-facing control surfaces.
- Owns local operational truth in SQLite and high-level system coordination.
- Owns centralized tool governance and control-layer alignment.

### Hunter
- Finds, qualifies, and stores leads through deterministic ingestion paths.
- Uses the LLM only where profile scoring or content reasoning is explicitly required.

### Echo
- Generates outreach, follow-ups, replies, and similar content.
- Uses the LLM for writing tasks only.
- Never sends without the approved operational path.

### Forge
- Builds and validates automation workflows and supporting delivery assets.
- Must follow current runtime constraints and deployment-safe workflow rules.
- Uses the `skills/n8n/` skill set only.

### Pulse
- Produces plans, reports, summaries, and cadence checks.
- Uses deterministic data reads first and uses the LLM only when a generation task actually requires it.

### Sentinel
- Monitors health, errors, quotas, and recovery signals.
- Writes alerts and health records directly without LLM dependency.

## 11. Remaining Markdown Rules
- README is a short entrypoint and must link here.
- `.saturn/SATURN.md` governs the control layer and must remain aligned with this file.
- Any retained Saturn markdown file must explicitly defer to this file.
- If a Saturn markdown file conflicts with this spec, this spec wins automatically.
