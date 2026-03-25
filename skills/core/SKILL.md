---
name: saturn-core
description: Shared Saturn execution rules for local tools, database-first work, structured outputs, and control-layer alignment.
---

# Saturn Core

- Use tools first.
- Prefer deterministic database and API work before LLM calls.
- Keep outputs structured at the API boundary.
- Never read secrets or env files directly.
- Defer behavior to `docs/SATURN_CANONICAL_SPEC.md` and `.saturn/SATURN.md`.
