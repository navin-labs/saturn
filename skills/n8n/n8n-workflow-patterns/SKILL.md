---
name: n8n-workflow-patterns
description: Minimal workflow pattern guide for Forge.
---

# n8n Workflow Patterns

- Use one clear trigger, one transformation path, and one delivery path.
- Prefer deterministic API and database steps before optional AI nodes.
- Add explicit error handling and validate workflows before deploy.
- Keep workflow JSON simple, typed, and production-safe.
