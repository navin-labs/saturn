---
name: saturn-ai
description: Shared LLM execution rules for Saturn agents.
---

# Saturn AI

- Use the LLM only for reasoning, writing, scoring, or summarization.
- Route all LLM calls through the central queue.
- Never use the LLM for Notion sync, logging, alerts, or simple state updates.
- Keep prompts minimal and grounded in local data first.
