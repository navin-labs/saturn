---
name: saturn-notion
version: 2
---

# SATURN NOTION

## Integration Rules
1. All Notion operations use direct Notion REST API. No SDK that abstracts error handling.
2. Zero call_llm() calls anywhere in backend/tools/notion_sync.py.
3. Read from SQLite first. Notion receives only validated local records.
4. Every SQLite-to-Notion relationship tracked in _saturn_id_map (saturn_id → notion_page_id).
5. Missing page_id = create. Present page_id = update. Never infer.

## Field Mapping Contract
Each synced entity must have an explicit map: SQLite_column → Notion_property_name → Notion_property_type.
Undeclared fields are not synced. No dynamic field discovery.

## Failure Behavior
- Notion API error: log to error_log, return {"notion_status": "failed", "reason": "..."} alongside local success.
- Network timeout: same as API error. Non-fatal.
- Local execution never blocks waiting for Notion. Notion sync is always the last step.

## Forbidden
- No Notion read used to overwrite SQLite state.
- No schema changes in Notion not reflected in SQLite first.
- No Notion call inside any LLM, reporting, or alert path.
