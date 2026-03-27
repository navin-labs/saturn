---
name: saturn-outreach
version: 2
---

# SATURN OUTREACH

## Pre-Generation Requirements
Read from SQLite and confirm all of the following before calling LLM:
- lead_id resolves to a record in leads
- email_status is verified or guessed (not unknown)
- name and company are non-empty

## Generation Protocol
1. Build prompt: role (1 line) + lead context (name, company, pain_point) + output schema.
2. Output schema: {"subject": "<string>", "body": "<string>", "type": "outreach|followup|reply"}
3. Call call_llm(prompt, agent='echo', action='outreach_draft').
4. Parse response. Validate: subject <= 8 words, body <= 120 words, one CTA only.
5. Insert to outreach_drafts with status='pending'. Return draft_id.
6. Log to agent_log: agent='echo', action='draft_created', detail=f"lead_id={lead_id}", result='ok'

## Quality Rules (strip from all outputs)
- "I hope this finds you well" and equivalents
- Generic social proof
- Multiple CTAs or ambiguous next steps
- Filler adjectives: amazing, incredible, revolutionary, game-changing

## Draft State Machine
pending → approved → sent
pending → rejected
sent → bounced
Never advance a draft without an explicit status update.
Never send a pending draft.

## Fallback Contract
If LLM fails after 2 retries, return:
{"status": "fallback", "subject": "Quick question", "body": "Hi {name}, I came across {company} and wanted to ask a quick question — would a few minutes this week work?"}
Always return a usable subject and body. Never return empty.
