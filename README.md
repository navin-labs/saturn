# Saturn

![Status](https://img.shields.io/badge/status-verified-green)
![Architecture](https://img.shields.io/badge/architecture-local--first-blue)
![Control](https://img.shields.io/badge/control-human--approved-orange)

Saturn is a local-first AI automation system for outbound lead workflows with strict human approval control and full database-level auditability.

> Verified in April 2026 with SMTP delivery, approval API execution, inbound reply processing, and direct SQLite validation.

![Python](https://img.shields.io/badge/python-3.10+-blue) ![SQLite](https://img.shields.io/badge/database-SQLite-green) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Proof

| Claim | Evidence |
|---|---|
| SMTP delivery verified | Sent message confirmed in Gmail inbox. ([view](docs/proof/inbox.png)) |
| Approval API verified | Approval returned `{"status":"ok","message":"Draft 59 approved and sent"}`. ([view](docs/proof/api_approval.json)) |
| SQLite state verified | Draft creation, send log, and reply state transitions visible in SQLite after execution. ([view](docs/proof/db_tables.png)) |
| Reply loop verified | Inbound reply logged as `reply_received` and lead status updated to `qualified`. ([view](docs/proof/reply.png)) |
| Repo layout snapshot | Current repo layout referenced by this README. ([view](docs/proof/repo_tree.txt)) |

All claims above are backed by real execution artifacts. No simulated data or mocked outputs are used.

Artifacts in `docs/proof/`:

- [`inbox.png`](docs/proof/inbox.png)
- [`db_tables.png`](docs/proof/db_tables.png)
- [`api_approval.json`](docs/proof/api_approval.json)
- [`reply.png`](docs/proof/reply.png)
- [`repo_tree.txt`](docs/proof/repo_tree.txt)

---

## What Saturn Does

- Hunter collects leads from search providers and writes them to SQLite.
- Echo generates outreach drafts through the shared LLM queue and stores them in `outreach_drafts`.
- The approval API is the send gate. Unapproved drafts are rejected at send time.
- Approved drafts are sent over SMTP by default. A Gmail API send path is used only when it is explicitly configured.
- The email reader pulls unread Gmail replies over IMAP, matches by normalized sender email, logs `reply_received`, and updates lead status.
- Sentinel, Pulse, and Forge provide health checks, reporting, and workflow deployment without owning the core outreach state machine.

---

## Verified Flow

```text
Hunter   -> search providers -> INSERT leads
Echo     -> Gemini queue -> INSERT outreach_drafts (pending)
Operator -> approval API -> UPDATE outreach_drafts (approved)
Echo     -> SMTP send -> UPDATE outreach_drafts (sent) + INSERT email_send_log
Reader   -> Gmail IMAP -> INSERT reply_received + UPDATE leads
Sentinel -> health checks -> INSERT system_alerts
Pulse    -> SQLite reports
Forge    -> n8n workflow build and deploy on demand
```

LLM calls route through [`backend/modules/llm_queue.py`](backend/modules/llm_queue.py), using `models/gemini-2.5-flash` first, then `models/gemini-2.0-flash` and `models/gemini-2.0-flash-lite` if a non-rate-limit provider failure occurs.

---

## Repository Layout

Tracked source layout:

```text
.
├── .gitignore
├── README.md
├── .saturn/
│   ├── LOCK.md
│   ├── SATURN.md
│   ├── agents/
│   ├── commands/
│   └── hooks/
├── backend/
│   ├── forge.py
│   ├── path_guard.py
│   ├── saturn-api.py
│   ├── saturn_api.py
│   ├── voice_alert.py
│   ├── modules/
│   └── tools/
├── configs/
│   ├── base_path.env
│   ├── paths.py
│   ├── saturn-server.py
│   ├── saturn.crontab
│   ├── saturn_server.py
│   └── workflows/
├── docs/
│   ├── SATURN_CANONICAL_SPEC.md
│   └── proof/
├── scripts/
│   ├── lib/
│   ├── saturn-db-migrate.py
│   ├── saturn-email-reader.sh
│   ├── saturn-hourly.sh
│   ├── saturn-init.sh
│   ├── saturn-morning.sh
│   ├── saturn-progress-review.sh
│   └── saturn-report.sh
└── skills/
    ├── ai/
    ├── coordination/
    ├── core/
    ├── cost/
    ├── n8n/
    ├── notion/
    ├── outreach/
    └── web/
```

Runtime directories such as `database/` and `logs/` are intentionally not tracked. The full repo snapshot used for this section lives in [`docs/proof/repo_tree.txt`](docs/proof/repo_tree.txt).

---

## Running Saturn

Requirements:

- Python 3.10+
- SQLite3
- Gemini API access for draft generation
- SMTP credentials for outbound mail
- Gmail IMAP credentials for reply ingestion

Core runtime variables are read from the shell or service manager. The outreach flow depends on:

- `GOOGLE_API_KEY`
- `SMTP_HOST`
- `SMTP_USER`
- `SMTP_PASS`
- `GMAIL_USER`
- `GMAIL_APP_PASSWORD`

Optional runtime variables:

- `SATURN_BASE_PATH` if the repo is not located at `~/Workspace/Saturn`
- `SMTP_FROM` to override the From address
- `SATURN_TELEGRAM_ENV` for Telegram alerts
- `NOTION_API_KEY` for Notion sync

The shell scripts source [`configs/base_path.env`](configs/base_path.env) for workspace path resolution.

Start the local API:

```bash
python3 scripts/saturn-db-migrate.py
python3 backend/saturn-api.py
```

Useful endpoints:

- `POST /api/tools/call` with `{"tool":"get_cost_status","args":{}}`
- `POST /api/tools/call` with `{"tool":"trigger_echo_for_pending_leads","args":{"dry_run":true}}`
- `GET /api/saturn/approvals`
- `POST /api/saturn/approvals/{id}/approve`
- `POST /api/saturn/approvals/{id}/reject`
- `GET /api/saturn/health/full`

---

## Validation Commands

```bash
sqlite3 database/saturn.db "
SELECT id, lead_id, draft_id, status, sent_at
FROM email_send_log
ORDER BY id DESC
LIMIT 5;
"

sqlite3 database/saturn.db "
SELECT id, status, updated_at
FROM leads
ORDER BY id DESC
LIMIT 5;
"

sqlite3 database/saturn.db "
SELECT agent, action, tokens_used, log_date
FROM token_usage_log
ORDER BY id DESC
LIMIT 10;
"
```

---

## Operational Guarantees

**Approval gate**

`send_outreach_email()` reads the stored draft row and rejects any draft whose state is not `approved` or `sent`. The response is a structured JSON error when approval is missing.

**Rate-limit handling**

Gemini rate limits return a structured `rate_limited` response. Echo treats that as an LLM failure, records the error, and inserts a deterministic fallback draft instead of aborting the draft cycle. Telegram and API callers receive a safe rate-limit message rather than an empty response.

**SMTP safety**

Missing SMTP settings return `{"status":"error","type":"SMTP_NOT_CONFIGURED"}` before any send attempt. Send failures log `SMTP_FAILURE` and revert the draft from `approved` back to `pending`.

**Reply processing**

The reader normalizes the sender address, matches it against `leads.email`, writes `reply_received` into `email_send_log`, and updates the lead to `qualified` only when the reply is classified as interested and the lead is already `contacted`. Negative replies can move a `contacted` lead to `lost`.

**Fail-open Notion**

Notion sync errors are logged and do not block the local SQLite pipeline.

---

## Security And Repo Hygiene

- `.gitignore` excludes `.env` files, secret files, `database/`, `logs/`, and SQLite runtime artifacts.
- No credentials or API key values are documented in this README.
- The tracked repo description excludes runtime database and log files.
- [`backend/path_guard.py`](backend/path_guard.py) constrains write paths to the Saturn workspace.

---

## Known Constraints

- [`configs/saturn-server.py`](configs/saturn-server.py) remains a large monolithic tool surface.
- Reply matching is exact on normalized sender email. Alias and forward-chain handling is intentionally conservative.
- Saturn ships a local API and operator workflow, not a web frontend.

---

## License

MIT License

Copyright (c) 2026 Navin
