---
name: saturn-web
version: 2
---

# SATURN WEB

## Search Protocol
1. Construct narrowest query returning useful leads. Target 3-6 words.
2. Check api_usage_log for service quota status before calling. If paused=1, return quota_blocked immediately.
3. Parse response into: name, company, website, email, source.
4. Normalize website to root domain (strip subdomains, trailing slashes, protocol).
5. Check leads.website_norm for match before insert. Skip if duplicate.
6. Skip records missing both email and website. Do not insert partial records.
7. Increment call counter in api_usage_log after successful response.

## Email Confidence
- Hunter.io direct match → email_status: verified
- Pattern inference or scrape → email_status: guessed
- No email found → email_status: unknown
- Only verified and guessed records advance to outreach.

## Error Taxonomy (handle every case explicitly)
- 429 / quota response: set paused=1 in api_usage_log, return {"status": "quota_blocked", "service": "<n>"}
- 5xx / timeout: log to error_log with error_type="service_unavailable", return {"status": "error", "error": "service_unavailable"}
- Parse failure: log to error_log with error_type="parse_error", skip record, continue batch
- 401 / 403 auth error: log CRITICAL to error_log with error_type="auth_error", halt batch immediately, return {"status": "error", "error": "auth_error"}
- All quota blocks must write to error_log: error_type="QUOTA_EXCEEDED", agent=<caller>
