#!/usr/bin/env python3
# This script forwards voice alerts through the local Saturn API.
import sys
import os
import datetime
import json
import urllib.error
import urllib.request

# Get the message from command-line arguments
message = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Saturn alert."

# Define log file path
LOG_FILE = os.path.expanduser("/home/navin/Workspace/Saturn/logs/voice_alert_debug.log")
API_URL = os.environ.get("SATURN_API_URL", "http://127.0.0.1:8787/api/tools/call")

def log_message(text):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.datetime.now()}: {text}\n")

log_message(f"Attempting to send voice alert: {message[:50]}")

payload = json.dumps({"tool": "voice_alert", "args": {"message": message}}).encode("utf-8")
request = urllib.request.Request(
    API_URL,
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    result = parsed.get("result_json")
    if result is None and isinstance(parsed.get("result"), str):
        try:
            result = json.loads(parsed["result"])
        except Exception as exc:
            log_message(f"JSON parse failure for nested result: {exc}")
            result = {"status": "unknown", "raw": parsed.get("result")}
    status = (result or {}).get("status", "unknown")
    if status == "success":
        log_message(f"Success. response: {raw[:500]}")
        print("Voice sent to Telegram.")
    else:
        log_message(f"Voice alert returned non-success response: {raw[:500]}")
        print(raw)
except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", errors="replace")
    log_message(f"HTTP failure. code={exc.code} body={body[:500]}")
    print(body)
except Exception as exc:
    log_message(f"Failure calling local Saturn API: {exc}")
    print(f"Error calling local Saturn API: {exc}")
