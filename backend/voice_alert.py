#!/usr/bin/env python3
# This script now acts as a wrapper for the internal MCP TTS tool.
import sys
import subprocess
import os
import datetime

# Get the message from command-line arguments
message = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Saturn alert."

# Get the path to the saturn script
saturn_script_path = os.path.expanduser("/home/navin/Workspace/Saturn/scripts/saturn")

# Define log file path
LOG_FILE = os.path.expanduser("/home/navin/Workspace/Saturn/logs/voice_alert_debug.log")

def log_message(text):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.datetime.now()}: {text}\n")

log_message(f"Attempting to send voice alert: {message[:50]}")

# Call the internal Saturn MCP tts tool
# Note: The MCP tool will handle sending the voice to Telegram
result = subprocess.run(
    [saturn_script_path, "saturn-mcp.voice_alert", f"message='{message}'"],
    capture_output=True, text=True
)

if result.returncode == 0:
    log_message(f"Success. stdout: {result.stdout.strip()}")
    print("Voice sent to Telegram.") # Keep this for immediate feedback
else:
    log_message(f"Failure. returncode: {result.returncode}, stdout: {result.stdout.strip()}, stderr: {result.stderr.strip()}")
    print("Error calling internal TTS tool:")
    print(result.stderr)
