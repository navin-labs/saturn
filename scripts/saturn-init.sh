#!/bin/bash
#
# SATURN System Initialization & Verification Script
# Version 1.0
#
# This script performs a full health check of the SATURN system.
# It does NOT make any changes to the system automatically.
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/../configs/base_path.env"

# --- Colors for output ---
C_RED='\033[0;31m'
C_GREEN='\033[0;32m'
C_YELLOW='\033[1;33m'
C_BLUE='\033[0;34m'
C_NC='\033[0m' # No Color

# --- Helper Functions ---
print_header() {
    echo -e "\n${C_BLUE}# --- $1 ---${C_NC}"
}

print_status() {
    if [ "$2" == "OK" ]; then
        echo -e "${C_GREEN}[ OK ]${C_NC} $1"
    elif [ "$2" == "WARN" ]; then
        echo -e "${C_YELLOW}[ WARN ]${C_NC} $1"
    else
        echo -e "${C_RED}[ FAIL ]${C_NC} $1"
    fi
}

# --- Begin Checks ---
echo -e "${C_BLUE}### SATURN SYSTEM VERIFICATION ###${C_NC}"

# 1. Workspace Permissions
print_header "1. Verifying Workspace Permissions"
WORKSPACE_DIR="/home/openclaw-user/workspace"
if [ -d "$WORKSPACE_DIR" ]; then
    OWNER=$(stat -c '%U' "$WORKSPACE_DIR")
    if [ "$OWNER" == "openclaw-user" ]; then
        print_status "Workspace owner is correct ('openclaw-user')." "OK"
    else
        print_status "Workspace owner is INCORRECT (should be 'openclaw-user', but is '$OWNER'). Manual intervention required." "FAIL"
        echo -e "  > To fix, run: ${C_YELLOW}sudo chown -R openclaw-user:openclaw-user $WORKSPACE_DIR${C_NC}"
    fi
else
    print_status "Workspace directory does not exist at $WORKSPACE_DIR" "FAIL"
fi

# 2. saturn-sync Command
print_header "2. Verifying 'saturn-sync' Command"
if command -v saturn-sync &> /dev/null; then
    print_status "'saturn-sync' command is available in PATH." "OK"
else
    print_status "'saturn-sync' command not found in PATH. Sync operations will fail." "FAIL"
fi

# 3. Database Schema
print_header "3. Verifying Database"
if [ -f "$DB_PATH" ]; then
    print_status "Database file found at $DB_PATH" "OK"
    # Check for a key table from the migration
    TABLE_CHECK=$(sqlite3 "$DB_PATH" "SELECT name FROM sqlite_master WHERE type='table' AND name='leads';")
    if [ -n "$TABLE_CHECK" ]; then
        print_status "Core table 'leads' found. Schema appears to be migrated." "OK"
    else
        print_status "Core table 'leads' NOT found. Database migration may be required." "WARN"
        echo -e "  > To fix, run the migration script."
    fi
else
    print_status "Database file not found at $DB_PATH" "FAIL"
fi

# 4. Telegram Secrets
print_header "4. Verifying Telegram Secrets"
SECRETS_FILE="$HOME/.config/openclaw-secrets/telegram.env"
if [ -f "$SECRETS_FILE" ]; then
    print_status "Secrets file telegram.env found." "OK"
    PERMS=$(stat -c '%a' "$SECRETS_FILE")
    if [ "$PERMS" == "600" ]; then
        print_status "File permissions are correct (600)." "OK"
    else
        print_status "File permissions are INCORRECT ($PERMS). Should be 600 for security." "WARN"
        echo -e "  > To fix, run: ${C_YELLOW}chmod 600 $SECRETS_FILE${C_NC}"
    fi
else
    print_status "Secrets file telegram.env not found. Autonomous messages will fail." "FAIL"
fi

# 5. MCP Server
print_header "5. Verifying MCP Server"
if /home/openclaw-user/workspace/saturn saturn-mcp.ping &> /dev/null; then
    print_status "MCP server is online and responding to 'ping'." "OK"
else
    print_status "MCP server did not respond to 'ping'. It may be offline or misconfigured." "FAIL"
fi

# 6. n8n Service
print_header "6. Verifying n8n Service"
if systemctl --user is-active --quiet n8n.service; then
    print_status "n8n.service is active and running." "OK"
else
    print_status "n8n.service is INACTIVE. Lead generation workflow will fail." "FAIL"
    echo -e "  > To fix, run: ${C_YELLOW}systemctl --user start n8n.service${C_NC}"
fi

echo -e "\n${C_BLUE}### VERIFICATION COMPLETE ###${C_NC}"
