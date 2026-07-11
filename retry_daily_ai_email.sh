#!/bin/bash
set -u

ROOT="/Users/jiubao/Desktop/codex_workplace/AI_daily_report"
LOG_DIR="$ROOT/logs"
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  if [ -x "/usr/local/bin/python3" ]; then
    PYTHON_BIN="/usr/local/bin/python3"
  elif [ -x "/opt/homebrew/bin/python3" ]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="/usr/bin/python3"
  fi
fi
mkdir -p "$LOG_DIR"

cd "$ROOT" || exit 1

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] start pending email retry"
"$PYTHON_BIN" daily_ai_terminal_sender.py --retry-pending --queue-dir daily_ai_terminal_unsent
terminal_status=$?
"$PYTHON_BIN" daily_ai_terminal_sender.py --retry-pending --queue-dir daily_ai_unsent
brief_status=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] pending email retry exit terminal=$terminal_status brief=$brief_status"

if [ "$terminal_status" -ne 0 ] || [ "$brief_status" -ne 0 ]; then
  exit 1
fi
exit 0
