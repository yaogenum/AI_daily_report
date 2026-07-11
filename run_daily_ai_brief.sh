#!/bin/bash
set -u

ROOT="/Users/jiubao/Desktop/codex_workplace/AI_daily_report"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$LOG_DIR/daily_ai_brief.lock"
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
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] daily AI brief already running, skip"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] start daily AI brief"
"$PYTHON_BIN" daily_ai_brief_runner.py
status=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] daily AI brief exit=$status"
exit "$status"
