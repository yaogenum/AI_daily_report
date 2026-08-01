#!/bin/bash
set -u

ROOT="/Users/jiubao/Desktop/codex_workplace/AI_daily_report"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$LOG_DIR/daily_ai_brief.lock"
RUN_LOG="$LOG_DIR/daily_ai_brief.log"
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

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] start daily AI brief" | tee -a "$RUN_LOG"
"$PYTHON_BIN" daily_ai_brief_runner.py 2>&1 | tee -a "$RUN_LOG"
status=${PIPESTATUS[0]}
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] daily AI brief exit=$status" | tee -a "$RUN_LOG"
exit "$status"
