#!/bin/bash
set -u

ROOT="/Users/jiubao/Desktop/codex_workplace/AI_daily_report"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

cd "$ROOT" || exit 1

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] start daily AI brief"
/usr/bin/python3 daily_ai_brief_runner.py
status=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] daily AI brief exit=$status"
exit "$status"
