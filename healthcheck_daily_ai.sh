#!/bin/bash
set -u

ROOT="/Users/jiubao/Desktop/codex_workplace/AI_daily_report"
LOG_DIR="$ROOT/logs"
STATE_FILE="$LOG_DIR/daily_ai_healthcheck.state"
HEALTH_LOG="$LOG_DIR/daily_ai_healthcheck.log"
TODAY="$(date '+%Y-%m-%d')"
TODAY_REPORT="$ROOT/report/daily_ai_brief_${TODAY}.md"
BRIEF_ERR="$LOG_DIR/launchd_daily_ai_brief.err.log"
RETRY_ERR="$LOG_DIR/launchd_daily_ai_email_retry.err.log"

mkdir -p "$LOG_DIR"
cd "$ROOT" || exit 1

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*" | tee -a "$HEALTH_LOG"
}

file_size() {
  if [ -f "$1" ]; then
    stat -f '%z' "$1" 2>/dev/null || echo 0
  else
    echo 0
  fi
}

read_state_value() {
  key="$1"
  if [ -f "$STATE_FILE" ]; then
    awk -F= -v k="$key" '$1 == k {print $2}' "$STATE_FILE" | tail -n 1
  fi
}

scan_new_errors() {
  file="$1"
  old_size="$2"
  new_size="$(file_size "$file")"
  if [ "$new_size" -le "$old_size" ]; then
    echo ""
    return
  fi
  dd if="$file" bs=1 skip="$old_size" count=$((new_size - old_size)) 2>/dev/null \
    | grep -E "Operation not permitted|Permission denied|getcwd|No such file|Traceback|ERROR|FAIL" || true
}

send_alert() {
  subject="$1"
  body="$2"
  /usr/bin/python3 - "$subject" "$body" <<'PY'
import re
import smtplib
import ssl
import subprocess
import sys
from email.message import EmailMessage
from pathlib import Path

root = Path("/Users/jiubao/Desktop/codex_workplace/AI_daily_report")
config = {}
for line in (root / "report.config").read_text(encoding="utf-8").splitlines():
    text = line.strip()
    if not text or text.startswith("#") or text.startswith(";") or "=" not in text:
        continue
    k, v = text.split("=", 1)
    config[k.strip()] = v.strip().strip("'").strip('"')

to = [x.strip() for x in config.get("DAILY_AI_BRIEF_EMAIL_TO", "").replace(";", ",").split(",") if x.strip()]
if not to:
    sys.exit(0)
msg = EmailMessage()
msg["Subject"] = sys.argv[1]
msg["From"] = config.get("DAILY_AI_BRIEF_FROM") or config.get("DAILY_AI_BRIEF_SMTP_USER") or to[0]
msg["To"] = ", ".join(to)
msg.set_content(sys.argv[2])

user = config.get("DAILY_AI_BRIEF_SMTP_USER", "")
password = re.sub(r"\s+", "", config.get("DAILY_AI_BRIEF_SMTP_PASSWORD", ""))
host = config.get("DAILY_AI_BRIEF_SMTP_HOST", "smtp.gmail.com")
port = int(config.get("DAILY_AI_BRIEF_SMTP_PORT", "587") or "587")
try:
    if user and password:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            if config.get("DAILY_AI_BRIEF_SMTP_STARTTLS", "1").lower() in {"1", "true", "yes", "on"}:
                smtp.starttls(context=ssl.create_default_context())
            smtp.login(user, password)
            smtp.send_message(msg)
        sys.exit(0)
except Exception:
    pass

sendmail = Path(config.get("DAILY_AI_BRIEF_SENDMAIL_PATH", "/usr/sbin/sendmail"))
if sendmail.exists():
    proc = subprocess.run([str(sendmail), "-t"], input=msg.as_bytes(), capture_output=True)
    sys.exit(proc.returncode)
sys.exit(1)
PY
}

old_brief_size="$(read_state_value BRIEF_ERR_SIZE)"
old_retry_size="$(read_state_value RETRY_ERR_SIZE)"
old_brief_size="${old_brief_size:-$(file_size "$BRIEF_ERR")}"
old_retry_size="${old_retry_size:-$(file_size "$RETRY_ERR")}"

log "healthcheck start date=$TODAY"
new_errors="$(scan_new_errors "$BRIEF_ERR" "$old_brief_size")
$(scan_new_errors "$RETRY_ERR" "$old_retry_size")"

status="ok"
details=""

if [ ! -f "$TODAY_REPORT" ]; then
  status="missing_report"
  details="today report missing before repair: $TODAY_REPORT"
  log "$details"
  DAILY_AI_BRIEF_CATCHUP_DAYS=4 /usr/bin/python3 daily_ai_brief_runner.py >> "$HEALTH_LOG" 2>&1
  repair_status=$?
  log "repair run exit=$repair_status"
  if [ ! -f "$TODAY_REPORT" ]; then
    status="failed"
    details="$details
repair failed or report still missing: $TODAY_REPORT"
  else
    status="repaired"
    details="$details
repair generated report: $TODAY_REPORT"
  fi
fi

if [ -n "$(echo "$new_errors" | tr -d '[:space:]')" ]; then
  if [ "$status" = "ok" ]; then
    status="launchd_error"
  fi
  details="$details
new launchd/script errors:
$new_errors"
fi

if [ "$status" != "ok" ] && [ "$status" != "repaired" ]; then
  send_alert "AI日报健康检查异常：$TODAY" "状态：$status

$details

项目：$ROOT
日志：$HEALTH_LOG"
elif [ "$status" = "repaired" ]; then
  send_alert "AI日报已自动补跑：$TODAY" "状态：$status

$details

项目：$ROOT
日志：$HEALTH_LOG"
fi

{
  echo "BRIEF_ERR_SIZE=$(file_size "$BRIEF_ERR")"
  echo "RETRY_ERR_SIZE=$(file_size "$RETRY_ERR")"
  echo "LAST_RUN_AT=$(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "LAST_STATUS=$status"
  echo "LAST_REPORT=$TODAY_REPORT"
} > "$STATE_FILE"

log "healthcheck done status=$status"
exit 0
