#!/usr/bin/env python3
"""
Terminal sender for daily_ai_brief_runner.

Usage:
  python3 daily_ai_terminal_sender.py                # 发送目录下所有待发邮件
  python3 daily_ai_terminal_sender.py --retry-pending # 按退避策略重试待发邮件
  python3 daily_ai_terminal_sender.py --file xxx.json # 只发指定任务
"""

from __future__ import annotations

import argparse
import json
import os
import re
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email import message_from_string
from email.message import EmailMessage
from email.policy import default as email_policy
from pathlib import Path
from typing import Dict, List


CONFIG_FILE = Path(__file__).with_name("report.config")
_CONFIG_CACHE: dict[str, str] | None = None


def _load_config() -> dict[str, str]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    config: dict[str, str] = {}
    if not CONFIG_FILE.exists():
        _CONFIG_CACHE = config
        return config
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text or text.startswith("#") or text.startswith(";"):
                    continue
                if text.startswith("export "):
                    text = text[7:].strip()
                if "=" not in text:
                    continue
                k, v = text.split("=", 1)
                config[k.strip()] = v.strip().strip().strip("'").strip('"')
    except Exception:
        config = {}
    _CONFIG_CACHE = config
    return config


def _read_env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name, "")
        if value is not None:
            value = value.strip()
            if value:
                return value
    for name in names:
        value = _load_config().get(name, "").strip()
        if value:
            return value
    return default.strip()


SMTP_HOST = _read_env_first("DAILY_AI_BRIEF_SMTP_HOST", "SMTP_HOST", "EMAIL_SMTP_HOST", default="smtp.gmail.com")
SMTP_PORT = int(_read_env_first("DAILY_AI_BRIEF_SMTP_PORT", "SMTP_PORT", "EMAIL_SMTP_PORT", default="587"))
SMTP_USER = _read_env_first(
    "DAILY_AI_BRIEF_SMTP_USER",
    "EMAIL_SMTP_USER",
    "SMTP_USER",
    "SMTP_USERNAME",
)
SMTP_PASSWORD = re.sub(
    r"\s+",
    "",
    _read_env_first(
        "DAILY_AI_BRIEF_SMTP_PASSWORD",
        "EMAIL_SMTP_PASSWORD",
        "EMAIL_APP_PASSWORD",
        "SMTP_PASSWORD",
        "GMAIL_APP_PASSWORD",
    ),
)
SMTP_SSL = _read_env_first("DAILY_AI_BRIEF_SMTP_SSL", default="0").lower() in ("1", "true", "yes", "on")
SMTP_STARTTLS = _read_env_first("DAILY_AI_BRIEF_SMTP_STARTTLS", default="1").lower() in ("1", "true", "yes", "on")
SMTP_TIMEOUT = int(_read_env_first("DAILY_AI_BRIEF_SMTP_TIMEOUT", "SMTP_TIMEOUT", default="20"))
SMTP_CA_FILE = os.getenv("DAILY_AI_BRIEF_SMTP_CA_FILE", "").strip()
if not SMTP_CA_FILE:
    SMTP_CA_FILE = os.getenv("SSL_CERT_FILE", "")
SMTP_INSECURE = _read_env_first("DAILY_AI_BRIEF_SMTP_INSECURE", "SMTP_INSECURE", default="0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
EMAIL_TRANSPORT = _read_env_first("DAILY_AI_BRIEF_EMAIL_TRANSPORT", "EMAIL_TRANSPORT", default="auto").lower()
SENDMAIL_PATH = _read_env_first("DAILY_AI_BRIEF_SENDMAIL_PATH", "SENDMAIL_PATH", default="/usr/sbin/sendmail")

OUTBOX_DIR = Path(__file__).with_name("daily_ai_terminal_unsent")
RETRY_BASE_SECONDS = int(_read_env_first("DAILY_AI_BRIEF_RETRY_BASE_SECONDS", default="900"))
RETRY_MAX_DELAY_SECONDS = int(_read_env_first("DAILY_AI_BRIEF_RETRY_MAX_DELAY_SECONDS", default="14400"))
RETRY_MAX_AGE_SECONDS = int(_read_env_first("DAILY_AI_BRIEF_RETRY_MAX_AGE_SECONDS", default="86400"))


def _split_recipients(raw: str) -> List[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def _current_recipients() -> List[str]:
    raw = _read_env_first("DAILY_AI_BRIEF_EMAIL_TO", "EMAIL_TO_ADDRESSES", "EMAIL_TO")
    return _split_recipients(raw)


def _now() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def _parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    local_tz = _now().tzinfo
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=local_tz)
        return parsed.astimezone(local_tz)
    except Exception:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            return parsed.replace(tzinfo=local_tz)
        except Exception:
            return None


def _build_tls_context():
    if SMTP_INSECURE:
        return ssl._create_unverified_context()
    candidates = [SMTP_CA_FILE, "/etc/ssl/cert.pem", "/etc/pki/tls/certs/ca-bundle.crt", "/usr/local/etc/openssl@3/cert.pem"]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            try:
                return ssl.create_default_context(cafile=str(candidate))
            except Exception:
                continue
    if SMTP_CA_FILE and Path(SMTP_CA_FILE).exists():
        try:
            return ssl.create_default_context(cafile=SMTP_CA_FILE)
        except Exception:
            return ssl.create_default_context()
    return ssl.create_default_context()


def _send_via_smtp(msg: EmailMessage) -> Dict[str, object]:
    try:
        context = _build_tls_context()
        if SMTP_SSL:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT, context=context) as smtp:
                if SMTP_USER and SMTP_PASSWORD:
                    smtp.login(SMTP_USER, SMTP_PASSWORD)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as smtp:
                if SMTP_STARTTLS:
                    smtp.starttls(context=context)
                if SMTP_USER and SMTP_PASSWORD:
                    smtp.login(SMTP_USER, SMTP_PASSWORD)
                smtp.send_message(msg)
        return {"status": "sent"}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}


def _send_via_sendmail(msg: EmailMessage) -> Dict[str, object]:
    sendmail = Path(SENDMAIL_PATH)
    if not SENDMAIL_PATH or not sendmail.exists():
        return {"status": "skip", "reason": f"sendmail 不存在：{SENDMAIL_PATH}"}
    try:
        import subprocess

        proc = subprocess.run(
            [str(sendmail), "-t"],
            input=msg.as_bytes(),
            capture_output=True,
            text=False,
            timeout=20,
        )
        if proc.returncode == 0:
            return {"status": "sent", "method": "sendmail", "path": str(sendmail)}
        return {"status": "error", "reason": proc.stderr.decode(errors="ignore") if proc.stderr else "sendmail 返回非0"}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}


def _send_message(msg: EmailMessage) -> Dict[str, object]:
    errors: List[str] = []
    if EMAIL_TRANSPORT in {"auto", "smtp"}:
        result = _send_via_smtp(msg)
        if result.get("status") == "sent":
            return result
        errors.append(f"SMTP: {result.get('reason', '发送失败')}")
        if EMAIL_TRANSPORT == "smtp":
            return {"status": "error", "reason": "; ".join(errors)}
    if EMAIL_TRANSPORT in {"auto", "sendmail"}:
        result = _send_via_sendmail(msg)
        if result.get("status") == "sent":
            return result
        errors.append(f"sendmail: {result.get('reason', '发送失败')}")
    return {"status": "error", "reason": "; ".join(errors) or "无可用发送通道"}


def _load_queue_file(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _parse_message(payload: Dict) -> EmailMessage:
    raw = payload.get("raw", "").strip()
    if not raw:
        raise ValueError("queue 文件中缺少 raw 邮件内容")
    msg = message_from_string(raw, _class=EmailMessage, policy=email_policy)
    recipients = _current_recipients()
    if recipients:
        if msg.get("To"):
            msg.replace_header("To", ", ".join(recipients))
        else:
            msg["To"] = ", ".join(recipients)
        payload["to"] = ", ".join(recipients)
        payload["raw"] = msg.as_string()
    return msg


def _write_queue_file(path: Path, payload: Dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _mark_retry_failure(path: Path, payload: Dict, reason: str) -> None:
    now = _now()
    attempts = int(payload.get("attempts", 0) or 0) + 1
    created_at = _parse_dt(payload.get("created_at")) or now
    deadline = created_at + timedelta(seconds=max(0, RETRY_MAX_AGE_SECONDS))
    delay_seconds = min(max(RETRY_BASE_SECONDS, 1) * (2 ** max(0, attempts - 1)), max(RETRY_MAX_DELAY_SECONDS, 1))
    next_retry = min(now + timedelta(seconds=delay_seconds), deadline)
    payload.update(
        {
            "attempts": attempts,
            "last_attempt_at": now.isoformat(),
            "last_error": reason,
            "next_retry_at": next_retry.isoformat(),
            "retry_deadline_at": deadline.isoformat(),
        }
    )
    _write_queue_file(path, payload)


def _retry_status(payload: Dict) -> tuple[bool, str]:
    now = _now()
    created_at = _parse_dt(payload.get("created_at")) or now
    deadline = created_at + timedelta(seconds=max(0, RETRY_MAX_AGE_SECONDS))
    if now > deadline:
        return False, f"expired after {RETRY_MAX_AGE_SECONDS} seconds"
    next_retry = _parse_dt(payload.get("next_retry_at"))
    if next_retry and now < next_retry:
        return False, f"next retry at {next_retry.isoformat()}"
    return True, "due"


def send_file(path: Path) -> tuple[bool, str, Dict]:
    payload = _load_queue_file(path)
    msg = _parse_message(payload)
    result = _send_message(msg)
    if result.get("status") != "sent":
        reason = result.get("reason", "发送失败")
        _mark_retry_failure(path, payload, str(reason))
        return False, str(reason), payload
    return True, "sent", payload


def list_pending(path: Path) -> List[Path]:
    return sorted(path.glob("*.json"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-dir", default=str(OUTBOX_DIR))
    parser.add_argument("--file", help="指定单个队列文件发送")
    parser.add_argument("--retry-pending", action="store_true", help="只重试已到期且未超过24小时窗口的队列邮件")
    parser.add_argument("--dry-run", action="store_true", help="仅打印待发送文件，不实际发送")
    args = parser.parse_args()

    queue_dir = Path(args.queue_dir)
    files = []
    if args.file:
        files = [Path(args.file)]
    else:
        files = list_pending(queue_dir)

    if not files:
        print("No pending terminal email queue found.")
        return 0

    print(f"Found {len(files)} terminal email file(s).")
    failed = 0
    for file in files:
        try:
            payload = _load_queue_file(file)
            created_at = payload.get("created_at") or ""
            subject = payload.get("subject") or "(无主题)"
            to = payload.get("to") or ""
            if args.retry_pending:
                due, due_reason = _retry_status(payload)
                if not due:
                    print(f"SKIP {file.name}: {due_reason}")
                    continue
            print(f"[{datetime.fromisoformat(created_at).strftime('%F %T') if created_at else datetime.now().strftime('%F %T')}] {file.name} -> {to} | {subject}")
            if args.dry_run:
                continue
            ok, reason, payload = send_file(file)
            if ok:
                file.unlink(missing_ok=True)
                print(f"  SUCCESS -> {payload.get('to') or to}")
            else:
                failed += 1
                print(f"  FAIL: {reason}")
        except Exception as exc:
            failed += 1
            print(f"  FAIL: {exc}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
