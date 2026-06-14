#!/usr/bin/env python3
"""
Terminal sender for daily_ai_brief_runner.

Usage:
  python3 daily_ai_terminal_sender.py                # 发送目录下所有待发邮件
  python3 daily_ai_terminal_sender.py --file xxx.json # 只发指定任务
"""

from __future__ import annotations

import argparse
import json
import os
import re
import smtplib
import ssl
from datetime import datetime
from email import message_from_string
from email.message import EmailMessage
from email.policy import default as email_policy
from pathlib import Path
from typing import Dict, List


def _read_env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name, "")
        if value is not None:
            value = value.strip()
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

OUTBOX_DIR = Path(__file__).with_name("daily_ai_terminal_unsent")


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


def _load_queue_file(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _parse_message(payload: Dict) -> EmailMessage:
    raw = payload.get("raw", "").strip()
    if not raw:
        raise ValueError("queue 文件中缺少 raw 邮件内容")
    return message_from_string(raw, _class=EmailMessage, policy=email_policy)


def send_file(path: Path) -> tuple[bool, str]:
    payload = _load_queue_file(path)
    msg = _parse_message(payload)
    result = _send_via_smtp(msg)
    if result.get("status") != "sent":
        return False, result.get("reason", "发送失败")
    return True, "sent"


def list_pending(path: Path) -> List[Path]:
    return sorted(path.glob("*.json"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-dir", default=str(OUTBOX_DIR))
    parser.add_argument("--file", help="指定单个队列文件发送")
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
            print(f"[{datetime.fromisoformat(created_at).strftime('%F %T') if created_at else datetime.now().strftime('%F %T')}] {file.name} -> {to} | {subject}")
            if args.dry_run:
                continue
            ok, reason = send_file(file)
            if ok:
                file.unlink(missing_ok=True)
                print("  SUCCESS")
            else:
                failed += 1
                print(f"  FAIL: {reason}")
        except Exception as exc:
            failed += 1
            print(f"  FAIL: {exc}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
