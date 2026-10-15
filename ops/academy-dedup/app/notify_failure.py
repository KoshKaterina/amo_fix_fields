#!/usr/bin/env python3
"""Send a throttled Telegram alert when academy-dedup.service fails."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone

ENV_PATH = pathlib.Path("/opt/integrations/amo_fix_fields/.env")
STATE_PATH = pathlib.Path("/var/lib/academy-dedup/telegram-alert-state.json")
MENTION = "@koshkatarina"
REPEAT_AFTER_S = 6 * 60 * 60
MSK = timezone(timedelta(hours=3))


def env_values() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def service_error(service: str) -> str:
    proc = subprocess.run(
        ["journalctl", "-u", service, "-n", "30", "--no-pager", "-o", "cat"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    lines = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
    useful = [line for line in lines if "Bearer " not in line and "TOKEN=" not in line]
    return (" | ".join(useful[-5:]) if useful else "Причина не записалась в журнал.")[-900:]


def read_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_state(value: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_PATH)
    os.chmod(STATE_PATH, 0o600)


def send(text: str) -> None:
    values = env_values()
    token = values.get("TG_BOT_TOKEN")
    chat_id = values.get("TG_ALLOWED_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TG_BOT_TOKEN or TG_ALLOWED_CHAT_ID is missing")
    proxy = values.get("TG_PROXY_URL") or None
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
    opener = urllib.request.build_opener(*handlers)
    body = json.dumps({"chat_id": chat_id, "text": text, "disable_web_page_preview": True}).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener.open(request, timeout=30) as response:
        payload = json.loads(response.read())
    if not payload.get("ok"):
        raise RuntimeError("Telegram API rejected alert")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default="academy-dedup.service")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    now = int(time.time())
    stamp = datetime.now(MSK).strftime("%d.%m.%Y %H:%M МСК")
    if args.test:
        send(
            f"✅ {MENTION}\nИсправлено: уведомления сторожа дублей Академии адресованы Екатерине Кошенковой.\n"
            "Предыдущий тестовый тег был ошибочным.\n"
            f"Время проверки: {stamp}."
        )
        print("test alert sent")
        return 0

    error = service_error(args.service)
    signature = re.sub(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", "<timestamp>", error)
    signature = re.sub(r"\d{8}T\d{6}[+-]\d{4}", "<run>", signature)
    fingerprint = hashlib.sha256(signature.encode()).hexdigest()
    state = read_state()
    same_recent = state.get("fingerprint") == fingerprint and now - int(state.get("sent_at") or 0) < REPEAT_AFTER_S
    if same_recent:
        print("duplicate alert suppressed")
        return 0
    send(
        f"🚨 {MENTION}\nАвтосклейка дублей Академии остановилась с ошибкой.\n"
        f"Время: {stamp}.\nПричина: {error}\n"
        "После ошибки новые сделки автоматически не склеивались."
    )
    write_state({"sent_at": now, "fingerprint": fingerprint, "service": args.service})
    print("failure alert sent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
