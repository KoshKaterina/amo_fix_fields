#!/usr/bin/env python3
"""GetCourse в режиме apis: верный ключ (907 «Объект не найден») - ок, неверный
(901 «Неавторизованное API-обращение») - сломано. Ответы сняты с боевого API 10.11.2026.

Запуск: python3 test_watchdog_getcourse.py
"""
import importlib.util
import json
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("wd_gc", Path(__file__).with_name("watchdog.py"))
w = importlib.util.module_from_spec(spec)
sys.modules["wd_gc"] = w
spec.loader.exec_module(w)

FAILS = []
SECRETS = {"GETCOURSE_API_KEY": "k", "INSALES_API_KEY": "", "INSALES_API_PASSWORD": ""}
w.secret = lambda name: SECRETS.get(name, "")

for body, want in (({"success": False, "error_message": "Объект не найден", "error": True, "error_code": 907}, True),
                   ({"success": False, "error_message": "Неавторизованное API-обращение", "error": True, "error_code": 901}, False)):
    w.http = lambda url, headers=None, timeout=30, b=body: (200, json.dumps(b, ensure_ascii=False))
    got = dict((n, ok) for n, ok, _ in w.check_extra_apis())
    if got.get("GetCourse") is not want:
        FAILS.append(f"{body['error_code']}: ждали {want}, получили {got.get('GetCourse')}")

w.http = lambda url, headers=None, timeout=30: (0, "")
res = w.check_extra_apis()
if res != [("GetCourse", False, "HTTP нет ответа")]:
    FAILS.append(f"нет ответа: {res}")

if FAILS:
    print("ПАДЕНИЯ:\n" + "\n".join(FAILS))
    sys.exit(1)
print("ок: GetCourse")
