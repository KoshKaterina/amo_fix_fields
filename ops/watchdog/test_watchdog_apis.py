#!/usr/bin/env python3
"""Режим `apis` (10.11.2026): проверки ключей уходят в панель для плашек «Здоровья», в
Телеграм ничего не пишется, состояние сторожа не трогается.

Запуск: python3 test_watchdog_apis.py
"""
import importlib.util
import json
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("wd_apis", Path(__file__).with_name("watchdog.py"))
w = importlib.util.module_from_spec(spec)
sys.modules["wd_apis"] = w
spec.loader.exec_module(w)

w.log = lambda *a, **k: None
FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)


sent_tg, saved, posted = [], [], []
w.tg_send = lambda text: sent_tg.append(text) or True
w.save_state = lambda st: saved.append(st)


def fake_tokens(metrika_hours=24):
    check(metrika_hours == 3, f"метрика за {metrika_hours} ч, ждали 3")
    w._TIMINGS.update({"amoCRM": 120, "МойСклад": 300})
    return [("amoCRM", True, ""), ("МойСклад", False, "HTTP 401 · Unauthorized"),
            ("Яндекс.Метрика (загрузка заказов)", True, "за 3 ч отправлено заказов: 4")]


w.check_tokens = fake_tokens
w.check_telegram = lambda: ("Telegram Bot API", True, "")
w.parse_env_file = lambda path: {"ERRORS_INGEST_URL": "https://panel.test/api/ingest/errors", "INGEST_TOKEN": "t"}


class FakeResp:
    def close(self):
        pass


def fake_urlopen(req, timeout=0):
    posted.append((req.full_url, json.loads(req.data.decode("utf-8")), dict(req.header_items())))
    return FakeResp()


import urllib.request  # noqa: E402
urllib.request.urlopen = fake_urlopen

rc = w.run_apis()
check(rc == 0, "код выхода не 0")
check(sent_tg == [], "apis не должен писать в Телеграм")
check(saved == [], "apis не должен трогать состояние сторожа")
check(len(posted) == 1, f"ждали один запрос в панель, было {len(posted)}")
if posted:
    url, body, headers = posted[0]
    check(url == "https://panel.test/api/ingest/health", f"адрес {url}")
    names = {a["name"]: a for a in body["apis"]}
    check(names["amoCRM"] == {"name": "amoCRM", "ok": True, "ms": 120}, f"amo {names.get('amoCRM')}")
    check(names["МойСклад"]["why"].startswith("HTTP 401") and not names["МойСклад"]["ok"], "склад")
    check(names["Яндекс.Метрика (загрузка заказов)"]["info"] == "за 3 ч отправлено заказов: 4", "метрика")
    check("Telegram Bot API" in names, "телеграм")
    check(headers.get("X-ingest-token") == "t", "токен")
    check("TOKEN" not in json.dumps(body), "в теле не должно быть ключей")

check("apis" in w.CHECKS, "режима apis нет в CHECKS")

if FAILS:
    print("ПАДЕНИЯ:\n" + "\n".join(FAILS))
    sys.exit(1)
print("ок: режим apis")
