#!/usr/bin/env python3
"""Проверка догляда за контуром телеграм-уведомлений.

Ради этой проверки всё и затевалось: 28.08.2026 бот интеграции выключился на
старте контейнера и сутки молча глушил алерты отдела продаж. Контейнер был жив,
докер довольный - заметить было нечем.

Запуск: python3 test_watchdog_contour.py
"""
import importlib.util
import json
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("wd", Path(__file__).with_name("watchdog.py"))
wd = importlib.util.module_from_spec(spec)
sys.modules["wd"] = wd
spec.loader.exec_module(wd)


def fake_http(payload, code=200):
    body = payload if isinstance(payload, str) else json.dumps(payload)
    wd.http = lambda url, **kw: (code, body)


# ── 1) всё хорошо: опрос идёт, подавленных нет ─────────────────────────────
fake_http({"telegram": {"configured": True, "enabled": True, "polling": True,
                        "suppressed_streak": 0}})
name, ok, why = wd.telegram_contour_check()
assert ok and "опрос идёт" in why, (name, ok, why)
print("✓ живой контур: проверка зелёная")

# ── 2) боевой случай 28.08: бот выключен, контейнер жив ────────────────────
fake_http({"telegram": {"configured": True, "enabled": False, "polling": False,
                        "suppressed_streak": 53}})
name, ok, why = wd.telegram_contour_check()
assert not ok and "ВЫКЛЮЧЕН" in why and "рестартом" in why, (name, ok, why)
print("✓ выключенный бот: ловится, в тексте сразу сказано, чем лечить")

# ── 3) бот включён, а опрос стоит (упавший polling) ────────────────────────
fake_http({"telegram": {"configured": True, "enabled": True, "polling": False,
                        "suppressed_streak": 0}})
name, ok, why = wd.telegram_contour_check()
assert not ok and "опрос" in why, (name, ok, why)
print("✓ вставший опрос при включённом боте: ловится отдельно")

# ── 4) отправка не проходит подряд - тоже повод ────────────────────────────
fake_http({"telegram": {"configured": True, "enabled": True, "polling": True,
                        "suppressed_streak": 7}})
name, ok, why = wd.telegram_contour_check()
assert not ok and "7" in why, (name, ok, why)
print("✓ череда неотправленных сообщений: ловится")

# ── 5) молчим там, где сказать нечего ──────────────────────────────────────
fake_http({"telegram": {"configured": False}})
assert wd.telegram_contour_check() is None, "контур не настроен - жаловаться не на что"
fake_http({"lanes": {}})
assert wd.telegram_contour_check() is None, "старая версия интеграции - состояния нет"
fake_http("", code=0)
assert wd.telegram_contour_check() is None, "контейнер лежит - об этом скажет другая проверка"
print("✓ не настроен, старая версия, лежачий контейнер: молчим, не шумим зря")

# ── 6) мусор вместо JSON - жалуемся, а не падаем ───────────────────────────
fake_http("это не json", code=200)
name, ok, why = wd.telegram_contour_check()
assert not ok and "JSON" in why, (name, ok, why)
print("✓ мусор вместо состояния: честная жалоба вместо исключения")

# ── 7) проверка реально попадает в общий список сервисов ───────────────────
fake_http({"telegram": {"configured": True, "enabled": False, "polling": False,
                        "suppressed_streak": 1}})
wd._sh = lambda cmd, timeout=60: (0, "")
wd.WATCH_CONTAINERS = []
wd.WATCH_TIMERS = []
names = [n for n, _, _ in wd.services_checks()]
assert any("Телеграм-бот" in n for n in names), names
print("✓ проверка встроена в общий обход сервисов")

print("\nдогляд за контуром телеграм-уведомлений: все проверки прошли")
