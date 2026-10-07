#!/usr/bin/env python3
"""Проверка догляда за контуром телеграм-уведомлений.

Ради этой проверки всё и затевалось: 28.08.2026 бот интеграции выключился на
старте контейнера и сутки молча глушил алерты отдела продаж. Контейнер был жив,
докер довольный - заметить было нечем.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде проверки стояли голыми `assert` на уровне
модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА и ронял сбор всего
репозитория, а в сводке файл давал ноль тестов. Плюс первый упавший `assert` обрывал файл,
и про остальные шесть сценариев мы не узнавали ничего.

Запуск: python -m pytest ops/watchdog/test_watchdog_contour.py
"""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("wd", Path(__file__).with_name("watchdog.py"))
wd = importlib.util.module_from_spec(spec)
sys.modules["wd"] = wd
spec.loader.exec_module(wd)


def fake_http(payload, code=200):
    body = payload if isinstance(payload, str) else json.dumps(payload)
    wd.http = lambda url, **kw: (code, body)


def _state(**kw):
    """Состояние контура с разумными значениями по умолчанию."""
    base = {"configured": True, "enabled": True, "polling": True, "suppressed_streak": 0}
    base.update(kw)
    return {"telegram": base}


def test_zhivoy_kontur_proverka_zelenaya():
    fake_http(_state())
    name, ok, why = wd.telegram_contour_check()
    assert ok and "опрос идёт" in why, (name, ok, why)


def test_vyklyuchennyy_bot_lovitsya_i_skazano_chem_lechit():
    """Боевой случай 28.08.2026: бот выключен, контейнер жив."""
    fake_http(_state(enabled=False, polling=False, suppressed_streak=53))
    name, ok, why = wd.telegram_contour_check()
    assert not ok and "ВЫКЛЮЧЕН" in why and "рестартом" in why, (name, ok, why)


def test_vstavshiy_opros_pri_vklyuchennom_bote_lovitsya_otdelno():
    fake_http(_state(polling=False))
    name, ok, why = wd.telegram_contour_check()
    assert not ok and "опрос" in why, (name, ok, why)


def test_chereda_neotpravlennyh_soobshcheniy_lovitsya():
    fake_http(_state(suppressed_streak=7))
    name, ok, why = wd.telegram_contour_check()
    assert not ok and "7" in why, (name, ok, why)


@pytest.mark.parametrize(
    ("payload", "code", "pochemu_molchim"),
    [
        ({"telegram": {"configured": False}}, 200, "контур не настроен - жаловаться не на что"),
        ({"lanes": {}}, 200, "старая версия интеграции - состояния нет"),
        ("", 0, "контейнер лежит - об этом скажет другая проверка"),
    ],
)
def test_molchim_tam_gde_skazat_nechego(payload, code, pochemu_molchim):
    fake_http(payload, code=code)
    assert wd.telegram_contour_check() is None, pochemu_molchim


def test_musor_vmesto_json_chestnaya_zhaloba_a_ne_isklyuchenie():
    fake_http("это не json", code=200)
    name, ok, why = wd.telegram_contour_check()
    assert not ok and "JSON" in why, (name, ok, why)


def test_proverka_vstroena_v_obshchiy_obhod_servisov():
    """⚠️ Подменяем глобальные списки сервисов - и возвращаем их на место.

    Пока файл был скриптом, возврат был не нужен (процесс заканчивался). В pytest остаток
    достался бы соседним тестам, а порядок в прогоне не обещан.
    """
    was_sh, was_containers, was_timers = wd._sh, wd.WATCH_CONTAINERS, wd.WATCH_TIMERS
    try:
        fake_http(_state(enabled=False, polling=False, suppressed_streak=1))
        wd._sh = lambda cmd, timeout=60: (0, "")
        wd.WATCH_CONTAINERS = []
        wd.WATCH_TIMERS = []
        names = [n for n, _, _ in wd.services_checks()]
        assert any("Телеграм-бот" in n for n in names), names
    finally:
        wd._sh, wd.WATCH_CONTAINERS, wd.WATCH_TIMERS = was_sh, was_containers, was_timers
