#!/usr/bin/env python3
"""Проверка свежести отметки бэкапа панели.

Зачем именно такая проверка. Бэкап панели сам пишет в Телеграм, когда прогон ПАДАЕТ.
Непокрытым оставался тихий случай: прогон не состоялся вовсе - выключили таймер, сервер
постоял выключенным, снесли юнит. Тогда падать нечему, чат молчит, и «бэкап есть» мы
знаем только по памяти. Здесь проверяется, что сторож различает четыре состояния
отметки и в каждом говорит человеку, куда идти.

Запуск: python -m pytest ops/watchdog/test_watchdog_backup_status.py
"""
import importlib.util
import os
import sys
import time
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("wd", Path(__file__).with_name("watchdog.py"))
wd = importlib.util.module_from_spec(spec)
sys.modules["wd"] = wd
spec.loader.exec_module(wd)


@pytest.fixture
def status(tmp_path, monkeypatch):
    """Подсовывает сторожу свою отметку и возвращает «запиши вот это» одной функцией.

    ⚠️ Путь подменяем ФИКСТУРОЙ с возвратом, а не на уровне модуля: сторож - общий
    модуль, и оставленная подмена досталась бы соседним файлам (ровно эта болезнь
    разбиралась в задаче 10и по всему репозиторию).
    """
    path = tmp_path / "STATUS"
    monkeypatch.setattr(wd, "BACKUP_STATUS_FILE", str(path))
    monkeypatch.setattr(wd, "BACKUP_MAX_AGE_H", 26)

    def write(text, age_hours=0.0):
        path.write_text(text, encoding="utf-8")
        if age_hours:
            old = time.time() - age_hours * 3600
            os.utime(path, (old, old))
        return path

    return write


def test_svezhaya_otmetka_zelenaya(status):
    status("ok 2026-10-14T19:16:51+00:00 размер=1153208КБ sqlite=9 куда=tiana@201.51.4.65:/var/backups")
    name, ok, why = wd.backup_status_check()
    assert ok, (name, why)
    assert "свежая" in why


def test_otmetki_net_vovse(status, tmp_path, monkeypatch):
    monkeypatch.setattr(wd, "BACKUP_STATUS_FILE", str(tmp_path / "нет-такого"))
    name, ok, why = wd.backup_status_check()
    assert not ok
    assert "прогона не было ни разу" in why


def test_otmetka_govorit_fail(status):
    status("fail 2026-10-14T19:04:49+00:00 шаг=шифрование и отправка код=1")
    name, ok, why = wd.backup_status_check()
    assert not ok
    assert "НЕ удался" in why
    # В тексте обязан быть сам шаг: без него человек не знает, куда смотреть.
    assert "шифрование" in why


def test_otmetka_protuhla(status):
    """Главный случай: прогон когда-то удался, а потом перестал запускаться."""
    status("ok 2026-10-10T02:05:00+00:00 размер=1153208КБ sqlite=9 куда=локально", age_hours=48)
    name, ok, why = wd.backup_status_check()
    assert not ok
    assert "перестали запускаться" in why
    assert "48" in why or "2 дн" in why or "дн" in why


def test_granica_porogov_ne_srabatyvaet_rano(status):
    """25 часов - это ещё норма: прогон в 02:00 плюс его длительность.

    Ровно сутки порогом брать нельзя - один медленный прогон дал бы ложную тревогу.
    """
    status("ok 2026-10-13T02:05:00+00:00 размер=1 sqlite=9 куда=локально", age_hours=25)
    _, ok, why = wd.backup_status_check()
    assert ok, why


def test_pustaya_otmetka(status):
    status("   \n")
    _, ok, why = wd.backup_status_check()
    assert not ok
    assert "пустая" in why


def test_net_prav_prochitat_ne_molchim(status, monkeypatch):
    """Ослепшая проверка опаснее отсутствующей: она вечно зелёная."""
    status("ok 2026-10-14T19:16:51+00:00 размер=1 sqlite=9 куда=локально")

    def boom(*a, **kw):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr("builtins.open", boom)
    _, ok, why = wd.backup_status_check()
    assert not ok
    assert "нет прав" in why and "ослепла" in why


def test_proverka_vhodit_v_rezhim_services(status, monkeypatch):
    """Мало написать функцию - она должна попасть в прогон режима `services`."""
    status("ok 2026-10-14T19:16:51+00:00 размер=1 sqlite=9 куда=локально")
    monkeypatch.setattr(wd, "telegram_contour_check", lambda: None)
    monkeypatch.setattr(wd, "WATCH_CONTAINERS", [])
    monkeypatch.setattr(wd, "WATCH_TIMERS", [])
    monkeypatch.setattr(wd, "_sh", lambda *a, **kw: (0, ""))
    names = [name for name, _, _ in wd.services_checks()]
    assert "Бэкап панели · отметка" in names


def test_taymery_bekapa_pod_nablyudeniem():
    """Второй половиной сторожа: выключенный таймер тоже должен быть замечен."""
    assert "team-backup.timer" in wd.WATCH_TIMERS
    assert "team-backup-verify.timer" in wd.WATCH_TIMERS


def test_proverka_ne_srochnaya():
    """Бэкап клиента не задевает: будить этим нельзя, место такому - в сводке."""
    assert not wd.is_urgent("Бэкап панели · отметка")
