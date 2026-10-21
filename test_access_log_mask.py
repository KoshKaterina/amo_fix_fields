#!/usr/bin/env python3
"""Секрет из пути не должен попадать в журнал приложения.

Зачем эта проверка. 15.10.2026 секреты вебхуков убрали из access-лога nginx форматом лога.
Через минуту выяснилось, что **uvicorn пишет строку запроса сам**, в журнал контейнера, и
там секрет оставался - то есть мера была сделана наполовину, а выглядела законченной.
Здесь проверяется вторая половина: фильтр на `uvicorn.access`.

Запуск: python -m pytest test_access_log_mask.py
"""
import logging

import pytest

import webhooks


# ── что прячем, что нет ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("path, expected", [
    ("/wazzup/abc123SECRET", "/wazzup/HIDDEN"),
    ("/jivo/tokenZZZ", "/jivo/HIDDEN"),
    ("/jivo/tokenZZZ/site42", "/jivo/HIDDEN/site42"),
    ("/uis/secretVALUE", "/uis/HIDDEN"),
    ("/bothelp/academy/sss", "/bothelp/academy/HIDDEN"),
    ("/admin/lead-distribution/sss/sources", "/admin/lead-distribution/HIDDEN/sources"),
    ("/getcourse/gcSECRET", "/getcourse/HIDDEN"),
])
def test_sekret_iz_puti_pryachetsya(path, expected):
    assert webhooks.mask_secret_path(path) == expected


@pytest.mark.parametrize("path", [
    "/lead_change",
    "/contact_change",
    "/cdek_status",
    "/site_form",
    "/",
    "/barcode/1234567890",
])
def test_obychnye_puti_ne_trogaem(path):
    """Иначе разбор логов станет бесполезным: мы по ним считаем поток вебхуков."""
    assert webhooks.mask_secret_path(path) == path


def test_vtoroy_segment_sohranyaetsya():
    """У Jivo второй сегмент - идентификатор сайта, он не секрет и нужен в логе."""
    assert webhooks.mask_secret_path("/jivo/SEC/site") == "/jivo/HIDDEN/site"


# ── сам фильтр ────────────────────────────────────────────────────────────────────

def _record(args):
    return logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1,
                             '%s - "%s %s HTTP/%s" %d', args, None)


def test_filtr_pravit_imenno_put():
    f = webhooks.MaskSecretsInAccessLog()
    rec = _record(("1.2.3.4:5", "POST", "/wazzup/SECRET123", "1.1", 200))
    assert f.filter(rec) is True
    assert rec.args[2] == "/wazzup/HIDDEN"
    # остальные поля не тронуты
    assert rec.args[1] == "POST" and rec.args[4] == 200


def test_filtr_ne_portit_obychnuyu_zapis():
    f = webhooks.MaskSecretsInAccessLog()
    rec = _record(("1.2.3.4:5", "POST", "/lead_change", "1.1", 200))
    f.filter(rec)
    assert rec.args[2] == "/lead_change"


@pytest.mark.parametrize("args", [
    None,
    (),
    ("только один",),
    ("два", "аргумента"),
    ("путь не строкой", "POST", 42, "1.1", 200),
])
def test_filtr_bezotkazen_na_lyuboy_zapisi(args):
    """Исключение отсюда прилетело бы в середину журналирования запроса.

    Поэтому фильтр обязан пропускать всё, чего не понимает, а не падать.
    """
    f = webhooks.MaskSecretsInAccessLog()
    rec = _record(args)
    assert f.filter(rec) is True


def test_filtr_stavitsya_odin_raz():
    """Повторный подъём приложения не должен навешивать второй такой же фильтр."""
    logger = logging.getLogger("uvicorn.access")
    before = [f for f in logger.filters if isinstance(f, webhooks.MaskSecretsInAccessLog)]
    added = webhooks.MaskSecretsInAccessLog()
    logger.addFilter(added)
    try:
        assert len([f for f in logger.filters
                    if isinstance(f, webhooks.MaskSecretsInAccessLog)]) == len(before) + 1
    finally:
        logger.removeFilter(added)


def test_spisok_prefiksov_sovpadaet_s_zhivymi_marshrutami():
    """Список префиксов врёт МОЛЧА, если маршрут переименовали.

    Поэтому сверяем его с настоящими маршрутами приложения: у каждого префикса должен быть
    хотя бы один живой маршрут, который с него начинается.
    """
    paths = [getattr(r, "path", "") for r in webhooks.app.routes]
    for prefix in webhooks.SECRET_PATH_PREFIXES:
        if prefix == "/admin/lead-distribution/":
            continue  # живёт в отдельном модуле-роутере, не в webhooks.app
        assert any(p.startswith(prefix) for p in paths), (
            f"префикс {prefix} не соответствует ни одному маршруту - его переименовали?")
