"""Тесты заморозки на время миграции воронок (migration_freeze)."""

import asyncio

import pytest

import migration_freeze as mf


TAG = "перенесено из старой воронки"
FROM = 1785769200   # 03.08.2026 18:00 МСК
TO = 1785803400     # 04.08.2026 03:30 МСК


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_TAG", TAG)
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_FROM_TS", FROM)
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_TO_TS", TO)
    mf._cache.clear()


def _lead(*tags):
    return {"id": 1, "_embedded": {"tags": [{"name": t} for t in tags]}}


def test_window_closed_before_and_after():
    assert not mf.window_active(FROM - 1)
    assert mf.window_active(FROM)
    assert mf.window_active(TO)
    assert not mf.window_active(TO + 1)


def test_window_off_without_tag(monkeypatch):
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_TAG", "")
    assert not mf.window_active(FROM + 100)


def test_frozen_lead_in_window(monkeypatch):
    monkeypatch.setattr(mf.time, "time", lambda: FROM + 60)
    assert asyncio.run(mf.is_frozen(1, lead=_lead(TAG, "шоурум")))


def test_other_tags_not_frozen(monkeypatch):
    monkeypatch.setattr(mf.time, "time", lambda: FROM + 60)
    assert not asyncio.run(mf.is_frozen(1, lead=_lead("пропущенный")))


def test_after_window_tag_no_longer_blocks(monkeypatch):
    """Главное свойство: тег остаётся на сделке, но после окна не блокирует."""
    monkeypatch.setattr(mf.time, "time", lambda: TO + 1)
    assert not asyncio.run(mf.is_frozen(1, lead=_lead(TAG)))


def test_fetches_lead_when_not_given(monkeypatch):
    """Вебхук тегов не присылает — сделку дочитываем, ответ кешируем."""
    calls = []

    async def fake_get(lead_id, with_=()):
        calls.append(lead_id)
        return _lead(TAG)

    monkeypatch.setattr(mf.time, "time", lambda: FROM + 60)
    monkeypatch.setattr(mf.amo_service, "get_lead_full", fake_get)

    assert asyncio.run(mf.is_frozen(42))
    assert asyncio.run(mf.is_frozen(42))
    assert calls == [42], "второй раз должен браться из кеша"


def test_no_fetch_outside_window(monkeypatch):
    """Вне окна проверка бесплатная: в amo не ходим."""
    async def boom(*a, **kw):
        raise AssertionError("не должно быть запроса в amo вне окна")

    monkeypatch.setattr(mf.time, "time", lambda: TO + 100)
    monkeypatch.setattr(mf.amo_service, "get_lead_full", boom)
    assert not asyncio.run(mf.is_frozen(42))
