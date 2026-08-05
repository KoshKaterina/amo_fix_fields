"""Точечная отсечка вебхуков на время массового прогона (migration_freeze.bulk_skip)."""
import pytest

import migration_freeze as mf

FROM, TO = 1785790800, 1785823200
CLEVER, LEGACY, OFFICE = 10593102, 901105, 9421022
WON, LOST, WAYBILL_STAGE = 142, 143, 86475482

T = FROM + 60  # момент внутри окна


@pytest.fixture(autouse=True)
def _cfg(monkeypatch):
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_TAG", "перенесено из старой воронки")
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_FROM_TS", FROM)
    monkeypatch.setattr(mf, "MIGRATION_FREEZE_TO_TS", TO)
    monkeypatch.setattr(mf, "MIGRATION_SOURCE_PIPELINES", {LEGACY})
    monkeypatch.setattr(mf, "MIGRATION_BULK_PAUSE", True)


def test_off_without_flag(monkeypatch):
    monkeypatch.setattr(mf, "MIGRATION_BULK_PAUSE", False)
    assert not mf.bulk_skip(LEGACY, WON, T)


def test_ignored_outside_window():
    """Забыли выключить флаг — после окна он ничего не гасит."""
    assert not mf.bulk_skip(LEGACY, WON, TO + 1)
    assert not mf.bulk_skip(CLEVER, WON, FROM - 1)


def test_source_pipeline_skipped():
    """Первая волна: сделка ещё в старой воронке, ей ставят тег."""
    assert mf.bulk_skip(LEGACY, WON, T)
    assert mf.bulk_skip(LEGACY, 18029932, T)  # любой этап источника


def test_clever_closed_skipped():
    """Вторая волна: сделка приехала в основную на 142/143."""
    assert mf.bulk_skip(CLEVER, WON, T)
    assert mf.bulk_skip(CLEVER, LOST, T)


def test_live_flow_survives():
    """Главное: боевое НЕ глушим — накладные, счета, работа в основной воронке."""
    assert not mf.bulk_skip(OFFICE, WAYBILL_STAGE, T)   # накладная СДЭК
    assert not mf.bulk_skip(CLEVER, 87280230, T)        # «Оплата запрошена» → счёт
    assert not mf.bulk_skip(CLEVER, 83537714, T)        # «Новый лид»
    assert not mf.bulk_skip(10997702, 86476486, T)      # фулфилмент


def test_garbage_input_is_safe():
    assert not mf.bulk_skip(None, None, T)
    assert not mf.bulk_skip("не число", WON, T)


# ── is_bulk_move_event: отличаем событие прогона от живого закрытия (05.08.2026) ──


def test_move_event_from_source_is_bulk():
    """Сделку в 142/143 привёз прогон — приехала из воронки-источника."""
    assert mf.is_bulk_move_event(LEGACY, T)


def test_move_event_inside_clever_is_live():
    """Менеджер закрыл сделку внутри основной воронки — это боевое, не трогаем."""
    assert not mf.is_bulk_move_event(CLEVER, T)


def test_move_event_from_unlisted_pipeline_is_live():
    """Воронка не в списке источников — считаем боевым (лучше обработать лишнее,
    чем потерять заказ). Ловится предупреждением по объёму в reconcile."""
    assert not mf.is_bulk_move_event(OFFICE, T)


def test_move_event_off_outside_window_and_flag(monkeypatch):
    assert not mf.is_bulk_move_event(LEGACY, TO + 1)
    monkeypatch.setattr(mf, "MIGRATION_BULK_PAUSE", False)
    assert not mf.is_bulk_move_event(LEGACY, T)


def test_move_event_garbage_input_is_safe():
    assert not mf.is_bulk_move_event(None, T)
    assert not mf.is_bulk_move_event("не число", T)
