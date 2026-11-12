"""Тесты журнала ошибок интеграции (error_journal.py): классификация техчата, обработчик
лога, очередь и выключатель. Сеть не нужна: отправка подменяется."""
import logging
import os
import queue

import pytest

import error_journal as ej


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("TEAM_PANEL_BASE_URL", "https://panel.test")
    monkeypatch.setenv("TEAM_PANEL_INGEST_TOKEN", "t" * 64)
    monkeypatch.delenv("ERROR_JOURNAL_ENABLED", raising=False)
    monkeypatch.setattr(ej, "_q", queue.Queue(maxsize=ej.QUEUE_MAX))
    monkeypatch.setattr(ej, "_ensure_thread", lambda: None)
    monkeypatch.setattr(ej, "SPOOL", str(tmp_path / "spool.jsonl"))
    monkeypatch.setattr(ej, "_decided", {})
    yield


def drain():
    out = []
    while not ej._q.empty():
        out.append(ej._q.get_nowait())
    return out


def test_metrika_message_classified():
    ev = ej.classify_tech("Metrika: ошибка загрузки заказа 4512: Metrika simple_orders: 400")
    assert ev["code"] == "metrika.order_upload_failed" and ev["severity"] == "error"
    assert ev["component"] == "metrika_sync"


def test_people_notifications_are_not_journaled():
    """Разведение 09.11: работа для людей - уведомление, в журнал не идёт."""
    for text in ("Сделка 36553383: СДЭК отклонил заказ: Recipient location is not recognized",
                 "🚫 Сообщение НЕ отправлено\nwhatsapp",
                 "📬 Письмо клиента по закрытой сделке 123",
                 "Похоже, задвоилась сделка 4567",
                 "GetCourse: заказ 77 разобрать руками"):
        assert ej.classify_tech(text) is None, text


def test_html_stripped_and_tech_reports_kept():
    s = ej.classify_tech("📊 <b>Доставка Wazzup за сутки</b>\nвсего 120")
    assert s["code"] == "wazzup.daily_summary" and s["severity"] == "info"
    assert "<b>" not in s["message"]


def test_unknown_tech_message_grouped_by_text_without_numbers():
    a = ej.classify_tech("Что-то новое случилось 15 раз")
    b = ej.classify_tech("Что-то новое случилось 27 раз")
    assert a["code"] == "tech_chat.other" and a["fingerprint"] == b["fingerprint"]


def test_mirror_and_switch(monkeypatch):
    ej.mirror_tech_chat("Metrika: ошибка загрузки заказа 4512: 400")
    evs = drain()
    assert len(evs) == 1 and evs[0]["service"] == "amo-fix-fields"
    assert evs[0]["code"] == "metrika.order_upload_failed" and evs[0]["notified"] is True
    ej.mirror_tech_chat("🚫 Сообщение НЕ отправлено\nwhatsapp")
    assert drain() == []
    monkeypatch.setenv("ERROR_JOURNAL_ENABLED", "0")
    ej.mirror_tech_chat("Metrika: ошибка загрузки заказа 4512: 400")
    assert drain() == []


def test_catalog_event_by_key_once_and_notified():
    """Событие каталога разбирается по ключу и пишется один раз: зеркало техчата его
    пропускает. Отправляет каталог - notified; выключено в каталоге - журнал пишет сам."""
    text = "Metrika: ошибка загрузки заказа 4512 (сделка 36553383)"
    ej.on_decision("metrika_order_load_failed", text, text)
    ej.mirror_tech_chat(text)
    evs = drain()
    assert len(evs) == 1 and evs[0]["code"] == "metrika.order_upload_failed"
    assert evs[0]["notified"] is True and evs[0]["context"] == {"event": "metrika_order_load_failed"}
    ej.on_decision("metrika_order_load_failed", text, None)  # выключено в «Уведомлениях»
    assert [e["notified"] for e in drain()] == [False]
    # шаблон панели поменял текст - зеркало узнаёт и его
    ej.on_decision("queue_api_depth", "⚠️ amo_fix_fields: очередь API-пайплайна 40", "Очередь: 40 задач")
    ej.mirror_tech_chat("Очередь: 40 задач")
    assert [e["code"] for e in drain()] == ["queue.api_depth"]


def test_catalog_people_events_not_journaled():
    for key, text in (("wazzup_undelivered", "🚫 Сообщение НЕ отправлено"),
                      ("amgroup_duplicate", "Похоже, задвоилась сделка 1"),
                      ("ms_payment_over", "Переплата по сделке 1"),
                      ("missed_call", "Пропущенный звонок"),
                      ("waybill_create_failed", "Сделка 1: у контакта нет телефона"),
                      ("waybill_create_failed", "Сделка 1: СДЭК отклонил заказ: bad address")):
        ej.on_decision(key, text, text)
        ej.mirror_tech_chat(text)
    assert drain() == []


def test_waybill_server_failure_is_journaled():
    ej.on_decision("waybill_create_failed", "Сделка 36553383: СДЭК 502: Bad Gateway", "x")
    evs = drain()
    assert len(evs) == 1 and evs[0]["code"] == "cdek.request_failed"
    assert evs[0]["entity"] == {"type": "сделка", "id": "36553383"}


def test_log_handler_known_and_unknown(monkeypatch):
    lg = logging.getLogger("uvicorn.test_error_journal")
    h = ej.JournalHandler()
    lg.addHandler(h)
    try:
        lg.error("MS GET entity/customerorder/3f1c2a9e-1d2b-4c3d-9e8f-0a1b2c3d4e5f/positions → 404: {")
        lg.error("Что-то сломалось на сделке 123456")
        lg.warning("предупреждение не уходит")
        try:
            raise ValueError("boom")
        except ValueError:
            lg.exception("упало с исключением")
    finally:
        lg.removeHandler(h)
    evs = drain()
    assert len(evs) == 3
    assert evs[0]["code"] == "ms.order_positions_failed"
    assert evs[0]["fingerprint"].endswith("3f1c2a9e-1d2b-4c3d-9e8f-0a1b2c3d4e5f")
    assert evs[1]["code"] == "log.error" and evs[1]["fingerprint"].startswith("log:")
    assert "ValueError" in evs[2]["trace"]


def test_loop_spools_on_failure(monkeypatch):
    calls = []

    def bad(events):
        calls.append(len(events))
        raise OSError("down")

    monkeypatch.setattr(ej, "_post", bad)
    ej._write_spool([{"a": 1}])
    assert ej._read_spool() == [{"a": 1}]
    ej._write_spool([])
    assert ej._read_spool() == []


def test_report_never_raises_on_full_queue(monkeypatch):
    monkeypatch.setattr(ej, "_q", queue.Queue(maxsize=1))
    ej.report({"code": "x"})
    ej.report({"code": "y"})  # очередь полна - просто счётчик
    assert ej._stats["dropped"] >= 1
