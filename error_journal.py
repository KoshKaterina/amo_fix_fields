"""Журнал ошибок панели Team («Здоровье»): интеграция сообщает туда о своих сбоях.

Контракт - `POST {TEAM_PANEL_BASE_URL}/api/ingest/errors`, токен `TEAM_PANEL_INGEST_TOKEN`
(тот же, что у приёма Wazzup). Решения Кати 09.11.2026 -
dev-workspace-and-journal/features/zhurnal-oshibok/PREDLOZHENIE-2026-11-09.md.

Источники событий:

0. **Технические события каталога «Уведомлений»** - хук `alerts.decide` → `on_decision`,
   разбор по ключу события (`EVENT_RULES`). Уведомления для людей в журнал не идут.
1. **Остальное, что уходит в технический чат** (`telegram_bot.send_alert` без адресата
   или на TG_ALLOWED_CHAT_ID) - зеркалом, `mirror_tech_chat`. Тип события узнаём по началу
   текста (`TECH_RULES`), незнакомое ложится в `tech_chat.other`. Отправка в Телеграм НЕ
   меняется: интеграция пишет в чат сама, события идут с `notified`, журнал не дублирует.
2. **Строки ERROR/CRITICAL лога** - обработчик `install_log_handler` на логгере `uvicorn`.
   Это ровно то, о чём сейчас никто не узнаёт: 09.11 в логе за неделю нашлось 708 ошибок
   «МойСклад не отдаёт позиции заказа» по двум заказам, в чат не ушло ни одной.

Отправка идёт фоновым потоком пачками, приём и обработка вебхуков ею не задерживаются.
Панель недоступна - события копятся в файле на томе `var` и досылаются потом. Любая
ошибка здесь проглатывается: поломка журнала не должна ронять интеграцию.

Выключатель без правки кода: `ERROR_JOURNAL_ENABLED=0` в .env и пересоздание контейнера.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import queue
import re
import threading
import time
import urllib.request

SERVICE = "amo-fix-fields"
HOST = os.getenv("ERRORS_HOST", "85.193.91.169")
SPOOL = os.getenv("ERROR_JOURNAL_SPOOL", "var/error_journal_spool.jsonl")
BATCH = 50
FLUSH_EVERY_S = 5.0
QUEUE_MAX = 2000

_q: "queue.Queue[dict]" = queue.Queue(maxsize=QUEUE_MAX)
_thread: threading.Thread | None = None
_lock = threading.Lock()
_stats = {"queued": 0, "sent": 0, "dropped": 0, "failed_posts": 0, "last_error": None}


def _enabled() -> bool:
    return os.getenv("ERROR_JOURNAL_ENABLED", "1") != "0" and bool(_base_url()) and bool(_token())


def _base_url() -> str:
    return (os.getenv("TEAM_PANEL_BASE_URL") or "").strip().rstrip("/")


def _token() -> str:
    return (os.getenv("TEAM_PANEL_INGEST_TOKEN") or "").strip()


def stats() -> dict:
    return dict(_stats, backlog=_q.qsize())


# ─────────────────────────── очередь и отправка ───────────────────────────


def report(event: dict) -> None:
    """Положить событие в очередь. Никогда не бросает и не блокирует."""
    try:
        if not _enabled():
            return
        ev = {"v": 1, "service": SERVICE, "host": HOST, "env": "prod",
              "occurred_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), **event}
        _q.put_nowait(ev)
        _stats["queued"] += 1
        _ensure_thread()
    except queue.Full:
        _stats["dropped"] += 1
    except Exception:  # noqa: BLE001
        pass


def _ensure_thread() -> None:
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    with _lock:
        if _thread is None or not _thread.is_alive():
            _thread = threading.Thread(target=_loop, name="error-journal", daemon=True)
            _thread.start()


def _post(events: list[dict]) -> None:
    body = json.dumps({"events": events}, ensure_ascii=False, default=str).encode("utf-8")
    req = urllib.request.Request(
        _base_url() + "/api/ingest/errors", data=body, method="POST",
        headers={"Content-Type": "application/json", "X-Ingest-Token": _token(),
                 "User-Agent": "amo-fix-fields-error-journal/1"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        if r.status >= 300:
            raise RuntimeError(f"HTTP {r.status}")


def _read_spool() -> list[dict]:
    try:
        with open(SPOOL, encoding="utf-8") as f:
            return [json.loads(l) for l in f if l.strip()][-QUEUE_MAX:]
    except (OSError, ValueError):
        return []


def _write_spool(events: list[dict]) -> None:
    try:
        os.makedirs(os.path.dirname(SPOOL) or ".", exist_ok=True)
        with open(SPOOL, "w", encoding="utf-8") as f:
            for e in events[-QUEUE_MAX:]:
                f.write(json.dumps(e, ensure_ascii=False, default=str) + "\n")
    except OSError:
        pass


def _loop() -> None:
    pending = _read_spool()
    while True:
        try:
            deadline = time.monotonic() + FLUSH_EVERY_S
            while len(pending) < BATCH and time.monotonic() < deadline:
                try:
                    pending.append(_q.get(timeout=max(0.1, deadline - time.monotonic())))
                except queue.Empty:
                    break
            if not pending:
                continue
            chunk = pending[:BATCH]
            try:
                _post(chunk)
                _stats["sent"] += len(chunk)
                pending = pending[BATCH:]
                if not pending:
                    _write_spool([])
            except Exception as exc:  # noqa: BLE001 - панель недоступна: копим и ждём
                _stats["failed_posts"] += 1
                _stats["last_error"] = type(exc).__name__
                _write_spool(pending)
                time.sleep(30)
        except Exception:  # noqa: BLE001
            time.sleep(5)


# ─────────────────────────── техчат → журнал ───────────────────────────

# Разведение с «Уведомлениями» (Катя 09.11.2026). В журнал идёт только техническое: сбой
# сервера или запроса, наша настройка, справка для техспециалиста (сводки доставки,
# автопочинка). Работа для людей - клиент не получил сообщение, письмо клиента, дубль
# сделки, разобрать заказ руками, данные в сделке для накладной - это уведомление: оно
# уходит как раньше, в журнал не пишется (критичность None ниже).
# У события один отправитель в чат: всё, что журнал узнаёт отсюда, интеграция уже
# отправила сама, поэтому события идут с `notified` - журнал записывает и молчит.

# Начало текста → (компонент, критичность, код, название). Перечень снят с реального
# техчата 10.10-09.11.2026 (alert_sent.jsonl). Первое совпадение выигрывает. Работает для
# сообщений, отправленных мимо каталога «Уведомлений»; события каталога разбираются по
# ключу события (`EVENT_RULES`).
TECH_RULES: list[tuple[str, str, str | None, str, str]] = [
    ("Metrika: ошибка загрузки заказа", "metrika_sync", "error", "metrika.order_upload_failed", "Метрика не приняла заказ"),
    ("🚫 Сообщение НЕ отправлено", "wazzup_delivery", None, "wazzup.undelivered", "Клиент не получил сообщение (Wazzup)"),
    ("📊 <b>Доставка Wazzup за сутки", "wazzup_delivery", "info", "wazzup.daily_summary", "Сводка: доставка Wazzup за сутки"),
    ("⏱ <b>Отправлено, но в Wazzup не помечено", "wazzup_delivery", "info", "wazzup.not_marked_delivered", "Сводка: отправлено, но не помечено доставленным"),
    ("💰 Бюджет разошёлся", "budget_mismatch_watch", "info", "budget.auto_aligned", "Бюджет сделки разошёлся с заказом и выровнен автоматически"),
    ("⚠️ amo_fix_fields: очередь API-пайплайна", "queue_manager", "warning", "queue.api_depth", "Очередь запросов к amoCRM выше порога"),
    ("⚠️ amo_fix_fields: задача", "queue_manager", "warning", "queue.task_waited", "Задача долго ждала в очереди"),
    ("⚠️ amo_fix_fields: очередь", "queue_manager", "warning", "queue.lane_depth", "Дорожка очереди переполнена"),
    ("СДЭК отклонил заказ", "waybill_service", None, "cdek.order_rejected", "СДЭК отклонил заказ на накладную"),
    ("не определён тариф СДЭК", "waybill_service", None, "cdek.tariff_unknown", "Не определён тариф СДЭК"),
    ("не распознан код ПВЗ", "waybill_service", None, "cdek.pvz_unknown", "Не распознан код пункта выдачи СДЭК"),
    ("📬 Письмо клиента", "mail_watch", None, "mail.client_letter", "Письмо клиента по закрытой сделке"),
    ("Woo: ошибка", "woo_status_sync", "error", "woo.request_failed", "Ошибка запроса к сайту"),
    ("Похоже, задвоилась сделка", "amgroup_duplicate_watch", None, "amgroup.duplicate", "Похоже, задвоилась сделка по заказу"),
    ("🧪 Сухой режим", "amgroup_contact_heal", "info", "amgroup.dry_run", "Сухой прогон: в amo ничего не записано"),
    ("GetCourse", "getcourse_service", None, "getcourse.manual_review", "Заказ GetCourse нужно разобрать руками"),
    ("Заявка с сайта не создалась", "site_form_service", "critical", "site_form.lead_not_created", "Заявка с сайта не создалась в amoCRM"),
    ("⚠️ Заказ с сайта не доехал", "order_watchdog", "critical", "order.site_order_lost", "Заказ с сайта не доехал до МойСклада"),
]

# Событие каталога «Уведомлений» → (компонент, критичность, код, название). Только
# технические события; любое другое событие каталога - уведомление для людей и в журнал
# не идёт. Коды совпадают с прежними правилами по тексту, чтобы история не разъехалась.
EVENT_RULES: dict[str, tuple[str, str, str, str]] = {
    "metrika_order_load_failed": ("metrika_sync", "error", "metrika.order_upload_failed", "Метрика не приняла заказ"),
    "metrika_sync_counters_failed": ("metrika_sync", "error", "metrika.counters_failed", "Метрика: не удалось получить счётчики"),
    "metrika_sync_counter_ambiguous": ("metrika_sync", "warning", "metrika.counter_ambiguous", "Метрика: счётчиков несколько"),
    "queue_api_depth": ("queue_manager", "warning", "queue.api_depth", "Очередь запросов к amoCRM выше порога"),
    "queue_lane_depth": ("queue_manager", "warning", "queue.lane_depth", "Дорожка очереди переполнена"),
    "queue_task_waited": ("queue_manager", "warning", "queue.task_waited", "Задача долго ждала в очереди"),
    "woo_status_sync_error": ("woo_status_sync", "error", "woo.request_failed", "Ошибка запроса к сайту"),
    "order_watchdog_digest": ("order_watchdog", "critical", "order.site_order_lost", "Заказ с сайта не доехал до МойСклада"),
    "cdek_sync_no_pipeline": ("cdek_status_sync", "error", "cdek.sync_no_pipeline", "Синк СДЭК: воронка «Офис» не определена"),
    "cdek_sync_stages_missing": ("cdek_status_sync", "error", "cdek.sync_stages_missing", "Синк СДЭК: не найдены этапы"),
    "cdek_sync_webhook_failed": ("cdek_status_sync", "error", "cdek.webhook_subscribe_failed", "Синк СДЭК: не удалось подписаться на вебхуки"),
    "office_transfer_targets_missing": ("office_transfer", "error", "office_transfer.targets_missing", "Перенос в Офис: целевые этапы не найдены"),
    "office_transfer_no_since": ("office_transfer", "error", "office_transfer.no_since", "Перенос в Офис: сверка не запущена"),
    "lead_distribution_no_since": ("lead_distribution", "error", "lead_distribution.no_since", "Распределение: сверка не запущена"),
    "ms_payment_failed": ("ms_payment_flow", "error", "ms.payment_failed", "Платёж не записан в МойСклад: сбой"),
    "autopilot_failure": ("autopilot", "error", "autopilot.failure", "Авто-режим: поломка"),
    "waybill_amo_not_updated": ("waybill_service", "error", "waybill.amo_not_updated", "Накладная СДЭК создана, amoCRM не обновлён"),
    "waybill_track_not_restored": ("waybill_service", "error", "waybill.track_not_restored", "Трек-номер пропал и не восстановился"),
    "ozon_invoice_stages_missing": ("ozon_invoice", "error", "ozon_invoice.stages_missing", "Счета Ozon: этапы оплаты не найдены"),
}

# «Накладная СДЭК не создалась» бывает двух родов. Нет телефона, ПВЗ, город, тариф, СДЭК
# отклонил данные - это поправить сделку, уведомление. Ответ СДЭК с кодом ошибки, пустой
# ответ, исключение, amoCRM не отдал контакт - сбой, журнал.
_WAYBILL_SERVER = re.compile(
    r"СДЭК \d{3}|СДЭК не вернул UUID|СДЭК неожиданная ошибка|не удалось загрузить контакт"
    r"|не удалось проверить город")

_TAG = re.compile(r"<[^>]+>")
_DEAL = re.compile(r"[Сс]делк[аи]\D{0,3}(\d{6,})")


def _first_line(text: str) -> str:
    for line in (text or "").splitlines():
        line = _TAG.sub("", line).strip()
        if line:
            return line
    return ""


def classify_tech(text: str) -> dict | None:
    """Текст сообщения техчата → поля события; None - это уведомление для людей, в журнал
    не идёт. Незнакомое - `tech_chat.other`, склейка по первой строке без чисел, чтобы
    новые типы сообщений тоже собирались в группы."""
    head = _first_line(text)
    raw_head = (text or "").lstrip()
    for prefix, component, severity, code, title in TECH_RULES:
        if raw_head.startswith(prefix) or prefix in head:
            if severity is None:
                return None
            ev = {"component": component, "severity": severity, "code": code, "title": title}
            break
    else:
        norm = re.sub(r"\d+", "N", head)[:120]
        ev = {"component": "tech_chat", "severity": "warning", "code": "tech_chat.other",
              "title": head[:200] or "Сообщение в технический чат",
              "fingerprint": "tech_chat:" + hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]}
    body = _TAG.sub("", text or "").strip()
    ev["message"] = body[:2000]
    m = _DEAL.search(body)
    if m:
        ev["entity"] = {"type": "сделка", "id": m.group(1)}
        if ev["code"].startswith("cdek."):
            ev["fingerprint"] = f"{ev['code']}:{m.group(1)}"
    if ev["code"] in ("wazzup.daily_summary", "wazzup.not_marked_delivered"):
        ev["fingerprint"] = f"{ev['code']}:{datetime.date.today().isoformat()}"
    return ev


def classify_event(event_key: str, text: str) -> dict | None:
    """Событие каталога «Уведомлений» → поля события журнала; None - уведомление для людей."""
    rule = EVENT_RULES.get(event_key)
    if rule is None and event_key == "waybill_create_failed" and _WAYBILL_SERVER.search(text or ""):
        rule = ("waybill_service", "error", "cdek.request_failed",
                "Накладная СДЭК не создалась: запрос к СДЭК или amoCRM не прошёл")
    if rule is None:
        return None
    component, severity, code, title = rule
    body = _TAG.sub("", text or "").strip()
    ev = {"component": component, "severity": severity, "code": code, "title": title,
          "message": body[:2000], "context": {"event": event_key}}
    m = _DEAL.search(body)
    if m:
        ev["entity"] = {"type": "сделка", "id": m.group(1)}
    return ev


# Тексты, которые уже разобраны по ключу события в `alerts.decide`: когда они дойдут до
# техчата, зеркало их пропускает, иначе событие записалось бы дважды. Хранятся по хешу
# недолго - между решением и отправкой проходят секунды.
_DECIDED_TTL_S = 900.0
_DECIDED_MAX = 1000
_decided: dict[str, float] = {}


def _text_key(text: str) -> str:
    return hashlib.sha1((text or "").strip().encode("utf-8")).hexdigest()


def _mark_decided(*texts: str | None) -> None:
    now = time.monotonic()
    with _lock:
        if len(_decided) >= _DECIDED_MAX:
            for k, t in list(_decided.items()):
                if now - t > _DECIDED_TTL_S or len(_decided) >= _DECIDED_MAX:
                    _decided.pop(k, None)
        for t in texts:
            if t:
                _decided[_text_key(t)] = now


def _take_decided(text: str) -> bool:
    with _lock:
        t = _decided.pop(_text_key(text), None)
    return t is not None and time.monotonic() - t <= _DECIDED_TTL_S


def on_decision(event_key: str, legacy_text: str, sent_text: str | None) -> None:
    """Хук `alerts.decide`: событие каталога решено. Техническое - в журнал; `notified`, если
    каталог его отправляет (`sent_text` не None): тогда в чат пишет интеграция, журнал молчит.
    Выключено в «Уведомлениях» - журнал записывает без `notified` и сам сообщает в техчат:
    так отправка переезжает в журнал одним выключателем. Никогда не бросает."""
    try:
        _mark_decided(legacy_text, sent_text)
        ev = classify_event(event_key, legacy_text)
        if ev is not None:
            ev["notified"] = sent_text is not None
            report(ev)
    except Exception:  # noqa: BLE001
        pass


def mirror_tech_chat(text: str) -> None:
    """Сообщение ушло в техчат. Событие каталога уже записано в `on_decision`; остальное
    разбираем по тексту. Интеграция отправила его сама - `notified`."""
    try:
        if _take_decided(text):
            return
        ev = classify_tech(text)
        if ev is not None:
            ev["notified"] = True
            report(ev)
    except Exception:  # noqa: BLE001
        pass


# ─────────────────────────── лог → журнал ───────────────────────────

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_LOG_RULES: list[tuple[re.Pattern, str, str, str]] = [
    (re.compile(r"MS GET entity/customerorder/[0-9a-f-]{36}/positions"), "ms_client",
     "ms.order_positions_failed", "МойСклад не отдаёт позиции заказа"),
    (re.compile(r"MS POST entity/paymentin"), "ms_payment_flow", "ms.paymentin_failed", "МойСклад не создал входящий платёж"),
    (re.compile(r"MS POST entity/demand"), "ms_client", "ms.demand_failed", "МойСклад не создал отгрузку"),
    (re.compile(r"amgroup_shipment: МойСклад не создал отгрузку"), "amgroup_shipment", "amgroup.demand_not_created", "Отгрузка по заказу не создалась"),
    (re.compile(r"amgroup_shipment: сделка \d+ на целевом этапе, но пусто"), "amgroup_shipment", "amgroup.no_order_id", "Сделка на этапе отгрузки, но пусто поле «ID Заказа»"),
    (re.compile(r"site_form\[[^\]]*\]: заявка \S+ . сбой доставки"), "site_form_service", "site_form.delivery_failed", "Заявка с формы сайта не доставлена в amoCRM"),
    (re.compile(r"accept_unsorted"), "amo_unsorted", "amo.accept_unsorted_failed", "amoCRM не принял заявку из «Неразобранного»"),
    (re.compile(r"team_panel_client: сбой запроса"), "team_panel_client", "panel.request_failed", "Интеграция не дозвалась до панели"),
]


class JournalHandler(logging.Handler):
    """ERROR и выше → журнал. Известные строки - своим кодом, остальные - `log.error` со
    склейкой по тексту без чисел и uuid."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if record.name.startswith(("error_journal", "httpx", "httpcore", "urllib3")):
                return
            msg = record.getMessage()
            if "/api/ingest/errors" in msg:
                return
            sev = "critical" if record.levelno >= logging.CRITICAL else "error"
            ev: dict = {"severity": sev, "message": msg[:2000]}
            for pat, component, code, title in _LOG_RULES:
                if pat.search(msg):
                    ev.update(component=component, code=code, title=title)
                    if code == "ms.order_positions_failed":
                        u = _UUID.search(msg)
                        if u:
                            ev["fingerprint"] = f"{code}:{u.group(0)}"
                            ev["entity"] = {"type": "заказ МойСклада", "id": u.group(0)}
                    break
            else:
                first = msg.splitlines()[0] if msg else record.name
                norm = _UUID.sub("U", re.sub(r"\d+", "N", first))[:160]
                ev.update(component=(record.module or "log")[:64], code="log.error",
                          title=first[:200], fingerprint="log:" + hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16])
            if record.exc_info:
                ev["trace"] = "".join(logging.Formatter().formatException(record.exc_info))[-8000:]
            report(ev)
        except Exception:  # noqa: BLE001
            pass


_handler: JournalHandler | None = None


def install_log_handler(logger_names: tuple[str, ...] = ("uvicorn",)) -> None:
    """Подключить обработчик к логгерам интеграции. Повторный вызов безопасен.
    Только `uvicorn`: его дети (`uvicorn.error`) всплывают в него сами, второй обработчик
    на ребёнке задвоил бы каждую строку."""
    global _handler
    if not _enabled():
        return
    if _handler is None:
        _handler = JournalHandler()
    for name in logger_names:
        lg = logging.getLogger(name)
        if _handler not in lg.handlers:
            lg.addHandler(_handler)
