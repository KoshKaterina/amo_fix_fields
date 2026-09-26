import datetime
import json
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from starlette.status import HTTP_200_OK

import amgroup_duplicate_watch
import amgroup_fallback
import amgroup_lead_builder
import academy_lead_alert
import academy_invite_delivery
import academy_intent_alert
import academy_assignment
import academy_consent_stamp
import academy_bothelp_upsert
import amgroup_shipment
import amo_service
import cdek_client
import cdek_status_sync
import dup_autoclose
import jivo_service
import lead_distribution
import lead_distribution_profiles_client
import alert_settings_client
import metrika_sync
import migration_freeze
import autopilot
import new_lead_watch
import ms_client
import office_transfer
import order_note
import preorder_lead_name
import order_watchdog
import ozon_invoice
import reserve_service
import showroom_alert
import showroom_store
import showroom_tag
import site_form_service
import team_panel_client
import telegram_bot
import telegram_contact
import uis_missed_call
import unmiss_tag
import urgency_tag
import wazzup_sla
import wazzup_forward
import wazzup_delivery
import woo_status_sync
from api import init_api_pipeline, shutdown_api_pipeline
from help_function import (
    get_nested,
    parse_the_cart_field,
    parse_the_cart_field_2,
)
from lead_distribution_api import router as lead_distribution_router
from queue_manager import (
    enqueue_invoice,
    enqueue_jivo,
    enqueue_lead_distribution,
    enqueue_new,
    enqueue_office_transfer,
    enqueue_waybill,
    init_queue,
    queue_stats,
    shutdown_queue,
)
from waybill_config import (
    PIPELINE_ACADEMY,
    STATUS_ACADEMY_RECORDED_PRACTICUM,
    OFFICE_TRANSFER_ENABLED,
    STATUS_CLOSED_LOST,
    STATUS_CREATE_WAYBILL,
    STATUS_SUCCESS,
    UIS_WEBHOOK_SECRET,
    WAZZUP_WEBHOOK_SECRET,
    looks_like_uuid,
)


@asynccontextmanager
async def lifespan(app):
    msk = datetime.timezone(datetime.timedelta(hours=3))

    class MskFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.datetime.fromtimestamp(record.created, tz=msk)
            return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

    formatter = MskFormatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        for handler in logging.getLogger(name).handlers:
            handler.setFormatter(formatter)

    init_api_pipeline()
    init_queue()
    await amo_service.warm_pipeline_cache()
    await cdek_client.init()
    await telegram_bot.init_telegram_bot()
    await cdek_status_sync.init()
    await metrika_sync.init()
    await woo_status_sync.init()
    # Клиент МойСклада поднимаем здесь: его читает ozon_invoice (суммы заказа для
    # СБП-счёта) и reserve_service (позиции заказа). Раньше клиент вставал внутри
    # синка Фулфилмента — когда тот выключили 05.08, счета молча перестали
    # создаваться. Контур ФФ удалён, клиент остался.
    ms_client.init()
    await reserve_service.init()
    ozon_invoice.init()
    await wazzup_sla.init()
    await wazzup_forward.init()
    await wazzup_delivery.init()
    await office_transfer.init()
    lead_distribution_profiles_client.start()
    await lead_distribution.init()
    team_panel_client.start()
    # Настройки уведомлений из панели: без ALERT_SETTINGS_FROM_PANEL ничего не опрашивает.
    alert_settings_client.start()
    await showroom_store.init()
    await order_watchdog.init()
    await uis_missed_call.init()
    await new_lead_watch.init()
    # Формы сайта: без SITE_FORM_ENABLED роут отвечает 404 и ничего не делает.
    await site_form_service.init()
    # Протез amgroup (03.09.2026): их интеграция МойСклад -> amoCRM встала 02.09.
    # Сборку сделки подключаем точкой расширения, чтобы протез искал заказы, а
    # создавал их отдельный модуль - оба выключены флагами по умолчанию.
    amgroup_fallback.create_lead_for_order = amgroup_lead_builder.create_lead_for_order
    await amgroup_fallback.init()
    await amgroup_shipment.init()
    await amgroup_duplicate_watch.init()
    # Авто-режим ОП розница: ведёт сделку по этапам вместо менеджера. Сам первым делом
    # смотрит флаг AUTOPILOT_ENABLED и без него не поднимает ни хранилища, ни опроса
    # панели, ни фонового цикла.
    await autopilot.init()
    academy_invite_delivery.start()
    yield
    # Первым — досверка хвостов unmiss (спящие дебаунс-задачи), пока API-пайплайн жив.
    await wazzup_sla.shutdown()
    await wazzup_forward.shutdown()
    await wazzup_delivery.shutdown()
    await unmiss_tag.shutdown()
    await showroom_store.shutdown()
    await order_watchdog.shutdown()
    await uis_missed_call.shutdown()
    await new_lead_watch.shutdown()
    await site_form_service.shutdown()
    await amgroup_fallback.shutdown()
    await amgroup_shipment.shutdown()
    await amgroup_duplicate_watch.shutdown()
    await autopilot.shutdown()
    await academy_invite_delivery.stop()
    await office_transfer.stop_reconcile()
    await lead_distribution.stop_reconcile()
    await alert_settings_client.stop()
    await team_panel_client.stop()
    await lead_distribution_profiles_client.stop()
    await ozon_invoice.aclose()
    await reserve_service.shutdown()
    await ms_client.aclose()
    await woo_status_sync.shutdown()
    await metrika_sync.shutdown()
    await cdek_status_sync.shutdown()
    await telegram_bot.shutdown_telegram_bot()
    await cdek_client.aclose()
    await shutdown_queue()
    await shutdown_api_pipeline()


app = FastAPI(lifespan=lifespan)
app.include_router(lead_distribution_router)

logger = logging.getLogger("uvicorn")


@app.get("/")
async def health():
    """Здоровье + срез очередей (глубина дорожек, api_queue, последнее ожидание) —
    чтобы «очередь огромная» была проверяема одним запросом, без чтения логов.
    Блок telegram - состояние контура уведомлений: молчащий бот внутри живого
    контейнера иначе неотличим от тишины по отсутствию событий (инцидент 28.08.2026,
    сутки без алертов при зелёном контейнере)."""
    return {"status": "ok", "telegram": telegram_bot.telegram_health(), **queue_stats()}


def insert_nested(data, keys, value):
    cur = data
    for key in keys[:-1]:
        if key not in cur:
            cur[key] = {}
        cur = cur[key]
    cur[keys[-1]] = value


def contact_changed_field_ids(nested: dict) -> set[int]:
    """field_id из payload contacts.add/update amoCRM (индексы приходят строками)."""
    out: set[int] = set()
    contacts = nested.get("contacts") or {}
    for event in ("add", "update"):
        for contact in (contacts.get(event) or {}).values():
            fields = (contact or {}).get("custom_fields") or {}
            for field in fields.values():
                try:
                    out.add(int((field or {}).get("id")))
                except (TypeError, ValueError):
                    continue
    return out


@app.get("/barcode/{ident}")
async def barcode(ident: str):
    """Проксирует штрихкод СДЭК: принимает cdek_number или UUID заказа,
    ходит в СДЭК с токеном и отдаёт PDF. Ссылку кладём в примечание сделки."""
    try:
        if looks_like_uuid(ident):
            uuid = ident
        else:
            uuid = await cdek_client.find_uuid_by_cdek_number(ident)
        if not uuid:
            return Response("Заказ СДЭК не найден", status_code=404)
        pdf = await cdek_client.get_barcodes_batch_pdf([uuid])
    except cdek_client.CdekError as exc:
        logger.warning("barcode %s: ошибка СДЭК: %s", ident, exc)
        return Response(f"Штрихкод недоступен: {exc}", status_code=502)
    except Exception:
        logger.exception("barcode %s: неожиданная ошибка", ident)
        return Response("Внутренняя ошибка", status_code=500)
    return Response(
        pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="barcode_{ident}.pdf"'},
    )


@app.post("/cdek_status")
async def cdek_status(request: Request):
    """Вебхук СДЭК ORDER_STATUS. Отвечаем 200 всегда и быстро —
    обработка идёт через очередь с низшим приоритетом."""
    try:
        payload = await request.json()
    except Exception:
        logger.warning("CDEK webhook: невалидный JSON")
        return {"ok": False}
    try:
        cdek_status_sync.handle_webhook_event(payload)
    except Exception:
        logger.exception("CDEK webhook: ошибка обработки события")
    return {"ok": True}


@app.post("/jivo/{token}")
async def jivo_webhook(token: str, request: Request):
    """Вебхук Jivo (канал website → Integration Settings for Developers →
    Webhooks API). Без сегмента сайта → дефолтный сайт (Sunscrypt)."""
    return await _handle_jivo(token, None, request)


@app.post("/jivo/{token}/{site}")
async def jivo_webhook_site(token: str, site: str, request: Request):
    """Тот же вебхук с явным сайтом-источником в пути (/jivo/<secret>/<site>).
    У каждого канала Jivo (Sunscrypt, Tangemshop) — свой URL → сделка метится
    источником. Воронка и операторы общие."""
    return await _handle_jivo(token, site, request)


async def _handle_jivo(token: str, site: str | None, request: Request):
    """На завершение чата с контактом создаём контакт+сделку+примечание в amo —
    замена связки через Albato. Секрет в пути заменяет отсутствующую у Jivo
    подпись. Отвечаем быстро {"result":"ok"} (этого Jivo и ждёт), реальная
    работа — фоном через очередь."""
    if not jivo_service.secret_ok(token):
        logger.warning("Jivo webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)

    try:
        event = await request.json()
    except Exception:
        logger.warning("Jivo webhook: невалидный JSON")
        return {"result": "ok"}

    jivo_service.log_payload(event)
    event_name = event.get("event_name") if isinstance(event, dict) else None

    if not jivo_service.is_enabled():
        logger.info("Jivo webhook: получено '%s', обработка выключена (JIVO_WEBHOOK_ENABLED)", event_name)
        return {"result": "ok"}

    parsed = jivo_service.parse_event(event, site)
    if parsed is None:
        logger.info("Jivo webhook: '%s' пропущено (не наш тип события / нет контакта)", event_name)
        return {"result": "ok"}

    logger.info("Jivo webhook: '%s' принято, сайт=%s", event_name, parsed.get("site"))
    enqueue_jivo(parsed)
    return {"result": "ok"}


@app.get("/uis/{secret}")
async def uis_missed_call_webhook(secret: str, request: Request):
    """UIS HTTP-уведомление «Потерянный звонок» → алерт в ТГ отделу продаж.
    Секрет в пути (простая защита). Отвечаем 200 сразу — UIS ждёт быстрый ответ
    (иначе ретраит); реальная работа (поиск сделки + отправка) идёт в фоне."""
    if not UIS_WEBHOOK_SECRET or secret != UIS_WEBHOOK_SECRET:
        logger.warning("UIS webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)
    uis_missed_call.notify_bg(dict(request.query_params))
    return {"ok": True}


@app.post("/wazzup/{secret}")
async def wazzup_webhook(secret: str, request: Request):
    """Вебхук Wazzup (messages/statuses) → три независимых потребителя:
    SLA-таймер «клиент без ответа N мин», контроль доставки «сообщение не дошло»
    и пересылка текстов в панель. Секрет в пути — простая защита. Отвечаем 200
    сразу и всегда (Wazzup при ошибке/таймауте ретраит и может отключить вебхук).
    При установке подписки Wazzup шлёт тестовый запрос — на него тоже 200."""
    if not WAZZUP_WEBHOOK_SECRET or secret != WAZZUP_WEBHOOK_SECRET:
        logger.warning("Wazzup webhook: неверный секрет в пути")
        return Response("forbidden", status_code=403)
    try:
        payload = await request.json()
    except Exception:
        logger.warning("Wazzup webhook: невалидный JSON")
        return {"ok": True}
    if isinstance(payload, dict) and payload.get("test") is True:
        logger.info("Wazzup webhook: тестовый запрос — отвечаю 200")
        return {"ok": True}
    try:
        wazzup_sla.handle_webhook(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка обработки")
    # Контроль доставки (statuses[] + status в messages[]) — отдельно от SLA:
    # тот про молчание менеджера, этот про то, что сообщение не дошло до клиента.
    try:
        wazzup_delivery.handle_webhook(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка контроля доставки")
    try:
        academy_invite_delivery.record_webhook(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка статуса приглашения Академии")
    # Пересылка текстов в панель (wazzup_message) — независимо от остальных:
    # упавший таймер не должен терять сообщение (источник невосполним).
    try:
        wazzup_forward.enqueue(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка пересылки в панель")
    # Авто-режим — четвёртый потребитель. Ему из этого вебхука нужны две вещи: статус
    # доставки шаблона и текст ответа клиента. Читать переписку из amoCRM нельзя, все пути
    # к сообщениям в API закрыты, так что другого источника у робота нет.
    try:
        autopilot.on_wazzup(payload)
    except Exception:
        logger.exception("Wazzup webhook: ошибка авто-режима")
    return {"ok": True}


@app.post("/site_form")
async def site_form(request: Request):
    """Контактные формы сайта → сделка в amo (карта SITE_FORM_MAP). Авторизация —
    заголовок X-Api-Key: секрет не попадает ни в URL, ни в access-логи nginx;
    браузер посетителя сюда не ходит вовсе (постит сервер WP со своей стороны).

    Схема 1 (WPCode-сниппет, копии старых форм): 200 сразу, работа фоном.
    Схема 2 (плагин sun-contact-forms): 200 только когда заявка легла в очередь;
    422/429/503 — сайт покажет человеку «Не получилось отправить» и сохранит
    введённое. Сделка создаётся фоном с повторами (site_form_service)."""
    if not site_form_service.is_enabled():
        return Response("disabled", status_code=404)
    if not site_form_service.secret_ok(request.headers.get("X-Api-Key", "")):
        logger.warning("site_form: неверный X-Api-Key")
        return Response("forbidden", status_code=403)
    try:
        payload = await request.json()
    except Exception:
        logger.warning("site_form: невалидный JSON")
        return {"ok": False}
    if not isinstance(payload, dict):
        logger.warning("site_form: тело не объект")
        return {"ok": False}
    if payload.get("schema") == 2:
        status_code, body = await site_form_service.accept_v2(payload)
        return JSONResponse(body, status_code=status_code)
    fwd = request.headers.get("X-Forwarded-For", "")
    ip = fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "")
    if not site_form_service.allow_ip(ip):
        logger.warning("site_form: rate limit для %s — заявка отброшена", ip)
        return {"ok": True}
    site_form_service.handle_bg(payload, ip)
    return {"ok": True}


@app.post("/ozon_notify")
async def ozon_notify(request: Request):
    """Вебхук Ozon Pay о статусе платежа (notificationUrl наших СБП-счетов из
    amo, MAG-285 этап 2). Аутентификация — подпись requestSign в теле (обе
    боевые формулы плагина сайта); невалидная подпись просто игнорируется.
    Отвечаем 200 быстро, обработка (перевод сделки в «Оплата получена») — фоном."""
    try:
        payload = await request.json()
    except Exception:
        logger.warning("ozon_notify: невалидный JSON")
        return {"ok": True}
    try:
        ozon_invoice.handle_notification_bg(payload)
    except Exception:
        logger.exception("ozon_notify: ошибка постановки обработки")
    return {"ok": True}


@app.post("/talk_probe")
async def talk_probe(request: Request):
    """ВРЕМЕННЫЙ логгер вебхука amo add_talk/update_talk (эксперимент 31.07.2026:
    ловится ли кнопка «Не требует ответа»). Пишет тело в лог, отвечает 200.
    Снести вместе с подпиской вебхука 48238698, когда эксперимент закончится."""
    try:
        raw = (await request.body()).decode("utf-8", "replace")
        logger.info("TALK_PROBE raw: %s", raw[:3000])
        form = await request.form()
        nested: dict = {}
        for raw_key, value in form.items():
            keys = re.findall(r"([^\[\]]+)", raw_key)
            insert_nested(nested, keys, value)
        logger.info("TALK_PROBE parsed: %s", json.dumps(nested, ensure_ascii=False)[:3000])
    except Exception:
        logger.exception("TALK_PROBE: ошибка разбора (отвечаем 200)")
    return {"ok": True}


@app.post("/contact_change")
async def contact_change(request: Request):
    """Изменение контакта amoCRM — триггер одноразовой ссылки Академии.

    Отвечаем сразу; чтение контакта/сделок и Telegram API работают в фоне.
    Остальные интеграции контакта этот маршрут не затрагивает.
    """
    form = await request.form()
    nested = {}
    for raw_key, value in form.items():
        keys = re.findall(r"([^\[\]]+)", raw_key)
        insert_nested(nested, keys, value)
    contact_id = await get_nested(nested, ["contacts", "update", "0", "id"])
    if contact_id is None:
        contact_id = await get_nested(nested, ["contacts", "add", "0", "id"])
    changed_field_ids = contact_changed_field_ids(nested)
    academy_intent_alert.on_contact_change(contact_id, changed_field_ids)
    academy_consent_stamp.on_contact_change(contact_id, changed_field_ids)
    return {"status": "ok"}


@app.post("/bothelp/academy/{secret}")
async def bothelp_academy(secret: str, request: Request):
    """Полный профиль подписчика BotHelp -> одна карточка Академии."""
    if not academy_bothelp_upsert.authorized(secret):
        raise HTTPException(status_code=404, detail="Not found")
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    result = await academy_bothelp_upsert.process(payload if isinstance(payload, dict) else {})
    if not result.get("ok"):
        logger.error("ACADEMY_BOTHELP_UPSERT failed: %s", result)
        raise HTTPException(status_code=503, detail=result.get("reason", "upsert_failed"))
    return result


@app.post("/lead_change")
async def lead_change(request: Request):
    form = await request.form()

    nested = {}
    for raw_key, value in form.items():
        keys = re.findall(r"([^\[\]]+)", raw_key)
        insert_nested(nested, keys, value)

    goods = None
    delivery_type = None
    delivery_address = None
    lead_name = None
    promo_type = None
    comment = None

    lead_add_id = await get_nested(nested, ["leads", "add", "0", "id"])
    lead_id = await get_nested(nested, ["leads", "update", "0", "id"])
    if lead_id is None:
        lead_id = lead_add_id

    modified_by = await get_nested(nested, ["leads", "update", "0", "updated_by"])
    logger.info(f"lead_id: {lead_id}, modified_by: {modified_by}")

    # Массовый прогон миграции: гасим ТОЛЬКО его собственный поток (воронка-источник
    # и заходы в 142/143 основной), не читая сделку. Накладные, счета и новые заказы
    # идут дальше штатно. Разбор воронки/этапа — ниже, поэтому берём их тут же.
    _bulk_pipe = await get_nested(nested, ["leads", "update", "0", "pipeline_id"])
    if _bulk_pipe is None:
        _bulk_pipe = await get_nested(nested, ["leads", "add", "0", "pipeline_id"])
    _bulk_status = await get_nested(nested, ["leads", "update", "0", "status_id"])
    if _bulk_status is None:
        _bulk_status = await get_nested(nested, ["leads", "add", "0", "status_id"])
    if migration_freeze.bulk_skip(_bulk_pipe, _bulk_status):
        logger.info("lead_change: сделка %s — массовый прогон миграции, пропускаем", lead_id)
        return {"status": "skipped-migration-bulk"}

    # Миграция воронок (03.08.2026): сделка с тегом «перенесено из старой
    # воронки» в окне переноса — не наша забота. Выходим ДО всех обработчиков,
    # иначе перенос старой сделки в 142 читается как свежая продажа: перенос в
    # Офис/ФФ, конверсия в Метрику, Woo-заказ в completed. Вне окна проверка
    # стоит ноль (сравнение времени), сделку не дочитываем.
    if lead_id is not None and await migration_freeze.skip(lead_id, "lead_change"):
        return {"status": "skipped-migration-freeze"}

    # Автоснятие «пропущенный» при дозвоне: реконсиляция по дочитыванию (amo не шлёт
    # теги в вебхук). На любом изменении сделки в фоне сверяем теги: если есть
    # «Успешный звонок» И «пропущенный» — снимаем «пропущенный» (сделка + контакты).
    unmiss_tag.maybe_remove_bg(lead_id)

    # Контакты из заказа примечанием в ленту (костыль, см. order_note): amgroup
    # затирает email в карточке контрагента МойСклада через секунды после того,
    # как его туда записала woocommerce-sklad. Только на СОЗДАНИИ сделки — на
    # обычных изменениях писать нечего, примечание уже стоит. Выключено флагом
    # ORDER_NOTE_ENABLED по умолчанию.
    if await get_nested(nested, ["leads", "add", "0", "id"]) is not None:
        order_note.post_bg(lead_id)
        # Ник Телеграма из контрагента МС в контакт («TelegramUsername_WZ», только в пустое)
        telegram_contact.post_bg(lead_id)
        # Заявка с формы предзаказа: имя сделки «Заказ №<id>» вместо имени клиента
        # (Катя 21.09.2026). Модуль сам проверяет флаг, источник и тип заявки.
        preorder_lead_name.rename_bg(lead_id)

    status_update = await get_nested(nested, ["leads", "update", "0", "status_id"])
    status_add = await get_nested(nested, ["leads", "add", "0", "status_id"])
    incoming_status = status_update if status_update is not None else status_add
    if lead_id is not None and incoming_status is not None and str(incoming_status) == str(STATUS_CREATE_WAYBILL):
        logger.info("Lead %s entered STATUS_CREATE_WAYBILL — enqueue waybill", lead_id)
        enqueue_waybill(lead_id, source="webhook")

    # Метрика+Woo вебхуком БОЛЬШЕ НЕ триггерятся (08.07.2026): аналитике реальное
    # время не нужно, а вебхучный путь давал больше половины задач очереди.
    # Синк идёт сверкой по расписанию — см. metrika_sync (интрадей + ночная).
    pipeline_update = await get_nested(nested, ["leads", "update", "0", "pipeline_id"])
    pipeline_add = await get_nested(nested, ["leads", "add", "0", "pipeline_id"])
    incoming_pipeline = pipeline_update if pipeline_update is not None else pipeline_add

    # «Новый лид не взяли в работу» (Катя 28.08.2026): счётчик рабочего времени на входе
    # воронки. Здесь только словарь в памяти, без сети — вебхук ходит на каждое изменение
    # сделки, и лишний запрос в amo отсюда стоил бы дорого. Проверка и отправка — в
    # собственном цикле new_lead_watch.
    new_lead_watch.note_lead(lead_id, incoming_pipeline, incoming_status)

    # Новый лид в Академии (Катя 08.09.2026): сделка встала на «Входящий лид» воронки
    # Академии → уведомление Гладкову в топик УВЕДОМЛЕНИЯ. Здесь только сравнение
    # воронки и этапа, чтение сделки и отправка уходят в фон (academy_lead_alert).
    # Стоит ВЫШЕ блока `updates`: этап меняют и без правки полей сделки.
    academy_lead_alert.notify_bg(lead_id, incoming_pipeline, incoming_status)
    initial_responsible_user_id = await get_nested(
        nested, ["leads", "add", "0", "responsible_user_id"],
    )
    academy_assignment.assign_bg(
        lead_id,
        incoming_pipeline,
        incoming_status,
        is_new=lead_add_id is not None,
        initial_responsible_user_id=initial_responsible_user_id,
    )
    # Ручной перевод на «Записан на практикум» — явное намерение. Вебхук
    # может прислать текущий status_id и при иной правке, поэтому worker дополнительно
    # требует свежее amo-событие lead_status_changed именно для этой сделки.
    if (
        lead_id is not None
        and str(incoming_pipeline) == str(PIPELINE_ACADEMY)
        and str(incoming_status) == str(STATUS_ACADEMY_RECORDED_PRACTICUM)
    ):
        academy_invite_delivery.schedule_manual_stage(int(lead_id))

    # Протез отгрузок: пока amgroup лежит, отгрузку в МойСкладе не создаёт никто
    # и товар не списывается. Вешаемся на те же этапы воронки «Офис», на которых
    # её создавала amgroup (сверено на 22 сделках 03.09.2026). Модуль сам первым
    # делом смотрит флаг, воронку и этап и выходит без единого запроса в сеть -
    # вебхук приходит на каждое изменение любой сделки.
    amgroup_shipment.handle_lead_status_change_bg(lead_id, incoming_status, incoming_pipeline)

    # Авто-режим: сделка встала на этап маршрута, настроенного в панели. Модуль сам смотрит
    # флаг и режим и выходит без единого запроса в сеть, если выключен — вебхук приходит на
    # ЛЮБОЕ изменение ЛЮБОЙ сделки, и лишний поход в amo отсюда стоил бы дорого.
    autopilot.on_lead_change(lead_id)


    # Office Transfer: сделка воронки-источника зашла в УР(142)/ЗНР(143) →
    # вместо нативного копирования (F5-виджет/«Создать сделку») переносим ЭТУ
    # ЖЕ сделку в целевую воронку/этап (office_transfer.py). Мастер-флаг
    # OFFICE_TRANSFER_ENABLED + флаг конкретного правила (там же) — по умолчанию
    # выключено, включает Тиана по мере отключения нативной автоматики.
    # Источники: розница всегда, ОПТ — за OFFICE_TRANSFER_SOURCE_OPT (09.08.2026).
    # Гейт спрашиваем у office_transfer, чтобы список источников жил в одном
    # месте: разъехавшиеся гейты вебхука и диспетчера дали бы «вебхук ставит
    # задачу, диспетчер её скипает» — сделка ехала бы только страховкой раз в
    # две минуты, и то молча.
    if (
        OFFICE_TRANSFER_ENABLED
        and lead_id is not None
        and incoming_status is not None
        and str(incoming_status) in (str(STATUS_SUCCESS), str(STATUS_CLOSED_LOST))
        and (incoming_pipeline is None or office_transfer.is_source_pipeline(incoming_pipeline))
    ):
        logger.info(
            "Lead %s entered %s in pipeline %s — enqueue office_transfer",
            lead_id, incoming_status, incoming_pipeline,
        )
        enqueue_office_transfer(lead_id, source="webhook")

    # Lead Distribution: сделка вошла в точку входа (pipeline_id/status_id)
    # какого-то ВКЛЮЧЁННОГО профиля конструктора (lead_distribution.py) —
    # замена нативного виджета «Генезис». В отличие от office_transfer здесь
    # нет одной фиксированной пары воронка/этап — профили сами конфигурируют
    # свои точки входа, поэтому дешёвая проверка идёт через has_matching_*.
    # Часть событий amo приходит без pipeline_id в теле — это нормально,
    # тогда матчим по одному status_id (может дать ложный enqueue при двух
    # одинаковых status_id в разных воронках — не проблема: диспетчер
    # перечитывает сделку и сверяет пару заново).
    if lead_id is not None and incoming_status is not None:
        try:
            _ld_status = int(incoming_status)
        except (TypeError, ValueError):
            _ld_status = None
        if _ld_status is not None:
            if incoming_pipeline is not None:
                try:
                    _ld_matched = lead_distribution.has_matching_enabled_profile(int(incoming_pipeline), _ld_status)
                except (TypeError, ValueError):
                    _ld_matched = False
            else:
                _ld_matched = lead_distribution.has_matching_status(_ld_status)
            if _ld_matched:
                logger.info("Lead %s entered %s — enqueue lead_distribution", lead_id, incoming_status)
                enqueue_lead_distribution(lead_id, source="webhook")

    # Lead Distribution: ручная смена ответственного на сделке, распределённой
    # этим модулем СЕГОДНЯ, — коррекция счётчиков нагрузки (см. correct_reassignment).
    responsible_update = await get_nested(nested, ["leads", "update", "0", "responsible_user_id"])
    if lead_id is not None and responsible_update is not None:
        lead_distribution.correct_reassignment_bg(lead_id, responsible_update)

    # Счёт СБП (MAG-285): сделка зашла на тех-этап «Оплата запрошена» → создаём
    # платёжную ссылку Ozon из суммы заказа МС и одним PATCH пишем её в 577617 +
    # переводим сделку в «Ссылка отправлена», где штатные боты шлют шаблон.
    # Воронок две — розница и картотека «Работа с базой» (07.09.2026), список
    # спрашиваем у ozon_invoice, чтобы гейты вебхука и обработчика не разъехались.
    # Обработчик перечитывает сделку и проверяет пару воронка+этап заново.
    # Мастер-флаг OZON_INVOICE_ENABLED, картотека — ещё и OZON_INVOICE_DB_WORK.
    if (
        lead_id is not None
        and incoming_status is not None
        and ozon_invoice.is_invoice_entry(incoming_pipeline, incoming_status)
        and ozon_invoice.is_enabled()
    ):
        logger.info("Lead %s вошла в тех-этап оплаты (pipeline %s) — enqueue ozon invoice",
                    lead_id, incoming_pipeline)
        enqueue_invoice(lead_id, source="webhook")

    # Резерв товара в МойСклад (перенос с amGroup) — сделка сменила статус в
    # одной из отслеживаемых воронок (Основная/TangemShop/Офис). reserve_service
    # сам решает по свежим данным сделки, ставить резерв, снимать или не трогать.
    if lead_id is not None and incoming_status is not None:
        reserve_service.maybe_apply_bg(lead_id, incoming_pipeline)

    updates = await get_nested(nested, ["leads", "update", "0", "custom_fields"])
    if updates:
        # Автотег «Срочно»: Срочность → «Срочно» → вешаем тег (в фоне, не блокирует).
        urgency_tag.maybe_apply_bg(updates, lead_id)
        # Авто-перенос дубля в ЗИН: если «Причина отказа» стала «Дубль сделки» →
        # в фоне переводим сделку в 143 (её воронка). Идемпотентно.
        dup_autoclose.maybe_close_bg(updates, lead_id)
        for updated_field in updates:
            info = updates[updated_field]
            if info["id"] == "576703":
                order_summary = info["values"]["0"]["value"]
                goods, delivery_type = await parse_the_cart_field(order_summary)
                # Резерв товара: состав заказа изменился (например, допродажа) —
                # пересмотреть резерв даже без смены статуса сделки.
                reserve_service.maybe_apply_bg(lead_id, incoming_pipeline)
            if info["id"] == "576711":
                comment_summary = info["values"]["0"]["value"]
                promo_type, comment = await parse_the_cart_field_2(comment_summary)
            if info["id"] == "576719":
                delivery_address = info["values"]["0"]["value"]
            if info["id"] == "577415":
                lead_name = f'Заказ №{info["values"]["0"]["value"]}'
                logger.info(f"lead_name: {lead_name}")

        # Автотег «Запись в шоурум»: тип доставки (577315) = самовывоз из офиса
        # Sunscrypt → вешаем тег (в фоне, идемпотентно). «CDEK: Самовывоз» не триггерит.
        showroom_tag.maybe_apply_bg(delivery_type, lead_id)
        # Алерт в ТГ: самовывоз (офис ИЛИ шоурум) → в топик ШОУРУМ с тегом Кати,
        # чтобы записать клиента на визит. Шире автотега выше: тег вешается только на
        # самовывоз из офиса, а записывать надо и тех, кто забирает из шоурума.
        showroom_alert.notify_bg(delivery_type, lead_id)

        if (
            goods is not None
            or delivery_type is not None
            or delivery_address is not None
            or lead_name is not None
            or promo_type is not None
            or comment is not None
        ):
            if lead_id is None:
                logger.warning("Skipping update because lead_id is missing in payload")
                return HTTP_200_OK

            enqueue_new({
                "lead_id": lead_id,
                "goods": goods,
                "delivery_type": delivery_type,
                "delivery_address": delivery_address,
                "lead_name": lead_name,
                "promo_type": promo_type,
                "comment": comment,
            })
            return HTTP_200_OK

        logger.info(f"lead_id {lead_id}, nothing to update")
    else:
        logger.info(f"lead_id: {lead_id}, no Updates")

    return HTTP_200_OK
