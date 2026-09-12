"""Алерт в Telegram: новый заказ с самовывозом → записать клиента в шоурум.

Заказ приходит с сайта, тип доставки (577315) парсится из корзины (576703) и
прилетает вебхуком /lead_change через пару секунд после создания сделки. Если
доставка — любой из НАШИХ самовывозов (из офиса или из шоурума), шлём сообщение
в супергруппу ОП, топик ШОУРУМ, с @-тегом Кати: её задача записать клиента.

Четыре гейта, и все четыре нужны (07.08.2026, разбор спама в бою):
  1. самовывоз — «CDEK: Самовывоз» это ПВЗ, не наш офис, он НЕ считается;
  2. воронка «ОП розница» и этап «Новый лид» (хаб или буферный) — только свежая
     заявка, а не сделка в работе, в Офисе или в тестовой воронке;
  3. сделка создана не давнее SHOWROOM_ALERT_MAX_AGE_MIN — отсекает массовые
     прогоны по старью: 07.08 чужой прогон переписал корзину у полутора десятков
     закрытых сделок от 10-29.07, и в топик улетела пачка сообщений;
  4. мастер-флаг SHOWROOM_ALERT_ENABLED — выключить без выкатки кода.

Задержка перед отправкой: поля сделки заполняются не разом. Первое сообщение
того же дня ушло без товара («0.00 рублей»), потому что состав ещё не записался.
Ждём SHOWROOM_ALERT_DELAY_S и только потом читаем сделку — ровно так же, как
делал прежний триггер Цифровой воронки (он ждал минуту).

Дедуп — по lead_id, и он ПЕРЕЖИВАЕТ рестарт: список отправленных лежит в
/app/var (постоянный том контейнера). Память процесса гасит параллельные
вебхуки, файл — повтор после пересборки. Без файла 07.08 второй выкат заново
уведомил про заказ, о котором уже писали 45 минут назад: сделка всё ещё висела
в «Новый лид», а память процесса обнулилась.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque

import amo_service
import telegram_bot
import alerts
from api import BASE_URL
from tg_recipients import NOTIFY_CHAT_ID, SHOWROOM_ALERT_THREAD_ID, SHOWROOM_ALERT_TAG
from waybill_config import (
    DELIVERY_PICKUP_MARKERS,
    FIELD_COMPOSITION,
    FIELD_DELIVERY_TYPE,
    PIPELINE_CLEVER_MAIN,
    SHOWROOM_ALERT_DELAY_S,
    SHOWROOM_ALERT_ENABLED,
    SHOWROOM_ALERT_MAX_AGE_MIN,
    STATUS_NEW_LEAD_ALL,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
_seen_leads: set = set()
_seen_order: deque = deque()
_SEEN_CAP = 5000
_SEEN_PATH = os.getenv("SHOWROOM_ALERT_SEEN_PATH", "/app/var/showroom_alert_seen.json")
_seen_loaded = False


def _load_seen() -> None:
    """Поднять список уведомлённых сделок с диска. Файла нет / битый — начинаем с пустого."""
    global _seen_loaded
    if _seen_loaded:
        return
    _seen_loaded = True
    try:
        with open(_SEEN_PATH, encoding="utf-8") as f:
            for key in json.load(f):
                _seen_leads.add(str(key))
                _seen_order.append(str(key))
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("Шоурум-алерт: не прочитался %s — дедуп с нуля", _SEEN_PATH)


def _save_seen() -> None:
    """Сбросить список на диск. Не удался — не беда, дедуп в памяти остаётся."""
    try:
        os.makedirs(os.path.dirname(_SEEN_PATH), exist_ok=True)
        tmp = f"{_SEEN_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(_seen_order), f)
        os.replace(tmp, _SEEN_PATH)
    except Exception:
        logger.exception("Шоурум-алерт: не записался %s", _SEEN_PATH)


def _is_new(lead_id) -> bool:
    """True — по этой сделке ещё не слали (слать). False — уже слали (эхо вебхука
    или повтор после рестарта: список поднимается с диска)."""
    _load_seen()
    key = str(lead_id)
    if key in _seen_leads:
        return False
    _seen_leads.add(key)
    _seen_order.append(key)
    if len(_seen_order) > _SEEN_CAP:
        _seen_leads.discard(_seen_order.popleft())
    _save_seen()
    return True


def is_pickup(delivery_type) -> bool:
    """Тип доставки — наш самовывоз (офис или шоурум)? «CDEK: Самовывоз» — нет."""
    if not delivery_type:
        return False
    text = str(delivery_type).casefold()
    return any(marker in text for marker in DELIVERY_PICKUP_MARKERS)


def is_fresh_new_lead(lead: dict) -> bool:
    """Сделка — свежая заявка в «Новый лид» воронки ОП розница?

    Проверяется по СДЕЛКЕ, а не по вебхуку: массовый прогон по старым сделкам
    шлёт такие же вебхуки, и отличить их можно только этапом и возрастом."""
    if not lead:
        return False
    if str(lead.get("pipeline_id")) != str(PIPELINE_CLEVER_MAIN):
        return False
    if lead.get("status_id") not in STATUS_NEW_LEAD_ALL:
        return False
    created = lead.get("created_at") or 0
    return (time.time() - created) <= SHOWROOM_ALERT_MAX_AGE_MIN * 60


def notify_bg(delivery_type, lead_id) -> None:
    """delivery_type = «Тип доставки» (577315), распарсенный из корзины.
    Самовывоз и по сделке ещё не слали → в фоне проверяем остальное и шлём.
    Вебхук не блокируем: тяжёлые проверки уходят в фон."""
    if not SHOWROOM_ALERT_ENABLED:
        return
    if lead_id is None or not is_pickup(delivery_type):
        return
    if SHOWROOM_ALERT_THREAD_ID is None:
        logger.warning(
            "Шоурум-алерт: топик не настроен (SHOWROOM_ALERT_THREAD_ID=None) — "
            "сделка %s не уведомлена", lead_id,
        )
        return
    if not _is_new(lead_id):
        return
    task = asyncio.create_task(_apply(lead_id, delivery_type))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id, delivery_type) -> None:
    try:
        # Ждём, пока плагин сайта дозапишет состав, сумму и контакт.
        if SHOWROOM_ALERT_DELAY_S:
            await asyncio.sleep(SHOWROOM_ALERT_DELAY_S)

        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not is_fresh_new_lead(lead):
            logger.info(
                "Шоурум-алерт: сделка %s не свежая заявка в «Новый лид» — молчим", lead_id,
            )
            return

        # Тип доставки перечитываем из сделки: к этому моменту поле уже записано.
        delivery = amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or delivery_type
        if not is_pickup(delivery):
            logger.info("Шоурум-алерт: у сделки %s доставка уже не самовывоз — молчим", lead_id)
            return

        client = await _client_name(lead)
        composition = amo_service.get_custom_field_value(lead, FIELD_COMPOSITION)
        price = lead.get("price")

        text = _build_message(lead_id, client, composition, delivery, price)
        d = alerts.decide(
            "showroom_pickup", legacy_text=text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, thread_id=SHOWROOM_ALERT_THREAD_ID, lead=lead,
            values={
                "теги": SHOWROOM_ALERT_TAG,
                "клиент": client or "",
                "состав": composition or "",
                "доставка": delivery or "",
                "сумма": price or "",
                "ссылка_на_сделку": alerts.lead_link(lead_id),
            },
        )
        if d is None:
            logger.info("Шоурум-алерт: событие выключено в панели (сделка %s)", lead_id)
            return
        ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
        logger.info(
            "Шоурум-алерт: %s (сделка %s, клиент %s, доставка %s)",
            "отправлен" if ok else "НЕ отправлен", lead_id, client or "—", delivery or "—",
        )
    except Exception:
        logger.exception("Шоурум-алерт: ошибка на сделке %s", lead_id)


async def _client_name(lead: dict) -> str | None:
    """Имя клиента. ⚠️ Во вложенных контактах сделки amo отдаёт только id и
    is_main — имени там НЕТ, его надо дочитывать отдельным запросом."""
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    if not contacts:
        return None
    main = next((c for c in contacts if c.get("is_main")), contacts[0])
    try:
        contact = await amo_service.get_contact_by_id(main.get("id"))
    except Exception:
        logger.exception("Шоурум-алерт: контакт %s не прочитался", main.get("id"))
        return None
    return ((contact or {}).get("name") or "").strip() or None


def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(lead_id, client, composition, delivery, price) -> str:
    lines = [
        "🏬 Новый заказ с самовывозом",
        SHOWROOM_ALERT_TAG,
    ]
    if client:
        lines.append(f"👤 {_esc(client)}")
    if composition:
        lines.append(f"📦 {_esc(composition)}")
    if delivery:
        lines.append(f"🚚 {_esc(delivery)}")
    if price:
        lines.append(f"💰 {_esc(price)} ₽")
    lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
