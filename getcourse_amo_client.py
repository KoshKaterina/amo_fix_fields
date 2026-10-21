"""Тонкий клиент amo для интеграции GetCourse, со СВОИМ токеном.

Зачем свой, а не api.py. api.py ходит одним общим токеном интеграции amo_fix_fields, а сделки
GetCourse должны создаваться отдельной интеграцией «GetCourse Академия»: amo подписывает
сделку источником интеграции-создателя, и по этому источнику их раздаёт отдельное правило
распределителя (Артём и Кирилл поровну). У отдельной интеграции и свой лимит запросов.

Три вида ошибок, и различать их важно:
    AmoUnavailable - запрос не прошёл и ничего не изменил (сеть на чтении, 429, 5xx на чтении).
                     Повторять можно.
    AmoUncertain   - запрос на ЗАПИСЬ ушёл, а ответа нет (таймаут, обрыв, 5xx). Сделка или
                     примечание могли появиться. Вслепую не повторять.
    AmoError       - amo ответила отказом (4xx). Повтор того же запроса не поможет.
"""

import asyncio
import logging
import time
from typing import Any

import httpx

from waybill_config import GETCOURSE_AMO_BASE_URL, GETCOURSE_AMO_TOKEN

logger = logging.getLogger("uvicorn.error")

# Свой лимит у интеграции 7 запросов в секунду; потока GetCourse хватает 4 с запасом, а общий
# потолок по адресу сервера (около 10 в секунду) остаётся основному сервису.
MIN_INTERVAL_S = 0.25
TIMEOUT_S = 20.0
READ_ATTEMPTS = 3
READ_BACKOFF_S = (1.0, 3.0)

# Подменяется в тестах (httpx.MockTransport).
_transport: httpx.AsyncBaseTransport | None = None
_lock: asyncio.Lock | None = None
_last_call = 0.0


class AmoError(Exception):
    def __init__(self, status: int, detail: str = ""):
        super().__init__(f"amo {status}: {detail[:200]}")
        self.status = status


class AmoUnavailable(Exception):
    pass


class AmoUncertain(Exception):
    pass


def configured() -> bool:
    return bool(GETCOURSE_AMO_TOKEN)


async def _pace() -> None:
    global _lock, _last_call
    if _lock is None:
        _lock = asyncio.Lock()
    async with _lock:
        wait = _last_call + MIN_INTERVAL_S - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        _last_call = time.monotonic()


async def _send(method: str, path: str, *, params=None, json_body=None) -> httpx.Response:
    await _pace()
    headers = {"Authorization": f"Bearer {GETCOURSE_AMO_TOKEN}"}
    async with httpx.AsyncClient(base_url=GETCOURSE_AMO_BASE_URL, timeout=TIMEOUT_S,
                                 transport=_transport) as client:
        return await client.request(method, path, params=params, json=json_body, headers=headers)


def _json(resp: httpx.Response) -> Any:
    if resp.status_code == 204 or not resp.content:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


async def _read(path: str, params=None) -> Any:
    """GET с повторами. 204 и 404 - «ничего нет» (None), это не ошибка."""
    last = ""
    for attempt in range(READ_ATTEMPTS):
        if attempt:
            await asyncio.sleep(READ_BACKOFF_S[min(attempt - 1, len(READ_BACKOFF_S) - 1)])
        try:
            resp = await _send("GET", path, params=params)
        except httpx.HTTPError as exc:
            last = type(exc).__name__
            continue
        if resp.status_code in (204, 404):
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            last = str(resp.status_code)
            continue
        if resp.status_code >= 400:
            raise AmoError(resp.status_code, resp.text)
        return _json(resp)
    raise AmoUnavailable(f"GET {path}: {last}")


async def _write(method: str, path: str, body: Any) -> Any:
    """Запись без повторов: ответ потерян - значит неизвестно, выполнена ли она."""
    try:
        resp = await _send(method, path, json_body=body)
    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        # Соединение не установилось - запрос до amo не дошёл.
        raise AmoUnavailable(f"{method} {path}: {type(exc).__name__}") from exc
    except httpx.HTTPError as exc:
        raise AmoUncertain(f"{method} {path}: {type(exc).__name__}") from exc
    if resp.status_code == 429:
        # Отказ лимита: amo запрос не выполняла.
        raise AmoUnavailable(f"{method} {path}: 429")
    if resp.status_code >= 500:
        raise AmoUncertain(f"{method} {path}: {resp.status_code}")
    if resp.status_code >= 400:
        raise AmoError(resp.status_code, resp.text)
    return _json(resp)


# ─────────────────────────── чтение ───────────────────────────

async def search_contacts(query: str) -> list[dict]:
    """Контакты по строке поиска amo, со ссылками на их сделки. Поиск даёт КАНДИДАТОВ:
    точное совпадение значений проверяет вызывающий."""
    data = await _read("/api/v4/contacts", {"query": query, "with": "leads", "limit": 50})
    return ((data or {}).get("_embedded") or {}).get("contacts") or []


async def get_contact(contact_id: int) -> dict | None:
    return await _read(f"/api/v4/contacts/{int(contact_id)}", {"with": "leads"})


async def get_leads(lead_ids: list[int]) -> list[dict]:
    """Сделки по списку ID (этап, воронка, поля, теги). Пустой список - пустой ответ без запроса."""
    ids = sorted({int(x) for x in lead_ids})
    leads: list[dict] = []
    for i in range(0, len(ids), 50):
        params = [("filter[id][]", str(x)) for x in ids[i:i + 50]] + [("limit", "50")]
        data = await _read("/api/v4/leads", params)
        leads.extend(((data or {}).get("_embedded") or {}).get("leads") or [])
    return leads


async def get_lead(lead_id: int) -> dict | None:
    return await _read(f"/api/v4/leads/{int(lead_id)}", {"with": "contacts"})


async def lead_note_texts(lead_id: int) -> list[str]:
    """Тексты последних обычных примечаний сделки - чтобы не продублировать своё."""
    data = await _read(
        f"/api/v4/leads/{int(lead_id)}/notes",
        {"filter[note_type]": "common", "limit": 250, "order[id]": "desc"},
    )
    notes = ((data or {}).get("_embedded") or {}).get("notes") or []
    return [str((n.get("params") or {}).get("text") or "") for n in notes]


# ─────────────────────────── запись ───────────────────────────

async def create_lead_complex(lead: dict) -> tuple[int, int | None]:
    """Сделка вместе с контактом (новым или существующим) одним запросом. (lead_id, contact_id)."""
    data = await _write("POST", "/api/v4/leads/complex", [lead])
    item = (data or [None])[0] if isinstance(data, list) else None
    if not item or not item.get("id"):
        # Ответ 200 без ID: что создалось - неизвестно.
        raise AmoUncertain("leads/complex: ответ без id")
    return int(item["id"]), (int(item["contact_id"]) if item.get("contact_id") else None)


async def add_note(lead_id: int, text: str) -> None:
    await _write("POST", f"/api/v4/leads/{int(lead_id)}/notes",
                 [{"note_type": "common", "params": {"text": text}}])


async def patch_lead(lead_id: int, body: dict) -> None:
    await _write("PATCH", f"/api/v4/leads/{int(lead_id)}", body)


async def patch_contact(contact_id: int, body: dict) -> None:
    await _write("PATCH", f"/api/v4/contacts/{int(contact_id)}", body)
