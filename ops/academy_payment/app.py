"""Isolated, fixed-price Academy payment page; no CRM/Woo imports or writes.

26.11.2026 (решение Кати): прямая ссылка НСПК qr.nspk.ru стоит на сертификате Минцифры, и Chrome,
Samsung Browser и Safari её не открывают. Теперь как у счетов amo и сайта: платёж СБП создаётся
вместе с сокращённым заказом Ozon; компьютер видит наш QR на этой странице, телефон уходит на
страницу оплаты Ozon (checkout.ozon.ru, там СБП и карта). Оплату подтверждает подписанный колбэк
или getOrderStatus по extId заказа (по заказу Ozon колбэк может не прийти, getPaymentDetails
такие платежи не видит).
"""
import asyncio
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import sqlite3
import time
import weakref
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

PREFIX = "/academy-pay"
ORIGIN = "https://team.sunscrypt.ru"
AMOUNT = 1_000_000  # 10 000 RUB; same units as Woo and ozon_invoice.
TTL = 86400
STATUS_RECHECK = 10  # seconds between getOrderStatus calls for one bill
# 26.11.2026: new cookie name, so pending SBP-only bills (qr.nspk.ru, no Ozon order) get a new
# bill with an order. Paid legacy sessions keep their paid guard (see entry()).
COOKIE = "academy_pay_session"
LEGACY_COOKIES = ("academy_sbp_only_session", "academy_deposit_session")
ACCESS = os.environ.get("OZON_PAY_ACCESS_KEY", "")
SECRET = os.environ.get("OZON_PAY_SECRET_KEY", "")
NOTIFY_SECRET = os.environ.get("OZON_PAY_NOTIFICATION_SECRET_KEY", "")
SESSION_SECRET = os.environ.get("ACADEMY_PAYMENT_SESSION_SECRET", "")
DB_PATH = Path(os.environ.get("ACADEMY_PAYMENT_DB", "/data/payments.sqlite3"))
session_locks = weakref.WeakValueDictionary()


def session_lock(session):
    """Serialize one browser's requests without queueing unrelated visitors."""
    key = session_hash(session)
    existing = session_locks.get(key)
    if existing is None:
        existing = asyncio.Lock()
        session_locks[key] = existing
    return existing


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS payments (
            session TEXT PRIMARY KEY, ext_id TEXT UNIQUE NOT NULL,
            order_ext_id TEXT UNIQUE NOT NULL, payment_id TEXT,
            pay_link TEXT, status TEXT NOT NULL, created INTEGER NOT NULL,
            ip_hash TEXT NOT NULL, paid_at INTEGER)""")
        # Additive migration 26.11.2026: QR code for the desktop page, Ozon order flag and
        # last bank status check. Old rows keep NULL: they are SBP-only bills without an order.
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(payments)")}
        for name, kind in (("qr_payload", "TEXT"), ("with_order", "INTEGER NOT NULL DEFAULT 0"),
                           ("checked_at", "INTEGER")):
            if name not in columns:
                conn.execute(f"ALTER TABLE payments ADD COLUMN {name} {kind}")


@asynccontextmanager
async def lifespan(app):
    if not all((ACCESS, SECRET, NOTIFY_SECRET, SESSION_SECRET)):
        raise RuntimeError("Required payment configuration is missing")
    init_db()
    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def headers(request, call_next):
    response = await call_next(request)
    response.headers.update({
        "Cache-Control": "no-store", "X-Robots-Tag": "noindex, nofollow",
        "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
    })
    return response


def signature(value):
    return hashlib.sha256(f"{value}{ACCESS}{SECRET}".encode()).hexdigest()


def session_hash(session):
    return hashlib.sha256(session.encode()).hexdigest()


def csrf(session):
    return hmac.new(SESSION_SECRET.encode(), session.encode(), hashlib.sha256).hexdigest()


def valid_session(request):
    session = request.cookies.get(COOKIE, "")
    if not re.fullmatch(r"[a-f0-9]{64}", session):
        raise HTTPException(403, "Откройте ссылку на оплату заново")
    return session


def payment(session):
    with db() as conn:
        return conn.execute("SELECT * FROM payments WHERE session=?", (session_hash(session),)).fetchone()


_STYLE = """body{margin:0;background:#fff;color:#151b26;font:17px/1.5 system-ui,sans-serif}
main{max-width:560px;padding:40px 24px;margin:8vh auto}h1{font-size:28px;line-height:1.2;text-wrap:balance}
p{max-width:60ch}button,a{font:inherit}button{min-height:48px;border:0;border-radius:8px;padding:12px 20px;background:#0164fa;color:#fff;cursor:pointer}
button:disabled{opacity:.6}a{color:#0149bf}:focus-visible{outline:3px solid #151b26;outline-offset:4px}
#qr[hidden]{display:none}#qr{margin:24px 0 0;padding:20px 22px;border:1px solid #e1e6f0;border-radius:16px;background:#f0f5fa}
#qr .t{font-size:19px;font-weight:700;line-height:1.2;margin:0 0 7px}#qr p{margin:7px 0 0;font-size:15px;color:#37414d}
#qr .row{display:flex;gap:22px;align-items:center;margin:16px 0 0}
#qr .code{flex:0 0 auto;width:232px;height:232px;padding:6px;box-sizing:border-box;background:#fff;border:1px solid #e1e6f0;border-radius:12px}
#qr .code svg{display:block;width:100%;height:100%}
#qr .wait{display:flex;align-items:center;gap:10px;margin-top:16px}
#qr .spin{width:16px;height:16px;flex:0 0 auto;border:2.5px solid #cdd8ea;border-top-color:#0164fa;border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}@media (prefers-reduced-motion:reduce){#qr .spin{animation:none}}
@media (max-width:600px){#qr .row{flex-direction:column;align-items:stretch;gap:12px}#qr .code{margin:0 auto}}"""

# Desktop QR block: filled by payment.js with the server-generated SVG. Hidden until then.
_QR_BLOCK = """<section id="qr" hidden aria-live="polite"><div class="t">Оплатите онлайн</div>
<p>Предоплата 10 000 ₽ за четвёртый поток «Инвестиции в DeFi».</p>
<div class="row"><div class="code" id="qr-code"></div>
<p>Наведите камеру телефона на QR-код или отсканируйте его в приложении банка.</p></div>
<p class="wait"><span class="spin" aria-hidden="true"></span><span id="qr-wait">Ждём оплату. После оплаты страница обновится сама.</span></p></section>"""


def page(title, message, session, returning=False):
    # This is a payment/status page, not the external Academy contact form.
    content = f"""<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<meta name="csrf" content="{csrf(session)}"><title>{html.escape(title)} — Sunscrypt</title>
<style>{_STYLE}</style>
<main data-return="{str(returning).lower()}"><p>Sunscrypt Академия</p><h1>{html.escape(title)}</h1>
<p>Четвёртый поток «Инвестиции в DeFi»</p><p id="status" role="status">{html.escape(message)}</p>
<form method="post" action="{PREFIX}/create"><input type="hidden" name="csrf" value="{csrf(session)}">
<button id="pay" type="submit">Перейти к оплате 10 000 ₽</button></form>
{_QR_BLOCK}
<p><a href="https://t.me/Sunscrypt_manager">Связаться с менеджером</a></p></main>
<script src="{PREFIX}/payment.js" defer></script></html>"""
    response = HTMLResponse(content)
    response.set_cookie(COOKIE, session, max_age=TTL, secure=True, httponly=True, samesite="lax", path=PREFIX)
    return response


@app.get("/health")
async def health():
    return {"ok": True}


@app.get(PREFIX + "/deposit-10000")
async def entry(request: Request):
    session = request.cookies.get(COOKIE, "")
    if not re.fullmatch(r"[a-f0-9]{64}", session):
        session = secrets.token_hex(32)
    row = payment(session)
    # Preserve the paid guard when upgrading a visitor from an older cookie.
    for name in LEGACY_COOKIES:
        legacy = request.cookies.get(name, "")
        if row is None and re.fullmatch(r"[a-f0-9]{64}", legacy):
            legacy_row = payment(legacy)
            if legacy_row and legacy_row["status"] == "paid":
                session, row = legacy, legacy_row
    if row and row["status"] == "pending" and row["created"] <= time.time() - TTL:
        session = secrets.token_hex(32)
        row = None
    if row and row["status"] == "paid":
        return page("Предоплата получена", "Спасибо! Менеджер свяжется с вами.", session, returning=True)
    return page("Предоплата 10 000 ₽", "Открываем оплату…", session)


# Desktop (mouse) gets the QR on this page; phones and tablets go straight to the Ozon page.
# Polling: every 4 s for 3 minutes, then every 10 s, up to 20 minutes; immediately when the
# tab becomes visible again (the buyer comes back from the bank app).
_SCRIPT = """const button=document.querySelector('#pay'), status=document.querySelector('#status');
const returning=document.querySelector('main').dataset.return==='true';
const desktop=window.matchMedia('(hover:hover) and (pointer:fine)').matches;
function paidView(){document.querySelector('h1').textContent='Предоплата получена';status.textContent='Спасибо! Менеджер свяжется с вами.';
button.hidden=true;const q=document.querySelector('#qr');if(q)q.hidden=true;}
function showQr(svg){const box=document.querySelector('#qr');document.querySelector('#qr-code').innerHTML=svg;box.hidden=false;
status.textContent='';button.hidden=true;watch();}
function watch(){const started=Date.now();let timer=null,busy=false,done=false;
const delay=()=>Date.now()-started<180000?4000:10000;
function giveUp(){done=true;const w=document.querySelector('#qr-wait');if(w)w.textContent='Пока не видим оплату. Если вы уже оплатили, обновите страницу через минуту.';
const s=document.querySelector('#qr .spin');if(s)s.hidden=true;}
function next(){if(done)return;if(Date.now()-started>=1200000){giveUp();return;}clearTimeout(timer);timer=setTimeout(poll,delay());}
async function poll(){if(busy||done)return;busy=true;try{const r=await fetch('/academy-pay/status',{cache:'no-store'});const d=await r.json();
if(d.paid){done=true;paidView();return;}}catch(e){}busy=false;next();}
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'&&!done){clearTimeout(timer);poll();}});
timer=setTimeout(poll,delay());}
async function start(e){if(e)e.preventDefault();button.disabled=true;
try{const r=await fetch('/academy-pay/create',{method:'POST',headers:{'X-Academy-CSRF':document.querySelector('meta[name=csrf]').content,'Accept':'application/json'}});
const d=await r.json();if(!r.ok)throw new Error(d.detail||'Оплата временно недоступна');
if(d.paid){paidView();return;}
if(desktop&&d.qr){showQr(d.qr);return;}
window.location.replace(d.url);}catch(e){status.textContent=e.message;button.disabled=false;}}
document.querySelector('form').addEventListener('submit',start);
if(!returning)start();else{button.hidden=true;let count=0;
async function check(){try{const r=await fetch('/academy-pay/status',{cache:'no-store'});const d=await r.json();
if(d.paid){paidView();return;}}
catch(e){}if(++count<30)setTimeout(check,4000);else status.textContent='Подтверждение оплаты пока не получено. Если деньги списались, повторно не оплачивайте — свяжитесь с менеджером.';}check();}
"""


@app.get(PREFIX + "/payment.js")
async def script():
    return Response(_SCRIPT, media_type="application/javascript")


def body_for(ext_id, order_ext_id):
    amount = {"currencyCode": "643", "value": str(AMOUNT)}
    expires = datetime.fromtimestamp(time.time() + TTL, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {
        "accessKey": ACCESS, "payType": "SBP", "amount": amount,
        "extId": ext_id, "requestSign": signature(ext_id), "ttl": TTL,
        "redirectUrl": ORIGIN + PREFIX + "/return",
        "notificationUrl": ORIGIN + PREFIX + "/notify",
        # Shortened Ozon order (same format as ozon_invoice / sunscrypt-sbp): it gives the Ozon
        # payment page for phones and getOrderStatus for confirmation. order is not signed.
        "order": {
            "extId": order_ext_id, "amount": amount, "paymentAlgorithm": "PAY_ALGO_SMS",
            "mode": "MODE_SHORTENED", "expiresAt": expires, "enableFiscalization": False,
            "successUrl": ORIGIN + PREFIX + "/return", "failUrl": ORIGIN + PREFIX + "/return",
        },
    }


async def create_ozon(body):
    async with httpx.AsyncClient(timeout=25) as client:
        # No automatic retry: a timeout may already have created the invoice.
        response = await client.post("https://payapi.ozon.ru/v1/createPayment", json=body)
        response.raise_for_status()
        return response.json()


async def order_status_ozon(order_ext_id):
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post("https://payapi.ozon.ru/v1/getOrderStatus", json={
            "extId": order_ext_id, "accessKey": ACCESS, "requestSign": signature(order_ext_id)})
        response.raise_for_status()
        return response.json()


def safe_payload(data):
    """QR code: only https qr.nspk.ru; amount/currency/type must match when present."""
    link = ((data.get("paymentDetails") or {}).get("sbp") or {}).get("payload") or ""
    url = urlsplit(link)
    params = parse_qs(url.query)
    if (url.scheme != "https" or url.hostname != "qr.nspk.ru" or url.port not in (None, 443)
            or not url.path.strip("/") or url.username or url.password
            or ("sum" in params and params["sum"] != [str(AMOUNT)])
            or ("cur" in params and params["cur"] != ["RUB"])
            or ("type" in params and params["type"] != ["02"])):
        # Current Ozon also returns opaque NSPK URLs without query parameters.
        # Their amount is fixed in the authenticated createPayment request.
        raise ValueError("Invalid payment QR")
    return link


def safe_checkout(data):
    """Phone link: only https checkout.ozon.ru from the order we created."""
    order = data.get("order") or {}
    link = (order.get("item") or {}).get("payLink") or order.get("payLink") or ""
    url = urlsplit(link)
    if (url.scheme != "https" or url.hostname != "checkout.ozon.ru" or url.port not in (None, 443)
            or not url.path.strip("/") or url.username or url.password):
        raise ValueError("Invalid payment page")
    return link


def order_paid(data, order_ext_id):
    """getOrderStatus: only our order, not a test, exactly 10 000 RUB, status STATUS_PAID."""
    if not isinstance(data, dict) or data.get("isTestMode") is True or not data.get("id"):
        return False
    if str(data.get("extId") or "") != order_ext_id:
        return False
    money = data.get("originalAmount") or {}
    if not isinstance(money, dict) or str(money.get("value")) != str(AMOUNT) or str(money.get("currencyCode")) not in ("643", "RUB"):
        return False
    return data.get("status") == "STATUS_PAID"


def _answer(row):
    """JSON for the page: phone link and, for bills with an order, the desktop QR."""
    out = {"url": row["pay_link"]}
    if row["with_order"] and row["qr_payload"]:
        out["qr"] = qr_svg(row["qr_payload"], "QR-код для оплаты")
    return out


@app.post(PREFIX + "/create")
async def create(request: Request):
    if request.headers.get("origin") != ORIGIN:
        raise HTTPException(403, "Откройте ссылку на оплату заново")
    session = valid_session(request)
    token = request.headers.get("X-Academy-CSRF", "")
    is_form = request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded")
    if is_form:
        raw = await request.body()
        if len(raw) > 1024:
            raise HTTPException(413, "Запрос слишком большой")
        token = parse_qs(raw.decode()).get("csrf", [""])[0]
    if not hmac.compare_digest(token, csrf(session)):
        raise HTTPException(403, "Откройте ссылку на оплату заново")
    async with session_lock(session):
        row = payment(session)
        if row:
            if row["status"] == "paid":
                return JSONResponse({"paid": True})
            if row["status"] == "pending" and row["created"] > time.time() - TTL:
                if not row["with_order"]:
                    # A pending SBP-only bill (qr.nspk.ru) under the new cookie should not exist;
                    # never hand out a link that Chrome/Safari cannot open.
                    raise HTTPException(409, "Ссылка на оплату обновлена. Откройте общий адрес заново.")
            else:
                raise HTTPException(409, "Не удалось подтвердить создание счёта. Свяжитесь с менеджером, повторно не оплачивайте.")
        else:
            now = int(time.time())
            ip = request.headers.get("X-Real-IP", "unknown")
            ip_hash = hmac.new(SESSION_SECRET.encode(), ip.encode(), hashlib.sha256).hexdigest()
            ext_id = "academy-deposit-" + secrets.token_hex(12)
            order_ext_id = "DEFI4-10000_" + secrets.token_hex(8)
            with db() as conn:
                own = conn.execute("SELECT count(*) FROM payments WHERE ip_hash=? AND created>?", (ip_hash, now-3600)).fetchone()[0]
                total = conn.execute("SELECT count(*) FROM payments WHERE created>?", (now-600,)).fetchone()[0]
                if own >= 20 or total >= 300:
                    raise HTTPException(429, "Слишком много попыток. Попробуйте позже или свяжитесь с менеджером.")
                conn.execute("INSERT INTO payments(session,ext_id,order_ext_id,status,created,ip_hash,with_order) VALUES(?,?,?,?,?,?,1)",
                             (session_hash(session), ext_id, order_ext_id, "creating", now, ip_hash))
            try:
                data = await create_ozon(body_for(ext_id, order_ext_id))
                checkout = safe_checkout(data)
                payload = safe_payload(data)
                pid = str((data.get("paymentDetails") or {}).get("paymentId") or "")
                with db() as conn:
                    conn.execute("UPDATE payments SET payment_id=?,pay_link=?,qr_payload=?,status='pending' WHERE session=? AND status='creating'",
                                 (pid, checkout, payload, session_hash(session)))
            except (httpx.HTTPError, ValueError, TypeError, KeyError):
                # Keep the durable attempt; do not silently issue another bill.
                raise HTTPException(502, "Не удалось открыть оплату. Свяжитесь с менеджером.") from None
            row = payment(session)
    if is_form:
        # No JavaScript: one link that opens everywhere — the Ozon payment page.
        return Response(status_code=303, headers={"Location": row["pay_link"]})
    return JSONResponse(_answer(row))


def notification_identity(data):
    received = str(data.get("requestSign") or "")
    amount = str(data.get("amount") if data.get("amount") is not None else "")
    currency = str(data.get("currencyCode") or "")
    ext_tx = str(data.get("extTransactionID") or "")
    ext_order = str(data.get("extOrderID") or data.get("extOrderId") or "")
    order = str(data.get("orderID") or data.get("orderId") or "")
    transaction = str(data.get("transactionID") or "")
    variants = (
        f"{ACCESS}|||{ext_tx}|{amount}|{currency}|{NOTIFY_SECRET}",
        f"{ACCESS}|{order}|{transaction}|{ext_order}|{amount}|{currency}|{NOTIFY_SECRET}",
    )
    if received and NOTIFY_SECRET:
        for index, value in enumerate(variants):
            if hmac.compare_digest(hashlib.sha256(value.encode()).hexdigest(), received):
                # The self-integration signature does NOT cover extOrderID.
                # Never trust that unsigned field to select a different invoice.
                return ("ext_id", ext_tx) if index == 0 else ("order_ext_id", ext_order)
    return None


def mark_paid(column, identifier):
    assert column in ("ext_id", "order_ext_id", "session")
    with db() as conn:
        # No pattern extraction of CRM ids. Only an exact, locally issued bill.
        conn.execute(f"UPDATE payments SET status='paid',paid_at=COALESCE(paid_at,?) WHERE {column}=?",
                     (int(time.time()), identifier))


@app.post(PREFIX + "/notify")
async def notify(request: Request):
    raw = await request.body()
    if len(raw) > 16384:
        raise HTTPException(413)
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(400) from None
    identity = notification_identity(data) if isinstance(data, dict) else None
    if identity is None:
        raise HTTPException(401)
    if data.get("status") != "Completed":
        return {"ok": True}
    if str(data.get("amount")) != str(AMOUNT) or str(data.get("currencyCode")) not in ("643", "RUB"):
        raise HTTPException(422)
    mark_paid(*identity)
    return {"ok": True}


@app.get(PREFIX + "/return")
async def returning(request: Request):
    session = valid_session(request)
    row = payment(session)
    if not row:
        raise HTTPException(404, "Счёт не найден. Свяжитесь с менеджером.")
    paid = row["status"] == "paid"
    return page("Предоплата получена" if paid else "Проверяем оплату",
                "Спасибо! Менеджер свяжется с вами." if paid else "Ожидаем подтверждение от банка. Повторно оплачивать не нужно.",
                session, returning=True)


async def refresh(row):
    """Ask Ozon about our order when the callback has not arrived. At most once per
    STATUS_RECHECK seconds per bill; a bank error leaves the bill pending."""
    if row["status"] != "pending" or not row["with_order"]:
        return row["status"] == "paid"
    now = int(time.time())
    if now - int(row["checked_at"] or 0) < STATUS_RECHECK:
        return False
    with db() as conn:
        # Claim the slot before the call: parallel polls of one bill do not stampede the bank.
        claimed = conn.execute("UPDATE payments SET checked_at=? WHERE session=? AND COALESCE(checked_at,0)<=?",
                               (now, row["session"], now - STATUS_RECHECK)).rowcount
    if not claimed:
        return False
    try:
        data = await order_status_ozon(row["order_ext_id"])
    except (httpx.HTTPError, ValueError, TypeError, KeyError):
        return False
    if order_paid(data, row["order_ext_id"]):
        mark_paid("session", row["session"])
        return True
    return False


@app.get(PREFIX + "/status")
async def status(request: Request):
    row = payment(valid_session(request))
    if not row:
        return {"paid": False}
    if row["status"] == "paid":
        return {"paid": True}
    return {"paid": await refresh(row)}


# --- QR (встроено из pay_qr.py) ---
# Копия генератора pay_qr.py из корня репозитория (сверен с python-qrcode): контейнер
# academy-deposit-link монтирует только этот файл, а в образе нет ни qrcode, ни segno.
# Совпадение с pay_qr.py проверяет test_app.py. Правите генератор — правьте оба.

# Блоки для уровня M: (количество блоков, всего кодовых слов в блоке, из них данных).
RS_M = {
    1: [(1, 26, 16)], 2: [(1, 44, 28)], 3: [(1, 70, 44)], 4: [(2, 50, 32)],
    5: [(2, 67, 43)], 6: [(4, 43, 27)], 7: [(4, 49, 31)],
    8: [(2, 60, 38), (2, 61, 39)], 9: [(3, 58, 36), (2, 59, 37)],
    10: [(4, 69, 43), (1, 70, 44)], 11: [(1, 80, 50), (4, 81, 51)],
    12: [(6, 58, 36), (2, 59, 37)], 13: [(8, 59, 37), (1, 60, 38)],
    14: [(4, 64, 40), (5, 65, 41)], 15: [(5, 65, 41), (5, 66, 42)],
    16: [(7, 73, 45), (3, 74, 46)], 17: [(10, 74, 46), (1, 75, 47)],
    18: [(9, 69, 43), (4, 70, 44)], 19: [(3, 70, 44), (11, 71, 45)],
    20: [(3, 67, 41), (13, 68, 42)], 21: [(17, 68, 42)], 22: [(17, 74, 46)],
    23: [(4, 75, 47), (14, 76, 48)], 24: [(6, 73, 45), (14, 74, 46)],
    25: [(8, 75, 47), (13, 76, 48)], 26: [(19, 74, 46), (4, 75, 47)],
    27: [(22, 73, 45), (3, 74, 46)], 28: [(3, 73, 45), (23, 74, 46)],
    29: [(21, 73, 45), (7, 74, 46)], 30: [(19, 75, 47), (10, 76, 48)],
    31: [(2, 74, 46), (29, 75, 47)], 32: [(10, 74, 46), (23, 75, 47)],
    33: [(14, 74, 46), (21, 75, 47)], 34: [(14, 74, 46), (23, 75, 47)],
    35: [(12, 75, 47), (26, 76, 48)], 36: [(6, 75, 47), (34, 76, 48)],
    37: [(29, 74, 46), (14, 75, 47)], 38: [(13, 74, 46), (32, 75, 47)],
    39: [(40, 75, 47), (7, 76, 48)], 40: [(18, 75, 47), (31, 76, 48)],
}

# Центры выравнивающих узоров по версиям.
ALIGN = {
    1: [], 2: [6, 18], 3: [6, 22], 4: [6, 26], 5: [6, 30], 6: [6, 34],
    7: [6, 22, 38], 8: [6, 24, 42], 9: [6, 26, 46], 10: [6, 28, 50],
    11: [6, 30, 54], 12: [6, 32, 58], 13: [6, 34, 62],
    14: [6, 26, 46, 66], 15: [6, 26, 48, 70], 16: [6, 26, 50, 74],
    17: [6, 30, 54, 78], 18: [6, 30, 56, 82], 19: [6, 30, 58, 86], 20: [6, 34, 62, 90],
    21: [6, 28, 50, 72, 94], 22: [6, 26, 50, 74, 98], 23: [6, 30, 54, 78, 102],
    24: [6, 28, 54, 80, 106], 25: [6, 32, 58, 84, 110], 26: [6, 30, 58, 86, 114],
    27: [6, 34, 62, 90, 118],
    28: [6, 26, 50, 74, 98, 122], 29: [6, 30, 54, 78, 102, 126], 30: [6, 26, 52, 78, 104, 130],
    31: [6, 30, 56, 82, 108, 134], 32: [6, 34, 60, 86, 112, 138], 33: [6, 30, 58, 86, 114, 142],
    34: [6, 34, 62, 90, 118, 146],
    35: [6, 30, 54, 78, 102, 126, 150], 36: [6, 24, 50, 76, 102, 128, 154],
    37: [6, 28, 54, 80, 106, 132, 158], 38: [6, 32, 58, 84, 110, 136, 162],
    39: [6, 26, 54, 82, 110, 138, 166], 40: [6, 30, 58, 86, 114, 142, 170],
}

_EXP = [0] * 512
_LOG = [0] * 256
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]


def _gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def _data_capacity(version: int) -> int:
    return sum(count * data for count, _total, data in RS_M[version])


def _pick_version(length: int) -> int:
    for v in range(1, 41):
        bits = 4 + (8 if v < 10 else 16) + 8 * length
        if bits <= _data_capacity(v) * 8:
            return v
    raise ValueError("QR: данные не помещаются")


def _put(bits: list[int], value: int, length: int) -> None:
    for i in range(length - 1, -1, -1):
        bits.append((value >> i) & 1)


def _rs_remainder(block: list[int], ec_count: int) -> list[int]:
    gen = [1]
    for i in range(ec_count):
        nxt = [0] * (len(gen) + 1)
        for j, coef in enumerate(gen):
            nxt[j] ^= coef
            nxt[j + 1] ^= _gf_mul(coef, _EXP[i])
        gen = nxt
    res = block + [0] * ec_count
    for i in range(len(block)):
        coef = res[i]
        if coef == 0:
            continue
        for j in range(1, ec_count + 1):
            res[i + j] ^= _gf_mul(gen[j], coef)
    return res[len(block):]


def _codewords(data_bytes: bytes, version: int) -> list[int]:
    """Данные + коррекция, перемешанные по блокам."""
    capacity = _data_capacity(version)
    bits: list[int] = []
    _put(bits, 0b0100, 4)
    _put(bits, len(data_bytes), 8 if version < 10 else 16)
    for b in data_bytes:
        _put(bits, b, 8)
    if len(bits) + 4 <= capacity * 8:
        _put(bits, 0, 4)
    while len(bits) % 8:
        bits.append(0)
    data = []
    for i in range(0, len(bits), 8):
        byte = 0
        for j in range(8):
            byte = (byte << 1) | bits[i + j]
        data.append(byte)
    pad = (0xEC, 0x11)
    i = 0
    while len(data) < capacity:
        data.append(pad[i % 2])
        i += 1

    data_blocks: list[list[int]] = []
    ec_blocks: list[list[int]] = []
    offset = 0
    for count, total, data_count in RS_M[version]:
        for _ in range(count):
            block = data[offset:offset + data_count]
            offset += data_count
            data_blocks.append(block)
            ec_blocks.append(_rs_remainder(block, total - data_count))
    out: list[int] = []
    for blocks in (data_blocks, ec_blocks):
        longest = max(len(b) for b in blocks)
        for i in range(longest):
            for b in blocks:
                if i < len(b):
                    out.append(b[i])
    return out


def _bch_digit(data: int) -> int:
    return data.bit_length()


def _bch_format(data: int) -> int:
    g = 0b10100110111
    d = data << 10
    while _bch_digit(d) - _bch_digit(g) >= 0:
        d ^= g << (_bch_digit(d) - _bch_digit(g))
    return ((data << 10) | d) ^ 0b101010000010010


def _bch_version(data: int) -> int:
    g = 0b1111100100101
    d = data << 12
    while _bch_digit(d) - _bch_digit(g) >= 0:
        d ^= g << (_bch_digit(d) - _bch_digit(g))
    return (data << 12) | d


def _mask_bit(mask: int, i: int, j: int) -> bool:
    if mask == 0:
        return (i + j) % 2 == 0
    if mask == 1:
        return i % 2 == 0
    if mask == 2:
        return j % 3 == 0
    if mask == 3:
        return (i + j) % 3 == 0
    if mask == 4:
        return (i // 2 + j // 3) % 2 == 0
    if mask == 5:
        return (i * j) % 2 + (i * j) % 3 == 0
    if mask == 6:
        return ((i * j) % 2 + (i * j) % 3) % 2 == 0
    return ((i * j) % 3 + (i + j) % 2) % 2 == 0


def _build(words: list[int], version: int, mask: int, test: bool) -> list[list[bool]]:
    """Сборка символа. test — служебные поля пустые (для подсчёта штрафа, как в эталоне)."""
    n = 17 + 4 * version
    m: list[list] = [[None] * n for _ in range(n)]

    for row, col in ((0, 0), (n - 7, 0), (0, n - 7)):
        for r in range(-1, 8):
            for c in range(-1, 8):
                if not (0 <= row + r < n and 0 <= col + c < n):
                    continue
                m[row + r][col + c] = (
                    (0 <= r <= 6 and c in (0, 6))
                    or (0 <= c <= 6 and r in (0, 6))
                    or (2 <= r <= 4 and 2 <= c <= 4)
                )

    pos = ALIGN[version]
    for row in pos:
        for col in pos:
            if m[row][col] is not None:
                continue
            for r in range(-2, 3):
                for c in range(-2, 3):
                    m[row + r][col + c] = abs(r) == 2 or abs(c) == 2 or (r == 0 and c == 0)

    for i in range(8, n - 8):
        if m[i][6] is None:
            m[i][6] = i % 2 == 0
        if m[6][i] is None:
            m[6][i] = i % 2 == 0

    bits = _bch_format(mask)  # уровень M = 00, поэтому старшие биты нулевые
    for i in range(15):
        mod = (not test) and ((bits >> i) & 1) == 1
        if i < 6:
            m[i][8] = mod
        elif i < 8:
            m[i + 1][8] = mod
        else:
            m[n - 15 + i][8] = mod
    for i in range(15):
        mod = (not test) and ((bits >> i) & 1) == 1
        if i < 8:
            m[8][n - i - 1] = mod
        elif i < 9:
            m[8][15 - i] = mod
        else:
            m[8][15 - i - 1] = mod
    m[n - 8][8] = not test

    if version >= 7:
        vbits = _bch_version(version)
        for i in range(18):
            mod = (not test) and ((vbits >> i) & 1) == 1
            m[i // 3][i % 3 + n - 11] = mod
            m[i % 3 + n - 11][i // 3] = mod

    inc = -1
    row = n - 1
    bit_index = 7
    byte_index = 0
    length = len(words)
    col = n - 1
    while col > 0:
        if col == 6:
            col -= 1
        while True:
            for c in (col, col - 1):
                if m[row][c] is not None:
                    continue
                dark = byte_index < length and ((words[byte_index] >> bit_index) & 1) == 1
                if _mask_bit(mask, row, c):
                    dark = not dark
                m[row][c] = dark
                bit_index -= 1
                if bit_index == -1:
                    byte_index += 1
                    bit_index = 7
            row += inc
            if row < 0 or row >= n:
                row -= inc
                inc = -inc
                break
        col -= 2
    return m


def _penalty(m: list[list[bool]]) -> int:
    """Штраф маски — четыре правила стандарта, в той же арифметике, что у эталона."""
    n = len(m)
    runs = [0] * (n + 1)
    for a in range(2):
        for i in range(n):
            prev = m[i][0] if a == 0 else m[0][i]
            length = 0
            for j in range(n):
                cur = m[i][j] if a == 0 else m[j][i]
                if cur == prev:
                    length += 1
                else:
                    if length >= 5:
                        runs[length] += 1
                    length = 1
                    prev = cur
            if length >= 5:
                runs[length] += 1
    score = sum(runs[l] * (l - 2) for l in range(5, n + 1))

    for r in range(n - 1):
        for c in range(n - 1):
            v = m[r][c]
            if m[r][c + 1] == v and m[r + 1][c] == v and m[r + 1][c + 1] == v:
                score += 3

    p1 = (True, False, True, True, True, False, True, False, False, False, False)
    p2 = tuple(reversed(p1))
    for a in range(2):
        for i in range(n):
            for j in range(n - 10):
                hit1 = hit2 = True
                for k in range(11):
                    if not (hit1 or hit2):
                        break
                    v = m[i][j + k] if a == 0 else m[j + k][i]
                    hit1 = hit1 and v == p1[k]
                    hit2 = hit2 and v == p2[k]
                if hit1 or hit2:
                    score += 40

    dark = sum(1 for row in m for v in row if v)
    score += int(abs(dark / (n * n) * 100 - 50) / 5) * 10
    return score


def qr_matrix(data: str, mask: int | None = None) -> list[list[bool]]:
    """Матрица модулей без поля тишины: True — тёмный модуль.
    mask=None — маска по штрафам стандарта, 0-7 — заданная (для сверки)."""
    data_bytes = data.encode("utf-8")
    version = _pick_version(len(data_bytes))
    words = _codewords(data_bytes, version)
    if mask is None:
        best = None
        for candidate in range(8):
            # Как в эталоне: штраф считаем с пустыми (светлыми) служебными полями.
            penalty = _penalty(_build(words, version, candidate, True))
            if best is None or penalty < best[0]:
                best = (penalty, candidate)
        mask = best[1]
    return _build(words, version, mask, False)


def qr_svg(data: str, title: str = "") -> str:
    """SVG с полем тишины в 4 модуля. Размер задаёт CSS, viewBox — в модулях."""
    m = qr_matrix(data)
    n = len(m)
    q = 4
    size = n + 2 * q
    parts = []
    for r in range(n):
        c = 0
        while c < n:
            if not m[r][c]:
                c += 1
                continue
            start = c
            while c < n and m[r][c]:
                c += 1
            parts.append(f"M{start + q} {r + q}h{c - start}v1h-{c - start}z")
    label = html.escape(title, quote=True)
    head = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" shape-rendering="crispEdges" role="img"'
    if label:
        head += f' aria-label="{label}"'
    return (head + ">" + (f"<title>{label}</title>" if label else "")
            + f'<rect width="{size}" height="{size}" fill="#fff"/>'
            + f'<path d="{"".join(parts)}" fill="#000"/></svg>')
