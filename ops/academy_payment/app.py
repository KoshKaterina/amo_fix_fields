"""Isolated, fixed-price Academy payment redirect; no CRM/Woo imports or writes."""
import asyncio
import hashlib
import hmac
import html
import os
import re
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

PREFIX = "/academy-pay"
ORIGIN = "https://team.sunscrypt.ru"
AMOUNT = 1_000_000  # 10 000 RUB; same units as Woo and ozon_invoice.
TTL = 86400
COOKIE = "academy_deposit_session"
ACCESS = os.environ.get("OZON_PAY_ACCESS_KEY", "")
SECRET = os.environ.get("OZON_PAY_SECRET_KEY", "")
NOTIFY_SECRET = os.environ.get("OZON_PAY_NOTIFICATION_SECRET_KEY", "")
SESSION_SECRET = os.environ.get("ACADEMY_PAYMENT_SESSION_SECRET", "")
DB_PATH = Path(os.environ.get("ACADEMY_PAYMENT_DB", "/data/payments.sqlite3"))
lock = asyncio.Lock()


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


def page(title, message, session, returning=False):
    # This is a redirect/status page, not the external Academy contact form.
    content = f"""<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<meta name="csrf" content="{csrf(session)}"><title>{html.escape(title)} — Sunscrypt</title>
<style>body{{margin:0;background:#fff;color:#151b26;font:17px/1.5 system-ui,sans-serif}}
main{{max-width:520px;padding:40px 24px;margin:8vh auto}}h1{{font-size:28px;line-height:1.2;text-wrap:balance}}
p{{max-width:60ch}}button,a{{font:inherit}}button{{min-height:48px;border:0;border-radius:8px;padding:12px 20px;background:#0164fa;color:#fff;cursor:pointer}}
button:disabled{{opacity:.6}}a{{color:#0149bf}}:focus-visible{{outline:3px solid #151b26;outline-offset:4px}}</style>
<main data-return="{str(returning).lower()}"><p>Sunscrypt Академия</p><h1>{html.escape(title)}</h1>
<p>Четвёртый поток «Инвестиции в DeFi»</p><p id="status" role="status">{html.escape(message)}</p>
<form method="post" action="{PREFIX}/create"><input type="hidden" name="csrf" value="{csrf(session)}">
<button id="pay" type="submit">Перейти к оплате 10 000 ₽</button></form>
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
    if row and row["status"] == "pending" and row["created"] <= time.time() - TTL:
        session = secrets.token_hex(32)
        row = None
    if row and row["status"] == "paid":
        return page("Предоплата получена", "Спасибо! Менеджер свяжется с вами.", session, returning=True)
    return page("Предоплата 10 000 ₽", "Открываем страницу оплаты Ozon…", session)


@app.get(PREFIX + "/payment.js")
async def script():
    return Response("""const button=document.querySelector('#pay'), status=document.querySelector('#status');
const returning=document.querySelector('main').dataset.return==='true';
async function start(e){if(e)e.preventDefault();button.disabled=true;
try{const r=await fetch('/academy-pay/create',{method:'POST',headers:{'X-Academy-CSRF':document.querySelector('meta[name=csrf]').content,'Accept':'application/json'}});
const d=await r.json();if(!r.ok)throw new Error(d.detail||'Оплата временно недоступна');
if(d.paid){status.textContent='Предоплата получена. Менеджер свяжется с вами.';button.hidden=true;return;}
window.location.replace(d.url);}catch(e){status.textContent=e.message;button.disabled=false;}}
document.querySelector('form').addEventListener('submit',start);
if(!returning)start();else{button.hidden=true;let count=0;
async function poll(){try{const r=await fetch('/academy-pay/status');const d=await r.json();
if(d.paid){document.querySelector('h1').textContent='Предоплата получена';status.textContent='Спасибо! Менеджер свяжется с вами.';return;}}
catch(e){}if(++count<15)setTimeout(poll,2000);else status.textContent='Подтверждение оплаты пока не получено. Если деньги списались, повторно не оплачивайте — свяжитесь с менеджером.';}poll();}
""", media_type="application/javascript")


def body_for(ext_id, order_ext_id):
    amount = {"currencyCode": "643", "value": str(AMOUNT)}
    return {
        "accessKey": ACCESS, "payType": "SBP", "amount": amount,
        "extId": ext_id, "requestSign": signature(ext_id), "ttl": TTL,
        "redirectUrl": ORIGIN + PREFIX + "/return",
        "notificationUrl": ORIGIN + PREFIX + "/notify",
        "order": {
            "extId": order_ext_id, "amount": amount,
            "paymentAlgorithm": "PAY_ALGO_SMS", "mode": "MODE_SHORTENED",
            "expiresAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() + TTL)),
            "successUrl": ORIGIN + PREFIX + "/return",
            "failUrl": ORIGIN + PREFIX + "/return", "enableFiscalization": False,
        },
    }


async def create_ozon(body):
    async with httpx.AsyncClient(timeout=25) as client:
        # No automatic retry: a timeout may already have created the invoice.
        response = await client.post("https://payapi.ozon.ru/v1/createPayment", json=body)
        response.raise_for_status()
        return response.json()


def safe_link(data):
    order = data.get("order") or {}
    link = (order.get("item") or {}).get("payLink") or order.get("payLink") or ""
    url = urlsplit(link)
    if url.scheme != "https" or url.hostname != "checkout.ozon.ru" or url.username or url.password:
        raise ValueError("Invalid payment redirect")
    return link


@app.post(PREFIX + "/create")
async def create(request: Request):
    if request.headers.get("origin") != ORIGIN:
        raise HTTPException(403, "Откройте ссылку на оплату заново")
    session = valid_session(request)
    token = request.headers.get("X-Academy-CSRF", "")
    is_form = request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded")
    if is_form:
        from urllib.parse import parse_qs
        raw = await request.body()
        if len(raw) > 1024:
            raise HTTPException(413, "Запрос слишком большой")
        token = parse_qs(raw.decode()).get("csrf", [""])[0]
    if not hmac.compare_digest(token, csrf(session)):
        raise HTTPException(403, "Откройте ссылку на оплату заново")
    async with lock:
        row = payment(session)
        if row:
            if row["status"] == "paid":
                return JSONResponse({"paid": True})
            if row["status"] == "pending" and row["created"] > time.time() - TTL:
                link = row["pay_link"]
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
                conn.execute("INSERT INTO payments(session,ext_id,order_ext_id,status,created,ip_hash) VALUES(?,?,?,?,?,?)",
                             (session_hash(session), ext_id, order_ext_id, "creating", now, ip_hash))
            try:
                data = await create_ozon(body_for(ext_id, order_ext_id))
                link = safe_link(data)
                pid = str((data.get("paymentDetails") or {}).get("paymentId") or "")
                with db() as conn:
                    conn.execute("UPDATE payments SET payment_id=?,pay_link=?,status='pending' WHERE session=? AND status='creating'", (pid, link, session_hash(session)))
            except (httpx.HTTPError, ValueError, TypeError, KeyError):
                # Keep the durable attempt; do not silently issue another bill.
                raise HTTPException(502, "Не удалось открыть оплату. Свяжитесь с менеджером.") from None
    if is_form:
        return Response(status_code=303, headers={"Location": link})
    return JSONResponse({"url": link})


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


@app.post(PREFIX + "/notify")
async def notify(request: Request):
    raw = await request.body()
    if len(raw) > 16384:
        raise HTTPException(413)
    try:
        import json
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
    column, identifier = identity
    with db() as conn:
        # No pattern extraction of CRM ids. Only an exact, locally issued bill.
        conn.execute(f"UPDATE payments SET status='paid',paid_at=COALESCE(paid_at,?) WHERE {column}=?",
                     (int(time.time()), identifier))
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


@app.get(PREFIX + "/status")
async def status(request: Request):
    row = payment(valid_session(request))
    return {"paid": bool(row and row["status"] == "paid")}
