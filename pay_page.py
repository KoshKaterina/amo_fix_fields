"""Своя страница оплаты счёта из amo: https://pay.sunscrypt.ru/o/<номер> (26.11.2026).

ЗАЧЕМ. Прямая ссылка НСПК qr.nspk.ru с 25.11.2026 стоит на сертификате Минцифры, и Chrome,
Samsung Browser и Safari её не открывают. Страница Ozon (checkout.ozon.ru) открывается, но это
чужая страница с выбором карты. Своя ссылка ведёт себя как страница оплаты сайта (sunscrypt-sbp
0.8.x, решение Кати 25.11): с компьютера — QR и «Оплатите заказ онлайн», без слова «СБП»;
с телефона — сразу на страницу оплаты Ozon. Если Ozon или НСПК снова что-то поменяют, чиним
страницу, а не ссылки во всех отправленных сообщениях.

КАК. Счёт создаётся как раньше: платёж СБП вместе с заказом Ozon (ozon_invoice._create_payment).
Из ответа берём ссылку Ozon и код QR НСПК, кладём в свою базу под случайным номером и отдаём
клиенту ссылку на номер. Подписи нет и не нужно: страница показывает только то, что лежит в
нашей базе под этим номером, чужой QR через неё не подсунуть.

БЕЗОПАСНОСТЬ.
  • номер — secrets.token_urlsafe(12): 16 символов, 96 бит, не перебрать;
  • на странице только номер заказа МС и сумма — ни имени, ни телефона, ни адреса;
  • срок жизни = срок счёта Ozon (OZON_INVOICE_TTL_S);
  • частоту запросов режет nginx (зона pay_page), номер в журналах скрыт (nginx и uvicorn);
  • страница отвечает только на имени pay.sunscrypt.ru: через team.sunscrypt.ru/amo/o/… — 404;
  • Referrer-Policy: no-referrer — номер не уходит в Ozon заголовком Referer.

СТАТУС. «Оплачено» страница узнаёт сама: getOrderStatus по extId заказа, не чаще раза в
STATUS_RECHECK_S на ссылку. Сделку страница НЕ двигает — это делают вебхук и сверка
(ozon_invoice), как и раньше.
"""
from __future__ import annotations

import datetime
import html
import logging
import re
import secrets
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

import pay_qr

logger = logging.getLogger("uvicorn")

_DB = Path(__file__).parent / "var" / "pay_page.sqlite3"
_LOGO = (Path(__file__).parent / "pay_page_assets" / "logo.svg").read_text(encoding="utf-8")

PAY_HOST = "pay.sunscrypt.ru"
STATUS_RECHECK_S = 10
TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
PAYLOAD_RE = re.compile(r"^https://qr\.nspk\.ru/[A-Za-z0-9]")
CHECKOUT_RE = re.compile(r"^https://([a-z0-9-]+\.)*ozon\.ru/", re.I)
_MSK = datetime.timezone(datetime.timedelta(hours=3))

# Телефон — как wp_is_mobile() на сайте: та же развилка, что у страницы оплаты сайта.
_MOBILE_MARKERS = ("Mobile", "Android", "Silk/", "Kindle", "BlackBerry", "Opera Mini", "Opera Mobi")

router = APIRouter()


# ---------------------------------------------------------------------------
# Хранение
# ---------------------------------------------------------------------------

def _connection() -> sqlite3.Connection:
    _DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE IF NOT EXISTS links ("
        " token TEXT PRIMARY KEY, lead_id INTEGER NOT NULL, ext_id TEXT NOT NULL,"
        " order_ext_id TEXT NOT NULL, pipeline_id INTEGER, amount INTEGER NOT NULL,"
        " order_name TEXT NOT NULL DEFAULT '', sbp_payload TEXT NOT NULL, checkout_url TEXT NOT NULL,"
        " created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,"
        " state TEXT NOT NULL DEFAULT 'pending', checked_at INTEGER NOT NULL DEFAULT 0, paid_at INTEGER)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS links_lead ON links(lead_id)")
    return conn


def create_link(
    *, lead_id: int, ext_id: str, order_ext_id: str, pipeline_id, amount_kopecks: int,
    order_name: str, sbp_payload: str, checkout_url: str, ttl_s: int,
) -> str:
    """Сохранить счёт и вернуть номер ссылки. Неполные данные — ValueError: тогда
    ozon_invoice отдаёт клиенту ссылку Ozon, как без страницы."""
    if not PAYLOAD_RE.match(sbp_payload or ""):
        raise ValueError("нет кода QR НСПК")
    if not CHECKOUT_RE.match(checkout_url or ""):
        raise ValueError("нет ссылки на страницу Ozon")
    if not order_ext_id or int(amount_kopecks) <= 0:
        raise ValueError("нет номера заказа Ozon или суммы")
    now = int(time.time())
    token = secrets.token_urlsafe(12)
    with closing(_connection()) as conn, conn:
        conn.execute(
            "INSERT INTO links (token, lead_id, ext_id, order_ext_id, pipeline_id, amount, order_name,"
            " sbp_payload, checkout_url, created_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (token, int(lead_id), ext_id, order_ext_id, int(pipeline_id) if pipeline_id else None,
             int(amount_kopecks), order_name or "", sbp_payload, checkout_url, now, now + int(ttl_s)),
        )
    return token


def get_link(token: str) -> dict | None:
    if not TOKEN_RE.match(token or ""):
        return None
    with closing(_connection()) as conn:
        row = conn.execute("SELECT * FROM links WHERE token = ?", (token,)).fetchone()
    return dict(row) if row else None


def _update(token: str, **fields) -> None:
    cols = ", ".join(f"{k} = ?" for k in fields)
    with closing(_connection()) as conn, conn:
        conn.execute(f"UPDATE links SET {cols} WHERE token = ?", (*fields.values(), token))


def effective_state(link: dict, now: float | None = None) -> str:
    """pending | paid | expired. Оплата сильнее срока: оплатил в последнюю минуту — оплачено."""
    if link["state"] == "paid":
        return "paid"
    if link["state"] == "closed" or (now or time.time()) >= link["expires_at"]:
        return "expired"
    return "pending"


async def refresh_state(link: dict) -> str:
    """Спросить Ozon о заказе, если давно не спрашивали. Ошибка Ozon — остаёмся при своём:
    страница не должна падать оттого, что банк не ответил."""
    state = effective_state(link)
    if state != "pending":
        return state
    now = int(time.time())
    if now - int(link["checked_at"] or 0) < STATUS_RECHECK_S:
        return state
    # Отметку ставим ДО запроса: параллельные опросы той же ссылки не ходят в Ozon гурьбой.
    _update(link["token"], checked_at=now)
    link["checked_at"] = now

    import ozon_invoice  # здесь, а не наверху: ozon_invoice сам импортирует этот модуль

    access_key, secret_key, _notify = ozon_invoice._keys_for_pipeline(link["pipeline_id"])
    data, err = await ozon_invoice._order_status_request(link["order_ext_id"], access_key, secret_key)
    if err:
        logger.info("pay_page: статус заказа lead %s не получен (%s)", link["lead_id"], err[:120])
        return state
    status, kopecks = ozon_invoice._extract_order_status(data or {}, link["order_ext_id"])
    if ozon_invoice.is_paid_status(status):
        if kopecks != int(link["amount"]):
            logger.warning("pay_page: lead %s оплачен на %s коп. вместо %s — «Оплачено» не показываем",
                           link["lead_id"], kopecks, link["amount"])
            return state
        _update(link["token"], state="paid", paid_at=now)
        link["state"], link["paid_at"] = "paid", now
        return "paid"
    if status.upper() in {"STATUS_CANCELED", "STATUS_CANCELLED", "STATUS_EXPIRED"}:
        _update(link["token"], state="closed")
        link["state"] = "closed"
        return "expired"
    return state


# ---------------------------------------------------------------------------
# Страница
# ---------------------------------------------------------------------------

def _rub(kopecks: int) -> str:
    rub, kop = divmod(int(kopecks), 100)
    text = f"{rub:,}".replace(",", " ")
    return f"{text},{kop:02d} ₽" if kop else f"{text} ₽"


def _until(ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=_MSK)
    return dt.strftime("%d.%m в %H:%M")


def _what(link: dict) -> str:
    name = html.escape(link.get("order_name") or "")
    return f"Заказ №{name}" if name else "Счёт"


def _heading(link: dict | None) -> str:
    """Заголовок страницы — что оплачиваем; состояние говорит блок под ним."""
    if link is None:
        return "Оплата заказа"
    name = html.escape(link.get("order_name") or "")
    return f"Оплата заказа №{name}" if name else "Оплата счёта"


_CONTACT = ('позвоните по номеру <a href="tel:+79056063678">+7&nbsp;(905)&nbsp;606-36-78</a> или напишите в '
            '<a href="https://t.me/Sunscrypt_manager" target="_blank" rel="noopener noreferrer">Телеграм</a>.')

_IC_QR = ('<svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true"><rect x="3.5" y="3.5" width="6.5" height="6.5" rx="1.5" stroke="#fff" stroke-width="2"/>'
          '<rect x="14" y="3.5" width="6.5" height="6.5" rx="1.5" stroke="#fff" stroke-width="2"/><rect x="3.5" y="14" width="6.5" height="6.5" rx="1.5" stroke="#fff" stroke-width="2"/>'
          '<path d="M14 14h2.5v2.5H14zM18 18h2.5v2.5H18zM18 14h2.5v2.5H18zM14 18h2.5v2.5H14z" fill="#fff"/></svg>')
_IC_CHECK = ('<svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M5 12.5l4.2 4.2L19 7" stroke="#fff" stroke-width="2.6" '
             'stroke-linecap="round" stroke-linejoin="round"/></svg>')
_IC_ALERT = ('<svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M12 7.5v6" stroke="#fff" stroke-width="2.6" stroke-linecap="round"/>'
             '<circle cx="12" cy="17" r="1.5" fill="#fff"/></svg>')

# Стили страницы оплаты сайта (sunscrypt-sbp 0.8.x), без темы WordPress вокруг.
_STYLE = """
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;min-height:100vh;display:flex;flex-direction:column;background:#fff;color:#0f141e;
font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}
header{border-bottom:1px solid #e1e6f0}
header .in,main,footer .in{width:100%;max-width:720px;margin:0 auto;padding-left:24px;padding-right:24px}
header .in{padding-top:18px;padding-bottom:18px}
header a{display:inline-block;line-height:0}
header svg{display:block;width:170px;height:auto}
main{flex:1 0 auto;padding-top:40px;padding-bottom:40px}
h1{font-size:28px;line-height:1.2;letter-spacing:-.01em;margin:0 0 24px}
.box{display:flex;gap:15px;align-items:flex-start;border:1px solid #e1e6f0;border-radius:16px;padding:20px 22px;background:#f0f5fa}
.ic{width:42px;height:42px;border-radius:12px;flex:0 0 auto;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#0164fa,#0046b0)}
.ic svg{display:block}
.body{flex:1 1 auto;min-width:0}
.t{font-size:19px;font-weight:700;letter-spacing:-.01em;line-height:1.2;margin:0 0 7px}
p{margin:7px 0 0;font-size:15px;line-height:1.5;color:#37414d}
.t+p{margin-top:0}
a{color:#0164fa;font-weight:600;text-decoration:underline;text-underline-offset:2px}
.paid .box{background:#edf3ff;border-color:#cfe0ff}
.expired .box{background:#fdf1ec;border-color:#f6d3c4}
.expired .ic{background:#ed5c2a}
.actions{display:flex;flex-direction:column;gap:10px;margin:16px 0 0}
.btn{display:block;width:100%;margin:0;padding:14px 18px;border-radius:12px;background:#0164fa;border:1.5px solid #0164fa;color:#fff;
font:inherit;font-size:16px;font-weight:700;line-height:1.25;text-align:center;text-decoration:none}
.btn:focus-visible,a:focus-visible{outline:3px solid #9cc2ff;outline-offset:2px}
.qr-wrap{display:flex;gap:22px;align-items:center;margin:18px 0 0}
.qr{flex:0 0 auto;width:232px;height:232px;padding:6px;background:#fff;border:1px solid #e1e6f0;border-radius:12px}
.qr svg{display:block;width:100%;height:100%}
.qr-text{flex:1 1 auto;min-width:0}
.qr-text p{margin:0}
.wait{display:flex;align-items:center;gap:10px;margin-top:16px}
.spin{width:16px;height:16px;flex:0 0 auto;border:2.5px solid #cdd8ea;border-top-color:#0164fa;border-radius:50%;display:inline-block;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.note{font-size:13px;line-height:1.45;color:#5b6676}
footer{border-top:1px solid #e1e6f0;color:#5b6676;font-size:13px;line-height:1.6}
footer .in{padding-top:20px;padding-bottom:24px}
footer p{margin:0;font-size:13px;color:#5b6676}
footer a{color:inherit;font-weight:400}
@media (hover:hover) and (pointer:fine){.touch{display:none}}
@media (hover:none),(pointer:coarse){.pointer{display:none}}
@media (max-width:600px){main{padding-top:28px}h1{font-size:24px}.box>.ic{display:none}
.qr-wrap{flex-direction:column;align-items:stretch;gap:12px}.qr{margin:0 auto}}
@media (prefers-reduced-motion:reduce){.spin{animation:none}}
"""

# Опрос статуса, как у страницы сайта: 3 минуты раз в 4 с, дальше раз в 10 с, всего 20 минут;
# вернулись из приложения банка — проверяем сразу.
_SCRIPT = """
(function(){
  var url = location.pathname.replace(/\\/$/, '') + '/status';
  var started = Date.now(), limit = 20 * 60 * 1000, timer = null, busy = false, done = false;
  function delay(){ return Date.now() - started < 3 * 60 * 1000 ? 4000 : 10000; }
  function giveUp(){
    done = true;
    var w = document.querySelector('.wait-text'); if (w) { w.textContent = 'Пока не видим оплату. Если вы уже оплатили, обновите страницу через минуту.'; }
    var s = document.querySelector('.wait .spin'); if (s) { s.style.display = 'none'; }
  }
  function next(){ if (done) { return; } if (Date.now() - started >= limit) { giveUp(); return; } clearTimeout(timer); timer = setTimeout(poll, delay()); }
  function poll(){
    if (busy || done) { return; }
    busy = true;
    fetch(url, {cache: 'no-store', credentials: 'omit'})
      .then(function(r){ return r.json(); })
      .then(function(d){ busy = false; if (d && d.state && d.state !== 'pending') { done = true; location.reload(); return; } next(); })
      .catch(function(){ busy = false; next(); });
  }
  document.addEventListener('visibilitychange', function(){ if (document.visibilityState === 'visible' && !done) { clearTimeout(timer); poll(); } });
  timer = setTimeout(poll, delay());
})();
"""

_FOOTER = ('<footer aria-label="Реквизиты получателя"><div class="in">'
           '<p>Получатель: ИП Перфилов Андрей Владимирович, ИНН 773423812500, ОГРНИП 322774600771372</p>'
           '<p><a href="https://sunscrypt.ru/about/" target="_blank" rel="noopener noreferrer">Все реквизиты</a></p>'
           '</div></footer>')


def render(link: dict | None, state: str) -> str:
    """HTML страницы. link=None — ссылка не найдена."""
    poll = False
    if link is None:
        title = "Ссылка не найдена"
        box = ('<div class="box"><span class="ic">' + _IC_ALERT + '</span><div class="body">'
               '<div class="t">Ссылка на оплату не найдена</div>'
               '<p>Проверьте, что ссылка скопирована целиком. Чтобы получить новую, ' + _CONTACT + '</p></div></div>')
        state = "expired"
    elif state == "paid":
        title = "Оплата получена"
        box = ('<div class="box"><span class="ic">' + _IC_CHECK + '</span><div class="body">'
               '<div class="t">Оплата получена</div>'
               f'<p>Спасибо! {_what(link)} на сумму {_rub(link["amount"])} оплачен.</p>'
               '<p>Менеджер свяжется с вами, чтобы подтвердить детали. Если есть вопросы, ' + _CONTACT + '</p></div></div>')
    elif state == "expired":
        title = "Срок оплаты истёк"
        box = ('<div class="box"><span class="ic">' + _IC_ALERT + '</span><div class="body">'
               '<div class="t">Срок ссылки на оплату истёк</div>'
               f'<p>{_what(link)} на сумму {_rub(link["amount"])} по этой ссылке уже не оплатить.</p>'
               '<p>Чтобы получить новую ссылку, ' + _CONTACT + '</p></div></div>')
    else:
        poll = True
        title = "Оплатите заказ онлайн"
        qr = pay_qr.svg(link["sbp_payload"], "QR-код для оплаты")
        box = ('<div class="box"><span class="ic">' + _IC_QR + '</span><div class="body">'
               '<div class="t">Оплатите заказ онлайн</div>'
               f'<p>{_what(link)} на сумму {_rub(link["amount"])}. Осталось оплатить.</p>'
               '<div class="actions touch"><a class="btn" href="' + html.escape(link["checkout_url"], quote=True)
               + '" rel="noopener noreferrer">Оплатить онлайн</a></div>'
               '<div class="qr-wrap pointer"><div class="qr">' + qr + '</div><div class="qr-text">'
               '<p>Наведите камеру телефона на QR-код или отсканируйте его в приложении банка.</p></div></div>'
               '<p class="wait"><span class="spin" aria-hidden="true"></span>'
               '<span class="wait-text" role="status" aria-live="polite">Ждём оплату. После оплаты страница обновится сама.</span></p>'
               f'<p class="note">Ссылка на оплату действует до {_until(link["expires_at"])}. '
               'Не получается оплатить? Чтобы связаться с нами, ' + _CONTACT + '</p></div></div>')
    return ('<!doctype html><html lang="ru"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta name="robots" content="noindex,nofollow"><meta name="referrer" content="no-referrer">'
            f'<title>{title} — Sunscrypt</title><style>{_STYLE}</style></head>'
            f'<body class="{state}"><header><div class="in"><a href="https://sunscrypt.ru/" rel="noopener noreferrer">{_LOGO}</a></div></header>'
            f'<main><h1>{_heading(link)}</h1>{box}</main>{_FOOTER}'
            + (f"<script>{_SCRIPT}</script>" if poll else "")
            + '</body></html>')


_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Robots-Tag": "noindex, nofollow",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:;"
        " connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
}


def _is_pay_host(request: Request) -> bool:
    """Страница живёт только на pay.sunscrypt.ru: Host выставляет nginx этого имени. Через
    team.sunscrypt.ru/amo/o/… (там те же порты сервиса) — 404, мимо ограничения частоты не пройти."""
    return (request.headers.get("host") or "").split(":")[0].lower() == PAY_HOST


def is_mobile(user_agent: str) -> bool:
    return any(marker in (user_agent or "") for marker in _MOBILE_MARKERS)


@router.get("/o/{token}/status")
async def page_status(token: str, request: Request):
    if not _is_pay_host(request):
        return Response(status_code=404)
    link = get_link(token)
    if link is None:
        return JSONResponse({"state": "missing"}, status_code=404, headers=_SECURITY_HEADERS)
    return JSONResponse({"state": await refresh_state(link)}, headers=_SECURITY_HEADERS)


@router.get("/o/{token}")
async def page(token: str, request: Request):
    if not _is_pay_host(request):
        return Response(status_code=404)
    link = get_link(token)
    if link is None:
        return HTMLResponse(render(None, "expired"), status_code=404, headers=_SECURITY_HEADERS)
    state = await refresh_state(link)
    # Телефон с неоплаченным счётом — сразу на страницу оплаты Ozon, без QR (решение Кати 25.11).
    if state == "pending" and is_mobile(request.headers.get("user-agent", "")):
        return RedirectResponse(link["checkout_url"], status_code=302, headers=_SECURITY_HEADERS)
    return HTMLResponse(render(link, state), headers=_SECURITY_HEADERS)
