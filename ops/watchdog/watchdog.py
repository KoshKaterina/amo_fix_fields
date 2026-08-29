#!/usr/bin/env python3
"""Сторож Sunscrypt: пишет Кате в личный Telegram, когда нужно её вмешательство.

Пять проверок, каждая запускается своим кроном:

    watchdog.py tokens     - живы ли доступы к внешним системам (раз в час)
    watchdog.py deadlines  - задачи YouGile: просроченные, сегодня, завтра (утром)
    watchdog.py renewals   - домены и хостинг: скоро продлевать (утром)
    watchdog.py defi       - жив ли прокси, через который живёт DeFi-дашборд (раз в полчаса)
    watchdog.py services   - упавшие юниты, мёртвые контейнеры, выключенные таймеры (каждые 5 минут)

Правила против спама:
  • tokens шлёт письмо на переходе «работал → сломался» и «сломался → починился»;
    пока сломано, напоминает раз в 6 часов, не чаще;
  • renewals по каждому домену пишет не чаще раза в 3 дня и только при пересечении
    порогов 30/14/7/3/1 день;
  • deadlines - одна сводка в день, и только если есть о чём писать;
  • services живёт по тому же правилу, что и tokens: одно письмо на падение,
    напоминание раз в 6 часов, отдельное письмо про восстановление.

Состояние - в state.json рядом со скриптом. Секреты только читаются из .env,
никуда не печатаются.
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, "state.json")
MSK = timezone(timedelta(hours=3))


# --------------------------------------------------------------------------- env
def parse_env_file(path):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = parse_env_file(os.path.join(HERE, ".env"))
ENV.update({k: v for k, v in os.environ.items() if k.isupper()})
# токены боевых интеграций живут в своём .env - не дублируем, читаем оттуда
ENV_INTEGRATION = parse_env_file(ENV.get("INTEGRATION_ENV", "/opt/integrations/amo_fix_fields/.env"))


def secret(name):
    return ENV.get(name) or ENV_INTEGRATION.get(name) or ""


def log(msg):
    print(f"[{datetime.now(MSK).strftime('%H:%M:%S')}] {msg}", flush=True)


# ------------------------------------------------------------------------ state
def load_state():
    try:
        return json.load(open(STATE_PATH, encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    json.dump(state, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_PATH)


# --------------------------------------------------------------------- telegram
def curl_cfg(key, value):
    """Строка конфига curl. Всё секретное отдаём curl через stdin, а не аргументами:
    argv любого процесса читает `ps` от имени ЛЮБОГО пользователя машины, а stdin
    виден только самому процессу. Значение берём в кавычки - внутри них curl понимает
    escape-последовательности, поэтому обратный слэш, кавычку и перевод строки экранируем."""
    v = (str(value).replace("\\", "\\\\").replace('"', '\\"')
         .replace("\n", "\\n").replace("\r", "\\r"))
    return f'{key} = "{v}"'


def curl_run(config, args, timeout):
    """curl с секретной частью запроса из stdin. `-K -` ставим ПОСЛЕДНИМ: так конфиг
    читается после флагов, и --globoff успевает отключить глоббинг до разбора URL."""
    return subprocess.run(["curl"] + args + ["-K", "-"],
                          input="\n".join(config) + "\n",
                          capture_output=True, text=True, timeout=timeout)


def tg_send(text):
    token, chat = secret("TG_BOT_TOKEN"), secret("TG_ALLOWED_CHAT_ID")
    if not token or not chat:
        log("некуда слать: нет TG_BOT_TOKEN или TG_ALLOWED_CHAT_ID")
        return False
    # токен бота сидит в самом URL, а пароль шлюза - в строке прокси: оба в stdin
    cfg = [curl_cfg("url", f"https://api.telegram.org/bot{token}/sendMessage")]
    proxy = secret("TG_PROXY_URL")
    if proxy:  # Telegram из РФ - только через наш шлюз
        cfg.append(curl_cfg("proxy", proxy))
    args = ["-s", "--max-time", "30", "-o", "/dev/null", "-w", "%{http_code}",
            "-d", f"chat_id={chat}", "--data-urlencode", f"text={text}"]
    p = curl_run(cfg, args, timeout=60)
    ok = (p.stdout or "").strip() == "200"
    if not ok:
        log(f"telegram не принял: {p.stdout!r}")
    return ok


# ------------------------------------------------------------------------- http
def http(url, headers=None, method="GET", data=None, timeout=30):
    """Возвращает (код, тело). URL, заголовки и тело уходят в curl через stdin: токены
    живут во всех трёх (Bearer в заголовке, ключи Woo прямо в URL, секрет СДЭК в теле),
    а аргументы командной строки видны в `ps` всем. В лог они тоже не попадают."""
    cfg = [curl_cfg("url", url)]
    for h in headers or []:
        cfg.append(curl_cfg("header", h))
    if data:
        cfg.append(curl_cfg("data", data))
    # --location нужен RDAP: rdap.org отвечает редиректом на сервер нужной зоны
    args = ["-s", "--globoff", "--compressed", "--location", "--max-time", str(timeout),
            "-w", "\n%{http_code}"]
    if method != "GET":
        args += ["-X", method]
    p = curl_run(cfg, args, timeout=timeout + 30)
    out = p.stdout or ""
    nl = out.rfind("\n")
    body, code = (out[:nl], out[nl + 1:].strip()) if nl >= 0 else ("", out.strip())
    return (int(code) if code.isdigit() else 0), body


# ===================================================================== проверка 1
def check_tokens():
    """Живы ли доступы. Возвращает список (имя, ok, пояснение)."""
    checks = []

    def add(name, code, body, ok_codes=(200,), hint=""):
        ok = code in ok_codes
        why = "" if ok else f"HTTP {code or 'нет ответа'}"
        if not ok and body:
            snippet = re.sub(r"\s+", " ", body)[:90]
            why += f" · {snippet}"
        checks.append((name, ok, why or hint))

    amo_base = ENV.get("AMO_BASE", "https://new5a2e8ea7b16b4.amocrm.ru").rstrip("/")
    if secret("TOKEN"):
        c, b = http(f"{amo_base}/api/v4/account", [f"Authorization: Bearer {secret('TOKEN')}"])
        add("amoCRM", c, b)

    if secret("MS_TOKEN"):
        c, b = http("https://api.moysklad.ru/api/remap/1.2/context/employee",
                    [f"Authorization: Bearer {secret('MS_TOKEN')}"])
        add("МойСклад", c, b)

    if secret("WC_URL") and secret("WC_CONSUMER_KEY"):
        url = (f"{secret('WC_URL').rstrip('/')}/wp-json/wc/v3/orders?per_page=1"
               f"&consumer_key={secret('WC_CONSUMER_KEY')}&consumer_secret={secret('WC_CONSUMER_SECRET')}")
        c, b = http(url)
        add("WooCommerce (sunscrypt.ru)", c, b)

    if secret("WAZZUP_API_KEY"):
        c, b = http("https://api.wazzup24.com/v3/channels",
                    [f"Authorization: Bearer {secret('WAZZUP_API_KEY')}"])
        add("Wazzup", c, b)

    if secret("CDEK_CLIENT_ID"):
        base = secret("CDEK_API_URL") or "https://api.cdek.ru/v2"
        c, b = http(f"{base.rstrip('/')}/oauth/token?parameters", method="POST",
                    headers=["Content-Type: application/x-www-form-urlencoded"],
                    data=(f"grant_type=client_credentials&client_id={secret('CDEK_CLIENT_ID')}"
                          f"&client_secret={secret('CDEK_CLIENT_SECRET')}"))
        add("СДЭК", c, b)

    # Метрику дёргать напрямую бесполезно: наш токен выписан только на загрузку
    # офлайн-заказов (CDP), а management и stat отвечают ему 403 - это норма, а не
    # поломка. Поэтому смотрим на факт: ругалась ли интеграция при отправке.
    checks.append(check_metrika_by_logs())

    if secret("YOUGILE_API_KEY"):
        c, b = http("https://ru.yougile.com/api-v2/users?limit=1",
                    [f"Authorization: Bearer {secret('YOUGILE_API_KEY')}"])
        add("YouGile", c, b)

    if secret("TIMEWEB_TOKEN"):
        c, b = http("https://api.timeweb.cloud/api/v1/account/status",
                    [f"Authorization: Bearer {secret('TIMEWEB_TOKEN')}"])
        add("Timeweb", c, b)

    return checks


def check_metrika_by_logs(hours=24):
    """Метрика у нас односторонняя: amo шлёт ей офлайн-заказы для сквозной аналитики.
    Проверяем по логу интеграции - жаловалась ли она на загрузку за сутки."""
    name = "Яндекс.Метрика (загрузка заказов)"
    container = ENV.get("INTEGRATION_CONTAINER", "amo-fix-fields")
    try:
        p = subprocess.run(["docker", "logs", "--since", f"{hours}h", container],
                           capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return (name, True, "лог интеграции недоступен, пропускаю")
    text = (p.stdout or "") + (p.stderr or "")
    errors = [ln for ln in text.splitlines()
              if "Metrika" in ln and ("ошибка загрузки" in ln or "MetrikaError" in ln)]
    sent = [ln for ln in text.splitlines() if re.search(r"Metrika: заказ \d+ →", ln)]
    if errors:
        return (name, False, f"{len(errors)} ошибок загрузки за {hours} ч · {errors[-1][-90:]}")
    return (name, True, f"за {hours} ч отправлено заказов: {len(sent)}")


def run_tokens():
    state = load_state()
    prev = state.get("tokens", {})
    now = int(time.time())
    checks = check_tokens()
    lines_broken, lines_fixed = [], []
    new_state = {}

    for name, ok, why in checks:
        was = prev.get(name, {})
        was_ok = was.get("ok", True)
        last_alert = was.get("last_alert", 0)
        new_state[name] = {"ok": ok, "last_alert": last_alert}
        log(f"  {name}: {'ок' if ok else 'СЛОМАНО'} {why}".rstrip())
        if not ok:
            # первое падение или напоминание раз в 6 часов
            if was_ok or now - last_alert > 6 * 3600:
                lines_broken.append(f"• {name}: {why}")
                new_state[name]["last_alert"] = now
        elif not was_ok:
            lines_fixed.append(f"• {name}")

    state["tokens"] = new_state
    save_state(state)

    if lines_broken:
        tg_send("🔑 Доступ не работает\n\n" + "\n".join(lines_broken) +
                "\n\nПока не почините, напомню через 6 часов.")
    if lines_fixed:
        tg_send("✅ Доступ восстановился\n\n" + "\n".join(lines_fixed))
    if not lines_broken and not lines_fixed:
        log("  новостей нет, письмо не отправляю")
    return 0


# ===================================================================== проверка 2
KATYA_ID = "bcb10f20-542a-44c3-8d2f-296061377320"


def yougile_tasks():
    key = secret("YOUGILE_API_KEY")
    if not key:
        log("нет YOUGILE_API_KEY")
        return []
    tasks, offset = [], 0
    while True:
        c, b = http(f"https://ru.yougile.com/api-v2/task-list?limit=1000&offset={offset}",
                    [f"Authorization: Bearer {key}"], timeout=60)
        if c != 200:
            log(f"YouGile отдал HTTP {c}")
            return tasks
        d = json.loads(b)
        chunk = d.get("content") or []
        tasks.extend(chunk)
        if not d.get("paging", {}).get("next") or not chunk:
            return tasks
        offset += len(chunk)


def run_deadlines():
    now = datetime.now(MSK)
    today = now.date()
    # Катины задачи в YouGile почти всегда без исполнителя: их заводят ей и никого
    # не назначают. Поэтому «мои» = без исполнителя ИЛИ явно на Катю. Задачи,
    # назначенные другим (например Тиане), не берём.
    def is_mine(t):
        assigned = t.get("assigned") or []
        return not assigned or KATYA_ID in assigned

    tasks = [t for t in yougile_tasks()
             if is_mine(t)
             and not t.get("completed") and not t.get("archived") and not t.get("deleted")]

    overdue, due_today, due_tomorrow = [], [], []
    for t in tasks:
        dl = (t.get("deadline") or {}).get("deadline")
        if not dl:
            continue
        d = datetime.fromtimestamp(dl / 1000, MSK).date()
        label = f"{t.get('idTaskProject') or t.get('idTaskCommon') or ''} {t.get('title','')}".strip()
        if d < today:
            overdue.append((d, label))
        elif d == today:
            due_today.append((d, label))
        elif d == today + timedelta(days=1):
            due_tomorrow.append((d, label))

    # задачи, которые тебе поставили за последние сутки
    day_ago = (now - timedelta(days=1)).timestamp() * 1000
    fresh = [t for t in tasks
             if (t.get("timestamp") or 0) > day_ago and t.get("createdBy") != KATYA_ID]

    if not (overdue or due_today or due_tomorrow or fresh):
        log("  дедлайнов и новых задач нет, молчу")
        return 0

    parts = ["📅 Задачи на сегодня"]
    if overdue:
        parts.append(f"\nПросрочено ({len(overdue)}):")
        for d, label in sorted(overdue)[:10]:
            parts.append(f"• {label} - срок был {d.strftime('%d.%m')}")
        if len(overdue) > 10:
            parts.append(f"• …и ещё {len(overdue) - 10}")
    if due_today:
        parts.append(f"\nСегодня ({len(due_today)}):")
        parts += [f"• {label}" for _, label in due_today[:10]]
    if due_tomorrow:
        parts.append(f"\nЗавтра ({len(due_tomorrow)}):")
        parts += [f"• {label}" for _, label in due_tomorrow[:10]]
    if fresh:
        parts.append(f"\nПоставили за сутки ({len(fresh)}):")
        for t in fresh[:10]:
            who = {"864665f1-3697-4993-87bd-f588285bc820": "Андрей",
                   "3480b624-16d8-4f94-80ef-33b2f07d06e8": "Влад",
                   "ce3fe8fc-2a04-4e1c-a510-5c283c690efc": "Гладков"}.get(t.get("createdBy"), "кто-то")
            label = f"{t.get('idTaskProject') or ''} {t.get('title','')}".strip()
            parts.append(f"• {label} (от {who})")

    tg_send("\n".join(parts))
    log(f"  отправлено: просрочено {len(overdue)}, сегодня {len(due_today)}, "
        f"завтра {len(due_tomorrow)}, новых {len(fresh)}")
    return 0


# ===================================================================== проверка 3
DOMAINS = [d.strip() for d in (ENV.get("WATCH_DOMAINS") or
           "sunscrypt.ru,tangemshop.ru,keystone-russia.ru,sunscrypt.global").split(",") if d.strip()]
THRESHOLDS = [30, 14, 7, 3, 1]

DATE_PATTERNS = [
    r"paid-till:\s*(\S+)",                    # .ru / .рф
    r"Registry Expiry Date:\s*(\S+)",         # gTLD
    r"Expiration Date:\s*(\S+)",
    r"Expiry Date:\s*(\S+)",
    r"expires:\s*(\S+)",
]


def _parse_date(raw):
    raw = raw.strip().rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d.%m.%Y", "%Y.%m.%d"):
        try:
            return datetime.strptime(raw[:19] if "T" in raw else raw, fmt).date()
        except ValueError:
            continue
    return None


def domain_expiry(domain):
    """Сначала whois, потом RDAP. Системный whois не знает часть новых зон
    (на .global отвечает «TLD is not supported»), а RDAP отвечает по всем."""
    p = subprocess.run(["whois", domain], capture_output=True, text=True, timeout=60)
    text = p.stdout or ""
    for pat in DATE_PATTERNS:
        m = re.search(pat, text, re.I)
        if m:
            d = _parse_date(m.group(1))
            if d:
                return d

    code, body = http(f"https://rdap.org/domain/{domain}", ["Accept: application/rdap+json"], timeout=40)
    if code == 200:
        try:
            for ev in json.loads(body).get("events") or []:
                if ev.get("eventAction") == "expiration":
                    d = _parse_date(str(ev.get("eventDate", "")))
                    if d:
                        return d
        except json.JSONDecodeError:
            pass
    return None


def timeweb_balance():
    if not secret("TIMEWEB_TOKEN"):
        return None
    c, b = http("https://api.timeweb.cloud/api/v1/account/finances",
                [f"Authorization: Bearer {secret('TIMEWEB_TOKEN')}"])
    if c != 200:
        return None
    try:
        fin = json.loads(b).get("finances") or {}
        return {
            "balance": fin.get("balance"),
            "monthly": fin.get("monthly_cost"),
            "days_left": fin.get("hours_left") // 24 if isinstance(fin.get("hours_left"), int) else None,
        }
    except (json.JSONDecodeError, TypeError):
        return None


def run_renewals():
    state = load_state()
    prev = state.get("renewals", {})
    now = int(time.time())
    today = datetime.now(MSK).date()
    lines = []
    new_prev = dict(prev)

    for domain in DOMAINS:
        exp = domain_expiry(domain)
        if not exp:
            log(f"  {domain}: дату продления вытащить не удалось")
            continue
        days = (exp - today).days
        log(f"  {domain}: до {exp.strftime('%d.%m.%Y')}, осталось {days} дн.")
        crossed = [t for t in THRESHOLDS if days <= t]
        if not crossed:
            continue
        was = prev.get(domain, {})
        if was.get("threshold") == min(crossed) and now - was.get("last_alert", 0) < 3 * 86400:
            continue  # уже писала про этот порог недавно
        lines.append(f"• {domain}: продлить до {exp.strftime('%d.%m.%Y')}, осталось {days} дн.")
        new_prev[domain] = {"threshold": min(crossed), "last_alert": now}

    fin = timeweb_balance()
    if fin:
        log(f"  Timeweb: баланс {fin['balance']} ₽, хватит на {fin['days_left']} дн. "
            f"(списание {fin['monthly']} ₽/мес)")
    if fin and isinstance(fin.get("days_left"), int) and fin["days_left"] <= 14:
        was = prev.get("timeweb", {})
        if now - was.get("last_alert", 0) > 3 * 86400:
            lines.append(f"• Timeweb: на балансе {fin['balance']} ₽, хватит примерно на "
                         f"{fin['days_left']} дн. (списание {fin['monthly']} ₽/мес)")
            new_prev["timeweb"] = {"last_alert": now}

    state["renewals"] = new_prev
    save_state(state)

    if lines:
        tg_send("⏳ Скоро продлевать\n\n" + "\n".join(lines))
    else:
        log("  ничего не горит")
    return 0


# ===================================================================== проверка 4
# DeFi-дашборд Пети (academy.sunscrypt.ru). Смотрим не на прокси и не на код
# ответа, а на сами данные: источник может отвалиться молча, и снаружи сайт
# останется живым - просто раздел опустеет. Morpho, Balancer и Compound вдобавок
# ходят через наш прокси в Вену (они режут российские IP), для них пустота почти
# всегда значит «прокси лёг».
DEFI_API = (ENV.get("DEFI_API") or "https://academy.sunscrypt.ru/defi-api").rstrip("/")

# раздел API -> какие протоколы там должны быть. Список снят с живой выдачи
# 03.08.2026; добавит Петя новый источник - дописать сюда, иначе не следим.
DEFI_SOURCES = {
    "yields": [("Morpho", "morpho", True), ("Aave", "aave-v3", False), ("Euler", "euler", False),
               ("Compound", "compound", True), ("Fluid", "fluid", False),
               ("Jupiter Lend", "jupiter-lend", False)],
    "pools": [("Uniswap", "uniswap", False), ("Balancer", "balancer", True),
              ("Aerodrome", "aerodrome", False)],
    "perpdex": [("GMTrade", "gmtrade", False), ("GMX", "gmx", False), ("Jupiter JLP", "jupiter", False)],
}
# сколько данные могут не обновляться, прежде чем считать воркер мёртвым.
# Фоновый рефреш идёт раз в 30 минут, три пропуска подряд - уже не случайность.
DEFI_STALE_HOURS = 2


def defi_checks():
    """Живы ли источники дашборда и обновляются ли вообще данные.

    Имена проверок постоянные: если API не ответил, все они возвращаются
    сломанными, иначе потом не поймать восстановление."""
    names = ["DeFi · Свежесть данных"]
    for section in DEFI_SOURCES.values():
        names += [f"DeFi · {title}" for title, _, _ in section]
    names.append("DeFi · Pendle")

    def all_broken(why):
        return [(n, False, why) for n in names]

    checks = []
    freshest = None

    for endpoint, wanted in DEFI_SOURCES.items():
        code, body = http(f"{DEFI_API}/api/v1/{endpoint}", timeout=40)
        if code != 200:
            return all_broken(f"API дашборда отвечает HTTP {code or 'ничего'}")
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return all_broken("API дашборда вернул не JSON")

        rows = data.get("rows") or data.get("pools") or []
        updated = data.get("updatedAt")
        if isinstance(updated, int):
            freshest = updated if freshest is None else max(freshest, updated)

        for title, protocol, via_proxy in wanted:
            n = sum(1 for r in rows if str(r.get("protocol", "")) == protocol)
            why = f"{n} строк" if n else "пусто"
            if not n and via_proxy:
                why += " - похоже, прокси в Вену отвалился"
            checks.append((f"DeFi · {title}", n > 0, why))

    code, body = http(f"{DEFI_API}/api/v1/pendle", timeout=40)
    try:
        markets = (json.loads(body).get("markets") or []) if code == 200 else []
    except json.JSONDecodeError:
        markets = []
    checks.append(("DeFi · Pendle", bool(markets),
                   f"{len(markets)} рынков" if markets else "пусто"))

    # Данные могут остаться на месте, а фоновый воркер - умереть. Тогда цифры
    # просто застынут, и по составу источников этого не увидеть.
    if freshest is None:
        checks.insert(0, ("DeFi · Свежесть данных", False, "в ответе нет метки времени"))
    else:
        age_h = (time.time() - freshest) / 3600
        checks.insert(0, ("DeFi · Свежесть данных", age_h <= DEFI_STALE_HOURS,
                          f"обновлялись {age_h:.1f} ч назад"))
    return checks


def run_defi():
    state = load_state()
    prev = state.get("defi", {})
    now = int(time.time())
    lines_broken, lines_fixed = [], []
    new_state = {}

    for name, ok, why in defi_checks():
        was = prev.get(name, {})
        was_ok = was.get("ok", True)
        last_alert = was.get("last_alert", 0)
        new_state[name] = {"ok": ok, "last_alert": last_alert}
        log(f"  {name}: {'ок' if ok else 'ПУСТО'} {why}".rstrip())
        if not ok:
            if was_ok or now - last_alert > 6 * 3600:
                lines_broken.append(f"• {name}: {why}")
                new_state[name]["last_alert"] = now
        elif not was_ok:
            lines_fixed.append(f"• {name}")

    state["defi"] = new_state
    save_state(state)

    if lines_broken:
        tg_send("📉 DeFi-дашборд: данные пропали\n\n" + "\n".join(lines_broken) +
                "\n\nСмотреть: контейнер sundemy-api на 201.51.4.65, а если пусто у "
                "Morpho, Balancer или Compound - ещё и xray на 82.97.249.88."
                "\nПока не почините, напомню через 6 часов.")
    if lines_fixed:
        tg_send("✅ DeFi-дашборд снова с данными\n\n" + "\n".join(lines_fixed))
    if not lines_broken and not lines_fixed:
        log("  данные на месте, письмо не отправляю")
    return 0


# ===================================================================== проверка 5
# Упавший сервис на сервере умирает тихо: systemd его не поднимает, наружу ничего
# не видно. 04.08.2026 так нашли tg-analytics - случайно, руками, через несколько
# часов после падения. Отсюда эта проверка: раз в 5 минут смотрим, что живо.
#
# Три угла зрения, потому что «сломано» выглядит по-разному:
#   • юнит упал          - systemctl list-units --state=failed;
#   • контейнер лежит    - боевые интеграции живут в докере, systemd их не видит;
#   • таймер выключен    - юнит цел, но его никто не запускает. Самый тихий случай:
#     04.08 таймер синка панели остановили руками на время миграции - забудь его
#     включить, и аналитика молча встала бы на сутки.
WATCH_CONTAINERS = [c.strip() for c in (ENV.get("WATCH_CONTAINERS") or
                    "amo-fix-fields,woo-sklad").split(",") if c.strip()]
WATCH_TIMERS = [t.strip() for t in (ENV.get("WATCH_TIMERS") or
                "tg-analytics-daily.timer,tg-analytics-watchdog.timer,"
                "team-sync-hourly.timer,team-scheduler-recompute.timer,"
                "team-nightly.timer").split(",") if t.strip()]
# у кого лог в файле, а не в journalctl: там от сервиса только «process exited»
UNIT_LOG_FILES = {"tg-analytics-daily.service": "/opt/tg-analytics/logs/daily.log"}
CONTAINER_GRACE_SEC = 20  # даём подняться, если попали на пересборку при выкатке


def _sh(cmd, timeout=60):
    """Возвращает (код, stdout). Ошибку запуска приравниваем к пустому ответу."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "").strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return -1, ""


def _unit_since(unit):
    """Когда юнит в последний раз менял состояние - человеческим временем по Москве."""
    _, raw = _sh(["systemctl", "show", unit, "-p", "StateChangeTimestamp",
                  "--value", "--timestamp=unix"])
    if raw.startswith("@") and raw[1:].isdigit():
        return datetime.fromtimestamp(int(raw[1:]), MSK).strftime("%d.%m %H:%M МСК")
    return raw or "время неизвестно"


def _unit_why(unit):
    """Последние строки лога - чтобы причина была прямо в сообщении, а не «сходи посмотри»."""
    lines = []
    path = UNIT_LOG_FILES.get(unit)
    if path and os.path.exists(path):
        _, out = _sh(["tail", "-n", "60", path])
        lines = out.splitlines()
    if not lines:
        _, out = _sh(["journalctl", "-u", unit, "-n", "60", "--no-pager", "-o", "cat"])
        lines = out.splitlines()
    # Выкидываем то, что в сообщении только занимает место: рамки прогонов,
    # болтовню systemd и внутренние кадры трейсбека (File "...", line N). Смысл
    # несёт последняя строка - у питона там сам текст исключения.
    skip = ("=== ", "Started ", "Starting ", "Stopped ", "Stopping ", 'File "')
    # \r и прочие управляющие из лога сборщика ломают показ - схлопываем в пробел
    lines = [re.sub(r"[\s\x00-\x1f\x7f]+", " ", ln).strip() for ln in lines]
    lines = [ln for ln in lines if ln and not ln.startswith(skip)]
    # кадры трейсбека и подчёркивания ^^^^ из питона 3.12 в сообщении бесполезны
    lines = [ln for ln in lines
             if not re.match(r"^(raise|return|response|result|with|for) ", ln)
             and not re.fullmatch(r"[\^~ ]+", ln)]
    return " · ".join(lines[-3:])[:300] or "в логе пусто"


def _container_alive(name, second_try=False):
    """Жив ли контейнер. Лежащий пересматриваем через паузу: при выкатке
    (docker compose up -d) он на секунды исчезает, и это не повод будить Катю."""
    code, out = _sh(["docker", "inspect", "-f", "{{.State.Running}} {{.State.Status}}", name])
    if code == 0 and out.startswith("true"):
        return True, "работает"
    if not second_try:
        time.sleep(CONTAINER_GRACE_SEC)
        return _container_alive(name, second_try=True)
    if code != 0:
        alive, _ = _sh(["docker", "version", "--format", "{{.Server.Version}}"])
        if alive != 0:  # не один контейнер лежит, а весь докер
            return False, "докер-демон не отвечает"
        return False, "докер такого контейнера не знает"
    return False, f"состояние: {out or 'непонятное'}"


def services_checks():
    checks = []

    _, out = _sh(["systemctl", "list-units", "--state=failed", "--no-legend",
                  "--plain", "--no-pager"])
    for line in out.splitlines():
        unit = line.split()[0] if line.split() else ""
        if unit:
            checks.append((f"Сервис · {unit}",
                           False,
                           f"упал {_unit_since(unit)} · {_unit_why(unit)}"))

    for name in WATCH_CONTAINERS:
        ok, why = _container_alive(name)
        checks.append((f"Контейнер · {name}", ok, why))

    for unit in WATCH_TIMERS:
        _, enabled = _sh(["systemctl", "is-enabled", unit])
        _, active = _sh(["systemctl", "is-active", unit])
        ok = enabled == "enabled" and active == "active"
        why = "по расписанию" if ok else f"{enabled or '?'} / {active or '?'} - запусков не будет"
        checks.append((f"Таймер · {unit}", ok, why))

    return checks


def run_services():
    state = load_state()
    prev = state.get("services", {})
    now = int(time.time())
    checks = services_checks()
    seen = {name for name, _, _ in checks}
    # упавший юнит, починившись, просто исчезает из списка failed - без этой
    # строчки о его восстановлении никто бы не узнал
    checks += [(name, True, "снова в порядке") for name in prev if name not in seen]

    lines_broken, lines_fixed = [], []
    new_state = {}
    for name, ok, why in checks:
        was = prev.get(name, {})
        was_ok = was.get("ok", True)
        last_alert = was.get("last_alert", 0)
        if not ok:  # проверка идёт каждые 5 минут - в лог пишем только плохое
            log(f"  {name}: ЛЕЖИТ {why}".rstrip())
        if ok and name not in seen:
            if not was_ok:
                lines_fixed.append(f"• {name}")
            continue  # динамический юнит починился - в состоянии его больше не держим
        new_state[name] = {"ok": ok, "last_alert": last_alert}
        if not ok:
            if was_ok or now - last_alert > 6 * 3600:
                lines_broken.append(f"• {name}\n  {why}")
                new_state[name]["last_alert"] = now
        elif not was_ok:
            lines_fixed.append(f"• {name}")

    state["services"] = new_state
    save_state(state)

    if lines_broken:
        tg_send("🔴 На сервере лежит сервис\n\n" + "\n".join(lines_broken) +
                "\n\nСервер 85.193.91.169. Поднять: systemctl start <юнит> "
                "(контейнер - docker compose up -d <имя>)."
                "\nПока лежит, напомню через 6 часов.")
    if lines_fixed:
        tg_send("✅ Сервис снова работает\n\n" + "\n".join(lines_fixed))
    if not lines_broken and not lines_fixed:
        lying = sum(1 for _, ok, _ in checks if not ok)
        log(f"  нового нет: лежит {lying}, про них уже писала" if lying
            else f"  всё живо ({len(checks)} проверок), письмо не отправляю")
    return 0


# --------------------------------------------------------------------------- main
CHECKS = {"tokens": run_tokens, "deadlines": run_deadlines, "renewals": run_renewals,
          "defi": run_defi, "services": run_services}

if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else ""
    if what not in CHECKS:
        print(f"как запускать: watchdog.py [{' | '.join(CHECKS)}]")
        sys.exit(2)
    log(f"проверка: {what}")
    try:
        sys.exit(CHECKS[what]())
    except Exception as e:  # noqa: BLE001
        log(f"ПАДЕНИЕ: {e}")
        tg_send(f"❗️Сторож упал на проверке «{what}»: {e}")
        sys.exit(3)
