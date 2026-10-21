#!/usr/bin/env python3
"""Минимальный READ-ONLY клиент amoCRM для среза (только stdlib).

- Токен читается из существующего .env активной интеграции (НЕ копируется,
  НЕ печатается). Путь и base URL переопределяются через переменные окружения.
- Никаких POST/PATCH/DELETE — клиент умеет только GET.
- Тротлинг под лимит amoCRM (~7 rps) + ретраи на 429/сетевых сбоях.

CLI:
    python3 amo.py <path> [--key EMBEDDED_KEY] [--all] [--out FILE.json]
Примеры:
    python3 amo.py leads/pipelines --key pipelines --out ../00-raw/pipelines.json
    python3 amo.py "users?with=role,group" --key users --all --out ../00-raw/users.json
"""
import json, os, subprocess, sys, time

BASE = os.environ.get("AMO_BASE", "https://new5a2e8ea7b16b4.amocrm.ru").rstrip("/")
# Лимит amo — 7 rps на КАЖДУЮ интеграцию (50 — общий потолок аккаунта). Ключ
# аналитики берём первым: у него своё ведро, и мы не отбираем запросы у панели
# team.sunscrypt.ru, которая сидит на ключе из scripts/amo_fix_fields/.env.
_ANALYTICS_ENV = "/Users/ekaterina/Documents/Sunscrypt/scripts/amo_analytics/.env"
_FALLBACK_ENV = "/Users/ekaterina/Documents/Sunscrypt/scripts/amo_fix_fields/.env"
ENV_PATH = os.environ.get(
    "AMO_ENV", _ANALYTICS_ENV if os.path.exists(_ANALYTICS_ENV) else _FALLBACK_ENV
)
MIN_INTERVAL = float(os.environ.get("AMO_MIN_INTERVAL", "0.2"))  # ~5 rps, ниже лимита 7

_last = [0.0]


def _token():
    t = os.environ.get("AMO_TOKEN")
    if t:
        return t.strip()
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("TOKEN="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("TOKEN not found in env")


def _throttle():
    dt = time.time() - _last[0]
    if dt < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - dt)
    _last[0] = time.time()


def get(path, tries=6):
    url = path if path.startswith("http") else f"{BASE}/api/v4/{path.lstrip('/')}"
    tok = _token()
    for attempt in range(tries):
        _throttle()
        try:
            p = subprocess.run(
                ["curl", "-s", "--globoff", "--max-time", "90", "-w", "\n%{http_code}",
                 "--variable", "%ACADEMY_AMO_TOKEN",
                 "--expand-header", "Authorization: Bearer {{ACADEMY_AMO_TOKEN}}",
                 "-H", "Accept: application/json", url],
                capture_output=True, text=True,
                env={**os.environ, "ACADEMY_AMO_TOKEN": tok},
            )
        except Exception:
            time.sleep(1.0 * (attempt + 1))
            continue
        out = p.stdout or ""
        nl = out.rfind("\n")
        body, code = (out[:nl], out[nl + 1:].strip()) if nl >= 0 else ("", out.strip())
        if code == "429":
            time.sleep(1.5 * (attempt + 1))
            continue
        if code == "204":
            return {}
        if code.startswith("2"):
            try:
                return json.loads(body) if body.strip() else {}
            except Exception:
                return {"_error": "parse", "_body": body[:300], "_url": url}
        if code in ("000", ""):  # сетевой сбой curl — ретрай
            time.sleep(1.0 * (attempt + 1))
            continue
        return {"_error": code, "_body": body[:300], "_url": url}
    return {"_error": "network", "_url": url}


def get_all(path, key, limit=250, max_pages=100000):
    items = []
    page = 1
    sep = "&" if "?" in path else "?"
    while page <= max_pages:
        d = get(f"{path}{sep}limit={limit}&page={page}")
        if d.get("_error"):
            if page == 1:
                return d  # пробросить ошибку наверх
            break
        emb = d.get("_embedded", {}).get(key, [])
        if not emb:
            break
        items.extend(emb)
        if not d.get("_links", {}).get("next"):
            break
        page += 1
    return {"_embedded": {key: items}}


def _count(d, key):
    if isinstance(d, dict) and d.get("_error"):
        return None
    if key:
        return len(d.get("_embedded", {}).get(key, []))
    return None


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--key", help="ключ в _embedded для пагинации/подсчёта")
    ap.add_argument("--all", action="store_true", help="постранично собрать всё")
    ap.add_argument("--out")
    a = ap.parse_args()

    data = get_all(a.path, a.key) if (a.all and a.key) else get(a.path)

    if isinstance(data, dict) and data.get("_error"):
        print(f"ERR  {a.path} -> HTTP {data['_error']} {str(data.get('_body',''))[:120]}")
        sys.exit(0)

    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        json.dump(data, open(a.out, "w"), ensure_ascii=False, indent=1)

    n = _count(data, a.key)
    tail = f" -> {a.out}" if a.out else ""
    print(f"OK   {a.path} -> {n if n is not None else 'saved'} {a.key or ''}{tail}")
