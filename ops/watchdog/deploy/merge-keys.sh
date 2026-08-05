#!/bin/bash
# Переносит два ключа во внутренний .env сторожа, не показывая значения.
# Ждёт /tmp/_yg.env (YOUGILE_API_KEY) и /tmp/_tw.env (TIMEWEB_TOKEN), после - стирает их.
set -e
TARGET=/opt/watchdog/.env

put() {  # put ИМЯ ФАЙЛ-ИСТОЧНИК
  local name="$1" src="$2"
  [ -f "$src" ] || { echo "нет файла $src, пропускаю $name"; return; }
  local val
  val=$(grep -m1 "^$name=" "$src" | cut -d= -f2-)
  [ -n "$val" ] || { echo "в $src нет $name"; return; }
  python3 - "$TARGET" "$name" "$val" <<'PY'
import sys
path, name, val = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(path, encoding="utf-8").read().splitlines()
out, done = [], False
for line in lines:
    if line.startswith(name + "="):
        out.append(f"{name}={val}")
        done = True
    else:
        out.append(line)
if not done:
    out.append(f"{name}={val}")
open(path, "w", encoding="utf-8").write("\n".join(out) + "\n")
PY
  echo "$name записан (${#val} символов)"
}

put YOUGILE_API_KEY /tmp/_yg.env
put TIMEWEB_TOKEN /tmp/_tw.env
shred -u /tmp/_yg.env /tmp/_tw.env 2>/dev/null || rm -f /tmp/_yg.env /tmp/_tw.env
chmod 600 "$TARGET"
echo "готово"
