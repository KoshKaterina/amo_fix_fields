#!/bin/sh
# Шаг D. Проверки приёмника событий Sipuni. Безопасно в режиме log: ни Telegram, ни amo не трогаются.
# На сервере (после выкатки): sudo sh server_smoke.sh
# Локально (образ на другом порту): BASE=http://127.0.0.1:18010 SECRET=localtest123 CONTAINER=sipuni-smoke NOMD5=1 sh server_smoke.sh
# Выдуманный звонок: внешний номер 70000000000, событие 1 (старт) и событие 2 (NOANSWER). Через тихое окно
# (SIPUNI_MISSED_QUIET_S, 20 секунд) в журнале должна появиться строка «пропущенный ... режим log, алерт НЕ шлём».
set -u
BASE="${BASE:-http://127.0.0.1:8010}"
CONTAINER="${CONTAINER:-amo-fix-fields}"
APP=/opt/integrations/amo_fix_fields
BUNDLE="${BUNDLE:-/tmp/sipuni-bundle}"
SECRET="${SECRET:-}"
[ -n "$SECRET" ] || SECRET=$(grep '^SIPUNI_WEBHOOK_SECRET=' "$APP/.env" | cut -d= -f2-)
FAIL=0
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
check() { # имя, получено, ожидалось
  if [ "$2" = "$3" ]; then echo "ok   $1: $2"; else echo "FAIL $1: получено $2, ожидалось $3"; FAIL=1; fi
}

i=0
until [ "$(code "$BASE/")" = "200" ]; do
  i=$((i+1)); [ "$i" -gt 40 ] && { echo "FAIL контейнер не поднялся за 80 секунд"; exit 1; }
  sleep 2
done
check "корень отвечает" "$(code "$BASE/")" 200
check "sipuni, неверный секрет" "$(code "$BASE/sipuni/wrong-secret-x")" 403
check "uis, неверный секрет (прежний маршрут жив)" "$(code "$BASE/uis/wrong-secret-x")" 403

T=$(date +%s)
CALL="smoke.$T"
R1=$(curl -s "$BASE/sipuni/$SECRET?event=1&call_id=$CALL&src_num=70000000000&src_type=1&dst_num=100&dst_type=2&timestamp=$T")
R2=$(curl -s -X POST -d "event=2&call_id=$CALL&src_num=70000000000&src_type=1&dst_num=100&dst_type=2&status=NOANSWER&call_start_timestamp=$T&call_answer_timestamp=0&timestamp=$T" "$BASE/sipuni/$SECRET")
check "событие 1 (GET), ответ" "$R1" '{"success":true}'
check "событие 2 (POST форма), ответ" "$R2" '{"success":true}'

echo "жду тихое окно, 26 секунд..."
sleep 26
LOGS=$(docker logs --since 3m "$CONTAINER" 2>&1)
N=$(echo "$LOGS" | grep -c "Sipuni событие: ev=" || true)
[ "$N" -ge 2 ] && echo "ok   строк «Sipuni событие» в журнале: $N" || { echo "FAIL строк «Sipuni событие»: $N, ожидалось не меньше 2"; FAIL=1; }
echo "$LOGS" | grep "Sipuni пропущенный: call=$CALL" | grep -q "режим log" \
  && echo "ok   решение «пропущенный» принято, режим log, алерт НЕ отправлен" \
  || { echo "FAIL нет строки «Sipuni пропущенный ... режим log» по смоук-звонку"; FAIL=1; }
S=$(echo "$LOGS" | grep -c "$SECRET" || true)
check "секрет в журнале контейнера (строк)" "$S" 0
T2=$(echo "$LOGS" | grep -c "Traceback" || true)
check "Traceback в журнале (строк)" "$T2" 0
MODE=$(docker exec "$CONTAINER" printenv SIPUNI_EVENTS_MODE 2>/dev/null || echo "?")
check "режим в контейнере" "$MODE" log

if [ -z "${NOMD5:-}" ] && [ -f "$BUNDLE/MD5SUMS" ]; then
  while read -r want name; do
    have=$(docker exec "$CONTAINER" sh -c "tr -d '\r' < /app/$name | md5sum | cut -d' ' -f1")
    check "md5 в контейнере $name" "$have" "$want"
  done < "$BUNDLE/MD5SUMS"
fi

[ "$FAIL" = 0 ] && echo "ВСЕ ПРОВЕРКИ ПРОШЛИ" || echo "ЕСТЬ ПРОВАЛЫ"
exit "$FAIL"
