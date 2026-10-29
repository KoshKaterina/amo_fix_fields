#!/bin/sh
# Откат выкатки приёмника Sipuni. Запуск на сервере: sudo sh server_rollback.sh [каталог_бэкапа]
# Без аргумента берёт последний бэкап из /root/.last-sipuni-backup.
# Возвращает четыре прежних файла, удаляет sipuni_events.py, убирает строки SIPUNI_ из .env, пересобирает контейнер.
# Маска в nginx остаётся (она безвредна). В кабинете Sipuni после отката очистить поле «URL принимающего скрипта».
set -eu
BK="${1:-$(cat /root/.last-sipuni-backup)}"
APP=/opt/integrations/amo_fix_fields
[ -d "$BK" ] || { echo "нет каталога бэкапа: $BK"; exit 2; }
REF="$APP/webhooks.py"
U=$(stat -c %u "$REF"); G=$(stat -c %g "$REF"); M=$(stat -c %a "$REF")
md5lf() { tr -d '\r' < "$1" | md5sum | cut -d' ' -f1; }

for f in webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py; do
  [ -f "$BK/$f" ] || { echo "в бэкапе нет $f"; exit 2; }
  install -o "$U" -g "$G" -m "$M" "$BK/$f" "$APP/$f"
done
rm -f "$APP/sipuni_events.py"
# переменные Sipuni убираем из .env (правка только строк с этим префиксом)
sed -i '/^SIPUNI_/d' "$APP/.env"
echo "файлы возвращены из $BK, sipuni_events.py удалён, строки SIPUNI_ убраны из .env"

cd /opt/integrations
docker compose build amo-fix-fields
docker compose up -d --force-recreate amo-fix-fields

i=0
until [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8010/)" = "200" ]; do
  i=$((i+1)); [ "$i" -gt 40 ] && { echo "ВНИМАНИЕ: контейнер не отвечает 200 за 80 секунд"; exit 3; }
  sleep 2
done
echo "корень отвечает 200"
for f in webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py; do
  [ "$(md5lf "$APP/$f")" = "$(tr -d '\r' < "$BK/$f" | md5sum | cut -d' ' -f1)" ] && echo "ok md5 $f" || echo "РАСХОЖДЕНИЕ $f"
done
echo "TRACEBACK в журнале за 2 минуты: $(docker logs --since 2m amo-fix-fields 2>&1 | grep -c Traceback || true)"
echo "откат выполнен"
