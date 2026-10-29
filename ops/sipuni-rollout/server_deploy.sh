#!/bin/sh
# Шаг C. Выкатка приёмника событий Sipuni в контейнер amo-fix-fields (режим log).
# Запуск на сервере: sudo sh server_deploy.sh /tmp/sipuni-bundle
# Пакет собран make_bundle.py (файлы в LF, MD5SUMS, PROD_MD5SUMS). Контейнер пересоздаётся: 10-30 секунд
# боевые вебхуки не принимаются. Состояние в памяти (дедуп, счётчики) теряется, как при любой выкатке.
set -eu
B="${1:-/tmp/sipuni-bundle}"
APP=/opt/integrations/amo_fix_fields
FILES="webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py sipuni_events.py"
TS=$(date +%Y%m%d-%H%M%S)
BK=/root/backups/sipuni-log-$TS

md5lf() { tr -d '\r' < "$1" | md5sum | cut -d' ' -f1; }

echo "== 1. прод равен базе, на которой собран пакет?"
for f in webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py; do
  want=$(grep "  $f\$" "$B/PROD_MD5SUMS" | cut -d' ' -f1)
  have=$(md5lf "$APP/$f")
  if [ "$want" != "$have" ]; then
    echo "СТОП: $f на проде не равен базе пакета (прод ушёл вперёд). Ничего не менялось."
    exit 2
  fi
done
echo "ok: прод равен базе"

echo "== 2. целостность пакета"
(cd "$B" && md5sum -c MD5SUMS)

echo "== 3. тихая проверка секретов окружения: SIPUNI_WEBHOOK_SECRET должен быть задан (шаг B)"
grep -q '^SIPUNI_WEBHOOK_SECRET=' "$APP/.env" || { echo "СТОП: нет SIPUNI_WEBHOOK_SECRET, сначала шаг B"; exit 2; }

echo "== 4. бэкап: $BK"
mkdir -p "$BK"
chmod 700 "$BK"
tar --exclude='amo_fix_fields/var' -czf "$BK/amo_fix_fields.tgz" -C /opt/integrations amo_fix_fields
chmod 600 "$BK/amo_fix_fields.tgz"
for f in $FILES; do
  [ -e "$APP/$f" ] && cp -p "$APP/$f" "$BK/$f" || true
done
echo "$BK" > /root/.last-sipuni-backup
ls -la "$BK" | sed 's/^/   /'

echo "== 5. установка файлов (владелец и права как у webhooks.py)"
REF="$APP/webhooks.py"
U=$(stat -c %u "$REF"); G=$(stat -c %g "$REF"); M=$(stat -c %a "$REF")
for f in $FILES; do
  install -o "$U" -g "$G" -m "$M" "$B/$f" "$APP/$f"
done
for f in $FILES; do
  want=$(grep "  $f\$" "$B/MD5SUMS" | cut -d' ' -f1)
  have=$(md5lf "$APP/$f")
  [ "$want" = "$have" ] || { echo "СТОП: $f после установки не совпал, откат: sudo sh server_rollback.sh $BK"; exit 3; }
done
echo "ok: пять файлов установлены, суммы совпали"

echo "== 6. сборка образа и пересоздание контейнера"
cd /opt/integrations
docker compose build amo-fix-fields
docker compose up -d amo-fix-fields
echo "контейнер пересоздан"

echo "== 7. проверки"
sh "$(dirname "$0")/server_smoke.sh" || { echo "ПРОВЕРКИ НЕ ПРОШЛИ, откат: sudo sh server_rollback.sh $BK"; exit 4; }
echo "== выкатка завершена, бэкап: $BK"
df -h / | tail -1 | sed 's/^/диск: /'
