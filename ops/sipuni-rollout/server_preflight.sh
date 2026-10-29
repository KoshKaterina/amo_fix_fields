#!/bin/sh
# Предполётная проверка перед выкаткой приёмника Sipuni. ТОЛЬКО ЧТЕНИЕ, ничего не меняет.
# Запуск на сервере: sudo sh server_preflight.sh [каталог_пакета]
# Код выхода 0: можно выкатывать; не ноль: что-то изменилось с момента подготовки, выкатку не начинать.
set -u
B="${1:-/tmp/sipuni-bundle}"
APP=/opt/integrations/amo_fix_fields
FAIL=0
md5lf() { tr -d '\r' < "$1" | md5sum | cut -d' ' -f1; }
ok()   { echo "ok   $1"; }
bad()  { echo "FAIL $1"; FAIL=1; }

for f in webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py; do
  want=$(grep "  $f\$" "$B/PROD_MD5SUMS" | cut -d' ' -f1)
  have=$(md5lf "$APP/$f")
  [ "$want" = "$have" ] && ok "прод равен базе пакета: $f" || bad "прод НЕ равен базе пакета: $f (прод ушёл вперёд, пересобрать пакет)"
done
(cd "$B" && md5sum -c MD5SUMS >/dev/null 2>&1) && ok "пакет цел (MD5SUMS)" || bad "пакет повреждён"
[ -e "$APP/sipuni_events.py" ] && echo "инфо sipuni_events.py уже лежит на проде (будет заменён)" || ok "sipuni_events.py на проде ещё нет"
if grep -q '^SIPUNI_WEBHOOK_SECRET=' "$APP/.env"; then echo "инфо SIPUNI_WEBHOOK_SECRET уже задан в .env"; else echo "инфо SIPUNI_WEBHOOK_SECRET ещё не задан (шаг B перед шагом C)"; fi
grep -q '/amo/sipuni/' /etc/nginx/conf.d/00-log-sanitize.conf && echo "инфо маска /amo/sipuni/ в nginx уже есть" || echo "инфо маски /amo/sipuni/ в nginx ещё нет (шаг A)"

FREE_KB=$(df -k / | awk 'NR==2{print $4}')
[ "$FREE_KB" -gt 2097152 ] && ok "свободно на диске: $((FREE_KB/1024)) МБ" || bad "мало места на диске: $((FREE_KB/1024)) МБ (нужно больше 2 ГБ для сборки образа)"
docker ps --format '{{.Names}} {{.Status}}' | grep -q '^amo-fix-fields Up' && ok "контейнер amo-fix-fields работает" || bad "контейнер amo-fix-fields не работает"
nginx -t >/dev/null 2>&1 && ok "nginx -t" || bad "nginx -t не проходит"
[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8010/)" = "200" ] && ok "localhost:8010 отвечает 200" || bad "localhost:8010 не отвечает 200"
[ "$FAIL" = 0 ] && echo "ПРЕДПОЛЁТ ПРОЙДЕН" || echo "ПРЕДПОЛЁТ НЕ ПРОЙДЕН"
exit "$FAIL"
