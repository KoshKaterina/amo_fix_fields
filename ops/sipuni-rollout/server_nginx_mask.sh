#!/bin/sh
# Шаг A. Маска секрета Sipuni в access-логе nginx: одна строка в /etc/nginx/conf.d/00-log-sanitize.conf.
# Запуск на сервере: sudo sh server_nginx_mask.sh
# Идемпотентно. Перед правкой копия конфига, после правки nginx -t, при ошибке конфиг возвращается.
set -eu
CONF=/etc/nginx/conf.d/00-log-sanitize.conf
TS=$(date +%Y%m%d-%H%M%S)
BK=/root/backups/nginx-mask-$TS.conf

if grep -q '/amo/sipuni/' "$CONF"; then
  echo "маска /amo/sipuni/ уже есть, ничего не делаю"
  exit 0
fi
grep -q '(/amo/getcourse/)' "$CONF" || { echo "СТОП: не нашла строку-якорь getcourse в $CONF"; exit 2; }

mkdir -p /root/backups
cp -p "$CONF" "$BK"
awk '{print} /\(\/amo\/getcourse\/\)/ && !d {printf "    \"~^(/amo/sipuni/)[^/]+(.*)$\"                   $1HIDDEN$2;\n"; d=1}' "$BK" > "$CONF.new"
cp "$CONF.new" "$CONF"
rm -f "$CONF.new"
echo "--- что изменилось:"
diff "$BK" "$CONF" || true

if ! nginx -t >/dev/null 2>&1; then
  cp -p "$BK" "$CONF"
  nginx -t || true
  echo "СТОП: nginx -t не прошёл, конфиг возвращён из $BK"
  exit 3
fi
nginx -s reload
echo "nginx перечитал конфиг, копия прежнего: $BK"

# Проверка маски. Перезагрузка nginx асинхронная: пока старые рабочие процессы не ушли, запрос может
# обслужить старый процесс со старым конфигом и записать путь без маски. Поэтому ждём и повторяем до трёх раз,
# и только если маска не сработала ни разу, возвращаем конфиг.
sleep 3
i=0
RESULT=""
while [ "$i" -lt 3 ]; do
  i=$((i+1))
  curl -sk -o /dev/null --resolve team.sunscrypt.ru:443:127.0.0.1 "https://team.sunscrypt.ru/amo/sipuni/maskcheck-$TS-$i" || true
  sleep 2
  LINE=$(tail -n 30 /var/log/nginx/access.log | grep "/amo/sipuni/" | grep -E "maskcheck-$TS-$i|/amo/sipuni/HIDDEN" | tail -n 1 || true)
  case "$LINE" in
    *"/amo/sipuni/HIDDEN"*) RESULT=ok; break;;
    *"maskcheck-$TS-$i"*) RESULT=raw; sleep 3;;
    *) RESULT=none; sleep 2;;
  esac
done
case "$RESULT" in
  ok) echo "проверка маски: ok (в журнале /amo/sipuni/HIDDEN, попытка $i)";;
  raw) cp -p "$BK" "$CONF"; nginx -s reload; echo "СТОП: путь попал в журнал без маски трижды, конфиг возвращён"; exit 4;;
  *) echo "проверка маски: запись в журнале не найдена, проверить руками: tail /var/log/nginx/access.log";;
esac
