#!/bin/sh
# Шаг B. Секрет приёмника Sipuni в .env интегратора. ПИШЕТ В БОЕВОЙ .env: запускать только по отдельному «да»
# на запись в .env. Запуск на сервере: sudo sh server_env_secret.sh
# Секрет не печатается. Готовый адрес для кабинета Sipuni пишется в /root/sipuni-events-url.txt (права 600).
set -eu
ENVF=/opt/integrations/amo_fix_fields/.env
URLF=/root/sipuni-events-url.txt

if grep -q '^SIPUNI_WEBHOOK_SECRET=' "$ENVF"; then
  echo "SIPUNI_WEBHOOK_SECRET уже задан в .env, ничего не меняю"
else
  SECRET=$(openssl rand -hex 24)
  # перед добавлением убеждаемся, что файл заканчивается переводом строки
  [ -z "$(tail -c1 "$ENVF")" ] || echo >> "$ENVF"
  {
    echo "SIPUNI_WEBHOOK_SECRET=$SECRET"
    echo "SIPUNI_EVENTS_MODE=log"
  } >> "$ENVF"
  umask 077
  echo "https://team.sunscrypt.ru/amo/sipuni/$SECRET" > "$URLF"
  chmod 600 "$URLF"
  echo "добавлено в .env: SIPUNI_WEBHOOK_SECRET (значение не показано) и SIPUNI_EVENTS_MODE=log"
  echo "адрес для кабинета Sipuni записан в $URLF (читать: sudo cat $URLF)"
fi
grep -c '^SIPUNI_' "$ENVF" | sed 's/^/строк SIPUNI_ в .env: /'
grep '^SIPUNI_EVENTS_MODE=' "$ENVF" | sed 's/^/режим: /'
