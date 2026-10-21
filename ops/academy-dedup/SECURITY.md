# Непривилегированный webhook Академии

Источник webhook_daemon.py/amo.py снят с действующего OLD VPS22.10.2026. Логика обработчика сохранена; добавлена настройка ACADEMY_DEDUP_WEBHOOK_STATE для отдельного каталога очереди.

Размещение: root-owned /opt/academy-dedup-hook, служебный nologin пользователь academy-hook, каталог очереди /var/lib/academy-dedup-hook mode0700. Drop-in security.conf для academy-dedup-webhook.service. В /etc/academy-dedup-hook.env root0600 только AMO_TOKEN, извлечённый на сервере из текущего TOKEN интеграции без вывода. При ротации TOKEN этот потребитель также обновляется. Остальные ключи интеграции процессу не передаются.

50-academy-hook.rules устанавливается root в /etc/polkit-1/rules.d/. Разрешён только start существующей academy-dedup.service. Запретить stop/restart и управление любыми другими службами. Основной job остаётся root-owned и недоступен для записи receiver; его код/поведение не меняются. pkcheck проверяет разрешение без запуска бизнес-операции.

Перед переносом остановить только receiver, снять SQLite backup с integrity_check, сравнить queue counts; перенести очередь в новый каталог, не создавать её пустой. Основной job не запускать вручную ради проверки. После запуска проверить UID, health, доступ к amo read-only и отказ чтения соседних env, штатное событие проверить отдельно по журналу.

Откат: остановить receiver, перенести текущую новую очередь обратно штатным SQLite backup (не восстановить устаревший снимок), убрать только security.conf и polkit-правило, daemon-reload, старт прежнего receiver. Исторический код/данные сохраняются закрытыми в /root/backups/security-20261021/dedup-hook.

Проверки:6offlineHTTP/SQLitecases; разрешённый/запрещённыеpolkitactions; отдельныйread-onlyGETaccount; livehealth/UID/queue/integrity. Никаких тестовых amoсделок/слияний/уведомлений.
