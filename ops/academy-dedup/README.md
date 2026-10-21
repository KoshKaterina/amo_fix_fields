# Сторож дублей Академии (academy-dedup)

**Обновление22.10.2026:** постоянный webhook работает отдельнымacademy-hook с отдельной очередью и узкимправомstartслужбы. Текущая схема/выкатка/откат — [SECURITY.md](SECURITY.md). Исторические пути ниже относятся к исходной схеме; основнойrun_cycleне менялся.

Склеивает открытые дубли в воронке «Академия» (`8642410`) amoCRM. Работает с 08.10.2026 на
сервере интеграций `85.193.91.169` прямо на хосте, под systemd, без докера. Как он устроен и
что делает, описано в [`app/README.md`](app/README.md): это серверный README, он лежит здесь без
изменений.

Папка — снимок боевых файлов, снятый с сервера 15.10.2026 только чтением. Все десять файлов
совпали с сервером по md5. Пуш в git ничего не выкатывает: бой меняется только руками, по
порядку ниже.

## Что где лежит

| В репозитории | На сервере | Владелец и права |
|---|---|---|
| `app/*.py` | `/opt/academy-dedup/` | `root:root 700` |
| `app/README.md` | `/opt/academy-dedup/README.md` | `root:root 644` |
| `systemd/*` | `/etc/systemd/system/` | `root:root 644` |

Сама папка `/opt/academy-dedup` закрыта (`root 700`), поэтому читать и ставить файлы можно
только через `sudo`. Маска вроде `/opt/academy-dedup/*.py` в команде без `sudo sh -c '…'`
не раскроется: у обычной учётки нет прав на список файлов.

Что нужно на сервере, но сознательно не лежит в git:

- `/opt/integrations/amo_fix_fields/.env` — общий с интеграцией. Отсюда `TOKEN` для amo,
  `TG_BOT_TOKEN` и `TG_ALLOWED_CHAT_ID` для алерта. Код читает файл сам, копий токена нет.
- `/etc/academy-dedup-webhook-path` и `/etc/nginx/snippets/academy-dedup-webhook.conf` —
  секретный путь, на который amo шлёт вебхук. Кто знает путь, тот может слать вебхуки, поэтому
  в git его нет. На сниппет ссылается `/etc/nginx/sites-enabled/team-panel`. Сам приёмник
  слушает `127.0.0.1:8031` и принимает только `POST /hook` и `GET /health`.
- `/var/lib/academy-dedup/` — состояние: очередь вебхуков `webhook-queue.sqlite3`, подписка amo
  `webhook-subscription.json`, троттл алерта `telegram-alert-state.json`, замок прогона
  `cycle.lock`, папки прогонов `runs/`.
- `__pycache__/`.

## Переводы строк и md5

`.gitattributes` в этой папке выключает перевод строк (`* -text`). Файлы и в git, и в рабочем
дереве на Windows лежат с LF, байт в байт как на сервере, поэтому md5 сравниваются как есть,
без `tr -d '\r'`. Правки сохраняйте с LF: если CRLF всё же проскочил, `git diff` покажет `^M`
в конце строк.

## Что перезапускать после замены

| Что поменялось | Что сделать | Почему |
|---|---|---|
| `build_plan.py`, `run_cycle.py`, `notify_failure.py`, `app/README.md` | ничего | каждый прогон и каждый алерт стартует новым процессом и сам подхватит новую версию |
| `amo.py` | рестарт `academy-dedup-webhook.service` | его импортирует и постоянный приёмник; без рестарта там остаётся старая версия |
| `webhook_daemon.py` | рестарт `academy-dedup-webhook.service` | постоянный процесс |
| `systemd/academy-dedup.service`, `systemd/academy-dedup-alert@.service` | `systemctl daemon-reload` | |
| `systemd/academy-dedup-webhook.service` | `daemon-reload` и рестарт `academy-dedup-webhook.service` | |
| `systemd/academy-dedup.timer` | `daemon-reload` и `systemctl restart academy-dedup.timer` | |

Рестарт приёмника — боевое действие, его нужно согласовать с Катей заранее, как часть плана
выкатки. Вебхуки, пропущенные за время рестарта, ничего не ломают: каждый прогон читает всю
воронку заново, и дубли подберёт следующий вебхук или суточный аудит около 03:30 UTC.

## Порядок выкатки

С офисного компа `ssh` и `scp` работают только из PowerShell (ключ лежит в ssh-agent Windows).
Подключения лучше собирать в одно: на `85.193.91.169` стоит fail2ban. Команды длиннее одной
строки кладите скриптом в файл, `scp` в `/tmp`, запускайте одним `ssh … 'sudo -n bash /tmp/…'`.

**0. Лок.** Займите ресурс «сторож дублей Академии» в `LOCKS.md` рабочей папки и снимите после
выкатки.

**1. Бой = git?** Перед любой правкой убедитесь, что на сервере ровно то, что лежит в `master`.
15.10.2026 правка `build_plan.py` и `run_cycle.py` какое-то время существовала только на
сервере, и заливка из git стёрла бы её.

На сервере:

```sh
sudo sh -c 'export LC_ALL=C; cd /opt/academy-dedup && md5sum *.py README.md; cd /etc/systemd/system && md5sum academy-dedup*'
```

В репозитории (рабочее дерево чистое, `git status` пуст):

```sh
export LC_ALL=C; cd ops/academy-dedup/app && md5sum *.py README.md; cd ../systemd && md5sum academy-dedup*
```

Вывод должен совпасть построчно. `LC_ALL=C` нужен, чтобы маска `academy-dedup*` раскрылась в
одном порядке на обеих сторонах. Если хоть один файл отличается, ничего не заливайте. Сначала
найдите боевую версию в git: прогоните её md5 по всем веткам
(`git rev-parse <ветка>:ops/academy-dedup/app/<файл>`, затем `git cat-file blob <хэш> | md5sum`).
Нашлась — значит, ветку выкатили и не влили; это вопрос слияния и владельца ветки. Не нашлась —
снимите боевой файл в git отдельным коммитом «снимок боя» и только потом правьте поверх него.

**2. Проверка правки.** Локально: `python3 -m py_compile app/*.py`. Остальное запускается
только на сервере: модулю `fcntl` нужен Linux. Положите в `/tmp/academy-dedup-deploy/` ВСЕ файлы
`app/` из коммита, а не одни изменённые: скрипты импортируют соседей из своей папки. Затем:

```sh
sudo python3 /tmp/academy-dedup-deploy/run_cycle.py --self-test
sudo env AMO_ENV=/opt/integrations/amo_fix_fields/.env \
  python3 /tmp/academy-dedup-deploy/build_plan.py --out /tmp/academy-dedup-deploy/plan --any-stage
```

Самотест обходится без сети и amo. Планировщик только читает amo: в `amo.py` есть одни GET.
`AMO_ENV` обязателен: без него `amo.py` ищет токен по путям с мака, а `run_cycle.py` передаёт
эту переменную планировщику сам. Планировщик пишет `plan.json`, `suspicious.json`,
`skipped.json` и `report.txt` в папку `--out`. Без `--out` он пишет прямо в
`/opt/academy-dedup`, поэтому `--out` указывайте всегда. Число групп в плане сравните с
последним прогоном в `/var/lib/academy-dedup/runs/`.

**3. Замена.** Одним скриптом под `sudo`, по шагам:

```sh
set -eu
NEW=/tmp/academy-dedup-deploy                      # файлы из коммита
APP=/opt/academy-dedup
B=/opt/backups/academy-dedup-<тема>-$(date +%Y%m%d-%H%M%S)

# 3.1 бой всё ещё тот, что сверяли на шаге 1: md5 файла в git ДО правки
echo "<md5 до>  $APP/run_cycle.py" | md5sum -c -

# 3.2 бэкап
mkdir -p "$B/app" "$B/systemd"
cp -a "$APP"/*.py "$APP"/README.md "$B/app/"
cp -a /etc/systemd/system/academy-dedup* "$B/systemd/"

# 3.3 замена под замком прогона: идущий прогон доработает, новый не стартует посреди замены
exec 9>/var/lib/academy-dedup/cycle.lock
flock -w 1500 9
for f in run_cycle.py; do                          # только изменённые файлы
  install -o root -g root -m 700 "$NEW/$f" "$APP/.$f.new"
  mv -f "$APP/.$f.new" "$APP/$f"                   # переименование атомарно
done
flock -u 9

# 3.4 после замены бой = коммит: md5 файла в git ПОСЛЕ правки
echo "<md5 после>  $APP/run_cycle.py" | md5sum -c -
```

По строке `md5sum -c` на каждый заменяемый файл. Ожидаемые md5 берите из git, а не с сервера:
«до» — из коммита, с которым бой сверен на шаге 1, «после» — из коммита с правкой. Юниты
ставятся так же, с правами `644` в `/etc/systemd/system/`, после них
`systemctl daemon-reload`. Рестарты — по таблице выше.

Пока `flock` держит замок, прогон, который запустит вебхук, сразу выйдет со строкой
`another cycle is running; exit` и кодом 0. Это не ошибка: следующий прогон снова прочитает всю
воронку. Ждать замок дольше 25 минут не придётся: у прогона `TimeoutStartSec=20min`.

**4. Проверка на бою.**

- md5 боевых файлов = md5 файлов коммита (шаг 3.4).
- `systemctl is-active academy-dedup-webhook.service` → `active`;
  `curl -s 127.0.0.1:8031/health` → `{"status": "ok", "pending": …}`.
- `sudo python3 /opt/academy-dedup/run_cycle.py` без `--apply` — сухой прогон боевым кодом:
  строит план, пишет папку прогона в `runs/` и ничего не склеивает.
- Первый настоящий прогон после выкатки: `journalctl -u academy-dedup.service -n 50`, строка
  `cycle complete: merged=…, skipped=…` без `failed`. Запуск руками
  (`systemctl start academy-dedup.service`) — это настоящая склейка, ровно то, что делает
  вебхук; запускать только по согласованию.

**5. Git и журнал.** Коммит с правкой должен оказаться в `master` до выкатки или сразу после
неё. Ветку, которую выкатили и не влили, следующий человек сотрёт заливкой из `master`. В
сообщении коммита укажите, что выкачено, и путь бэкапа. Запись в журнал — в
`projects/amo-cleanup/JOURNAL.md` рабочей папки.

## Откат

Верните файлы из бэкапа шага 3.2 тем же приёмом:

```sh
B=/opt/backups/academy-dedup-<тема>-<время>
sudo flock -w 1500 /var/lib/academy-dedup/cycle.lock sh -c "
  install -o root -g root -m 700 $B/app/*.py /opt/academy-dedup/ &&
  install -o root -g root -m 644 $B/app/README.md /opt/academy-dedup/ &&
  install -o root -g root -m 644 $B/systemd/* /etc/systemd/system/"
sudo systemctl daemon-reload
```

Файлы, которые выкатка добавила и которых нет в бэкапе, уберите руками. Если при выкатке
перезапускали приёмник, перезапустите его и после отката. Затем проверьте md5 по шагу 1: бой
должен совпасть с коммитом до правки.
