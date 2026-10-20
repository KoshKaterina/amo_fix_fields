"""Одноразовое заполнение lead_distribution_routed_store из журнала распределений панели.

Зачем. До 20.10.2026 запись «сделка уже распределена» жила в суточном файле
счётчиков и обнулялась в полночь. Новое хранилище при выкатке пустое: без
заполнения любая сделка, распределённая ДО выкатки и всё ещё стоящая на точке
входа, при первом же обновлении была бы роздана заново — та самая бага, только
разом по всем. Журнал панели (`lead_distribution_log`, outcome='routed') знает
все такие решения.

Панель только читается. Скрипт сам в Postgres не ходит: выгрузку делает psql на
сервере, сюда она приходит на stdin, по строке на сделку, через табуляцию:

    lead_id <TAB> routed_at (unix-секунды) <TAB> assigned_user_id <TAB> profile_id

Выгрузка (сервер №2, только чтение):

    sudo -u postgres psql -d team_panel -At -F $'\\t' -c "
      SELECT DISTINCT ON (lead_id) lead_id, extract(epoch FROM created_at)::bigint,
             coalesce(assigned_user_id::text, ''), coalesce(profile_id, '')
      FROM lead_distribution_log
      WHERE outcome = 'routed' AND created_at > now() - interval '30 days'
      ORDER BY lead_id, created_at DESC" > /tmp/ld_routed.tsv

Запуск в контейнере (база — /app/var/lead_distribution_routed.sqlite3):

    docker exec -i amo-fix-fields python lead_distribution_routed_backfill.py --dry-run < /tmp/ld_routed.tsv
    docker exec -i amo-fix-fields python lead_distribution_routed_backfill.py < /tmp/ld_routed.tsv

Повторный запуск безопасен: строку, которая уже есть и новее журнальной, не трогает.
"""

import argparse
import sys
import time

import lead_distribution_routed_store as routed_store


def _parse(lines) -> tuple[list[tuple], int]:
    rows, bad = [], 0
    for line in lines:
        line = line.rstrip("\r\n")
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            bad += 1
            continue
        try:
            rows.append((int(parts[0]), int(float(parts[1])), parts[2] or None, parts[3] or None))
        except ValueError:
            bad += 1
    return rows, bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="только разобрать вход и показать итог")
    args = ap.parse_args()

    rows, bad = _parse(sys.stdin)
    cutoff = int(time.time()) - routed_store.ROUTED_TTL_S
    fresh = [r for r in rows if r[1] >= cutoff]
    print(f"строк на входе: {len(rows) + bad}, разобрано: {len(rows)}, битых: {bad}, "
          f"старше срока (пропущены): {len(rows) - len(fresh)}, сделок: {len({r[0] for r in fresh})}")
    if bad:
        print("⚠️ есть битые строки — проверь формат выгрузки (4 поля через табуляцию)")
    if args.dry_run:
        print(f"DRY-RUN, база {routed_store.DB_PATH} не тронута")
        return 0
    routed_store.init()
    changed = routed_store.backfill(fresh)
    print(f"записано/обновлено: {changed} в {routed_store.DB_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
