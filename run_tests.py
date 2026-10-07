"""Прогон тестов интеграции: КАЖДЫЙ файл своим процессом pytest.

    python run_tests.py                      # всё
    python run_tests.py test_waybill_*.py    # только подходящие файлы

⚠️ ПОЧЕМУ НЕ ОДНОЙ КОМАНДОЙ `pytest`. Файлы этого репозитория не изолированы друг от друга:
многие из них правят ОБЩИЕ вещи прямо на импорте - подменяют модули в `sys.modules`, функции
в `amo_service`/`alerts`, константы в `waybill_service`. Пока файл запускали скриптом, это
было безвредно: процесс заканчивался вместе с проверками. В одном процессе pytest подмена
достаётся соседям, и это не теория:

| прогон | итог (замер 07.10.2026) |
|---|---|
| по процессу на файл | **1446 проходит, 3 падает, висов НЕТ**, 102 с |
| всё одним процессом | повисает (`test_lead_distribution`, `test_api_senders`) и на Windows умирает целиком |

⚠️ На Windows `pytest-timeout` работает методом `thread` и при срабатывании снимает ВЕСЬ
процесс - значит один повисший тест кончает прогон. В CI на Linux доступен метод `signal`,
там снимается только зависший тест. Но даже в CI прогон одним процессом остаётся неверным:
тесты будут влиять друг на друга, просто молча.

Настоящее лечение - перевести подмены общего на фикстуры с возвратом (задачи 10и и 10к в
`research/PLAN-tehdolg-2026-09.md`). До тех пор процесс на файл - не костыль, а единственный
способ получить осмысленный результат.

Код выхода: 0, если ни в одном файле нет падений и висов; иначе 1.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

# Секунд на файл. Самый долгий сейчас идёт около 7 с, так что это аварийный предел.
PER_FILE_TIMEOUT_S = 90

_SUMMARY = re.compile(r"(\d+) (passed|failed|skipped|error)")


def find_files(patterns: list[str], root: pathlib.Path) -> list[pathlib.Path]:
    if patterns:
        found: list[pathlib.Path] = []
        for pattern in patterns:
            found.extend(sorted(root.glob(pattern)))
        return found
    return sorted(
        [p for p in root.glob("test_*.py")]
        + [p for p in (root / "ops" / "watchdog").glob("test_*.py")]
    )


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent
    files = find_files(argv, root)
    if not files:
        print("не нашла ни одного файла тестов по заданному образцу")
        return 1

    total = {"passed": 0, "failed": 0, "skipped": 0, "error": 0}
    bad: list[tuple[str, str]] = []

    for path in files:
        rel = path.relative_to(root).as_posix()
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pytest", rel, "-q", "--no-header",
                 "-p", "no:cacheprovider", "--tb=short"],
                cwd=root, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=PER_FILE_TIMEOUT_S,
            )
        except subprocess.TimeoutExpired:
            print(f"ВИС  {rel}: не уложился в {PER_FILE_TIMEOUT_S} с")
            bad.append((rel, "вис"))
            continue

        tail = [line for line in (proc.stdout or "").splitlines() if _SUMMARY.search(line)]
        summary = tail[-1].strip() if tail else "СВОДКИ НЕТ"
        counts = {kind: int(n) for n, kind in _SUMMARY.findall(summary)}
        for kind in total:
            total[kind] += counts.get(kind, 0)

        mark = "OK  "
        if counts.get("failed") or counts.get("error") or summary == "СВОДКИ НЕТ":
            mark = "СБОЙ"
            bad.append((rel, summary))
            if proc.stdout:
                print(proc.stdout.rstrip()[-2000:])
        print(f"{mark} {rel:46} {summary[:52]}")

    print()
    print("=" * 78)
    print(f"файлов: {len(files)} | прошло: {total['passed']} | упало: {total['failed']} "
          f"| пропущено: {total['skipped']} | ошибок: {total['error']}")
    if bad:
        print(f"неблагополучных файлов: {len(bad)}")
        for rel, why in bad:
            print(f"   {rel}: {why[:60]}")
        return 1
    print("все файлы прошли")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
