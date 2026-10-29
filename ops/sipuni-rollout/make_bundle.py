"""Собирает пакет выкатки приёмника событий Sipuni из git-коммитов. Только читает git и пишет в каталог пакета.

Запуск (из любого места):
    python make_bundle.py <каталог_репозитория> <коммит_с_кодом> <коммит_базы_прода> <каталог_пакета>

В пакет попадают РОВНО файлы, которые меняются на проде, с окончаниями строк LF (так лежат на сервере):
    webhooks.py waybill_config.py lead_distribution.py uis_missed_call.py sipuni_events.py
Серверные скрипты ops/sipuni-rollout/*.sh копируются в корень пакета (LF из git).
MD5SUMS: суммы файлов пакета. PROD_MD5SUMS: суммы тех же файлов в базе прода, по ним серверный
скрипт проверяет, что прод не ушёл вперёд с момента подготовки.

Скрипт останавливается, если между базой и коммитом с кодом изменён ещё какой-то боевой .py в корне
репозитория (кроме тестов; каталог ops в образ как код не идёт): значит, пакет неполный и выкатка сломала бы прод.
"""
import hashlib
import os
import subprocess
import sys

CHANGED = ["webhooks.py", "waybill_config.py", "lead_distribution.py", "uis_missed_call.py", "sipuni_events.py"]


def git(repo, *args):
    r = subprocess.run(["git", "-C", repo, *args], capture_output=True)
    if r.returncode != 0:
        sys.exit("git " + " ".join(args) + ": " + r.stderr.decode("utf-8", "replace")[:300])
    return r.stdout


def md5(data):
    return hashlib.md5(data).hexdigest()


def lf(data):
    """Окончания строк LF: так файлы лежат на сервере, а git на Windows может отдать CRLF."""
    return data.replace(bytes([13, 10]), bytes([10]))


def main():
    repo, new, base, out = sys.argv[1:5]
    changed = git(repo, "diff", "--name-only", base, new).decode("utf-8").split()
    live = [f for f in changed if f.endswith(".py") and "/" not in f and not f.startswith("test_")]
    extra = sorted(set(live) - set(CHANGED))
    missing = sorted(set(CHANGED) - set(live))
    print("боевые .py, изменённые между базой и кодом:", sorted(live))
    if extra:
        sys.exit("СТОП: в коммите изменены боевые файлы вне пакета: " + ", ".join(extra))
    if missing:
        print("внимание: в пакете файлы без изменений относительно базы:", missing)
    os.makedirs(out, exist_ok=True)
    new_lines, prod_lines = [], []
    for name in CHANGED:
        data = lf(git(repo, "show", f"{new}:{name}"))
        with open(os.path.join(out, name), "wb") as fh:
            fh.write(data)
        new_lines.append(f"{md5(data)}  {name}")
        if name != "sipuni_events.py":
            base_data = lf(git(repo, "show", f"{base}:{name}"))
            prod_lines.append(f"{md5(base_data)}  {name}")
    with open(os.path.join(out, "MD5SUMS"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(new_lines) + "\n")
    with open(os.path.join(out, "PROD_MD5SUMS"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(prod_lines) + "\n")
    for script in sorted(git(repo, "ls-tree", "--name-only", new, "ops/sipuni-rollout/").decode("utf-8").split()):
        if script.endswith(".sh"):
            data = lf(git(repo, "show", f"{new}:{script}"))
            with open(os.path.join(out, os.path.basename(script)), "wb") as fh:
                fh.write(data)
    print("пакет собран:", out)
    print("\n".join(new_lines))


if __name__ == "__main__":
    main()
