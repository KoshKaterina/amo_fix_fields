#!/usr/bin/env python3
"""Проверка правил «срочное против несрочного» и порога устойчивости.

Ради этого всё и затевалось: 22.09.2026 сторож прислал Кате восемь писем за
двадцать минут - «лежит», «работает», «лежит», «работает» - по юниту, который
на самом деле просто отрабатывал по таймеру. Тесты держат два обещания:
дребезг молчит, а настоящая поломка звонит ровно столько раз, сколько нужно.

Сеть и сервер не трогаем: triage - чистая функция, run_services работает на
подменённых проверках.

Запуск: python3 test_watchdog_triage.py
"""
import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("wd", Path(__file__).with_name("watchdog.py"))
w = importlib.util.module_from_spec(spec)
sys.modules["wd"] = w
spec.loader.exec_module(w)

w.log = lambda *a, **k: None
FAILS = []


def report(title, got, want):
    ok = got == want
    print(f"{'✓' if ok else '✗'} {title}: писем {len(got)} (ждали {len(want)})")
    if not ok:
        print(f"    получили {got}\n    ждали    {want}")
        FAILS.append(title)


# ── правила на голой машине состояний ──────────────────────────────────────
def drive(name, pattern, *, step=300, fail_to_open=2, ok_to_close=3, why="упал"):
    """pattern: 'x' - проверка не прошла, '.' - прошла. Шаг между прогонами step секунд."""
    prev, now, sent = {}, 1_700_000_000, []
    for ch in pattern:
        now += step
        broken, fixed, quiet, prev = w.triage(prev, [(name, ch == ".", why)], now,
                                              fail_to_open=fail_to_open, ok_to_close=ok_to_close)
        sent += ["🔴"] * len(broken) + ["✅"] * len(fixed) + ["🟡"] * len(quiet)
    return sent


report("дребезг несрочного юнита каждые 5 минут",
       drive("Сервис · team-idle-watch.service", "x.x.x.x.x.x."), [])
report("несрочное лежит десять часов подряд - одно письмо и тишина",
       drive("Таймер · team-nightly.timer", "x" * 120), ["🟡"])
report("срочный контейнер: падение, два напоминания, восстановление",
       drive("Контейнер · amo-fix-fields", "x" * 150 + "." * 5), ["🔴", "🔴", "🔴", "✅"])
report("срочное мигнуло один раз - порог устойчивости это глотает",
       drive("МойСклад", "..x....."), [])
report("токен склада упал (ходим раз в час, порог 1) - письмо сразу",
       drive("МойСклад", "xxx", step=3600, fail_to_open=1, ok_to_close=1), ["🔴"])

# ── те же правила, но через весь run_services ──────────────────────────────
STORE, SENT, CLOCK = {}, [], [1_700_000_000]
w.load_state = lambda: STORE
w.save_state = lambda s: STORE.update(s)
w.tg_send = lambda t: SENT.append(t.split("\n")[0][:1])
w.time.time = lambda: CLOCK[0]

DEAD = [("Сервис · team-idle-watch.service", False, "упал 22.09 13:05 МСК · KeyError")]


def tick(raw, minutes=5):
    CLOCK[0] += minutes * 60
    w.services_checks = lambda: list(raw)
    w.run_services()


for _ in range(6):
    tick(DEAD)
    tick([])
report("тот же дребезг через весь обход сервисов", SENT, [])
name_left = list((STORE.get("services") or {}).keys())
report("починившийся юнит не копится в состоянии", name_left, [])

SENT.clear()
for _ in range(80):
    tick(DEAD)
report("настоящее падение того же юнита: одно письмо за шесть часов", SENT, ["🟡"])

SENT.clear()
for _ in range(4):
    tick([])
report("несрочное починилось - про это не пишем вовсе", SENT, [])
report("имя ушло из состояния после починки",
       list((STORE.get("services") or {}).keys()), [])

print("\n" + ("правила срочности и порог устойчивости: все проверки прошли"
              if not FAILS else f"УПАЛО: {FAILS}"))
sys.exit(1 if FAILS else 0)
