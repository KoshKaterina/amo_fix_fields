"""Первый проход подбора по всем старым сделкам одним потоком и потолок прогона от `per_hour` (`ai_engine_sweep`, просьба Кати 23.11.2026).

Что проверяем:
  • взвод: проход взведён на свежей таблице, взводится заново, когда движок видит подбор выключенным (одна запись на выключение; протухшие и чужие настройки не в счёт);
  • проход берёт ВСЁ подряд выше `per_hour`, `per_run`, потолка просмотров и `MAX_OPEN_PROPOSALS`; порциями с новым `exclude`; по порядку панели; сделки по одной;
  • проход встаёт при закрытом окне рабочих часов, рубильнике, сменённых настройках и продолжается с того же места (в том числе после рестарта) без повторов;
  • две неудачи подряд дают паузу `RETRY_ERROR_S`; конец: порция пуста, остались только отложенные, потолок `PASS_MAX`; пустой список сразу снимает взвод;
  • те же защиты, что в обычном режиме (отметки, владельцы, задачи, фильтры), предложения пилота без строки в чат ОП на каждое и один итог в конце;
  • потолок прогона `per_run = max(2, ceil(per_hour / 12))`, просмотров `max(6, 3 * per_run)`; срез состояния `first_pass`; сужение порции до 30 при 422 от прежней панели.

Сети нет: amo, МойСклад, панель и ИИ-сервис подставные. Запуск: python -m pytest test_ai_engine_sweep_pass.py -q
"""
import logging

import pytest

import ai_engine as E
import ai_engine_approval as A
import ai_engine_config as cfg
import ai_engine_marks as M
import ai_engine_settings as settings
import ai_engine_store as store
import ai_engine_sweep as S
from test_ai_engine import DEAL, PIPE_RETAIL, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_filters import AI_USER, OLD_MANAGER
from test_ai_engine_followup import drain
from test_ai_engine_sweep import HOUR, ON, PIPE_OTHER, _fresh, allow_other, iso, last_row, setup, w  # noqa: F401  (_fresh, w - фикстуры)


def make_deals(w, n, start=DEAL, **kw):
    return [w.deal(start + i, **kw) for i in range(n)]


def owners(n, start=DEAL):
    return [store.get_owner(start + i) for i in range(n)]


# =============================================================================================
# взвод
# =============================================================================================

def test_the_pass_is_armed_on_a_fresh_table(env):
    assert S.pass_state() == {"armed": True, "active": False, "started_at": None, "taken": 0, "examined": 0, "finished_at": None}
    assert S.pass_state()["armed"] is True and E.status()["sweep"]["first_pass"]["armed"] is True


def test_the_pass_is_armed_again_once_when_the_engine_sees_the_sweep_off(env):
    async def go():
        setup(env)                                                                              # проход закончен
        assert S.pass_state()["armed"] is False
        calls = []
        real = S.pass_arm
        env.mp.setattr(S, "pass_arm", lambda: (calls.append(1), real())[1])
        setup(env, sweep={**ON, "enabled": False}, armed=None)
        for _ in range(5):
            await S._watch_disabled(E)
        assert len(calls) == 1 and S.pass_state()["armed"] is True                              # записано один раз, а не на каждом проходе таймеров
        S.pass_finish(env.t)
        setup(env, armed=None)                                                                  # включили: метка снята
        await S._watch_disabled(E)
        assert len(calls) == 1 and S.pass_state()["armed"] is False
        for _ in range(3):
            setup(env, sweep=None, armed=None)                                                  # блока нет: это тоже «выключено»
            await S._watch_disabled(E)
        assert len(calls) == 2 and S.pass_state()["armed"] is True
        await S.tick(E, env.t)                                                                  # то же самое делает проход таймеров
        assert len(calls) == 2
    run(go())


def test_pass_arm_writes_only_on_a_change(env):
    assert S.pass_arm() == 0                                                                    # свежая таблица уже взведена
    S.pass_finish(1.0)
    assert S.pass_arm() == 1 and S.pass_arm() == 0
    S.pass_begin(5.0)
    assert S.pass_state()["active"] is True and S.pass_arm() == 1 and S.pass_state()["active"] is False


def test_stale_or_foreign_settings_do_not_re_arm(env):
    async def go():
        setup(env)
        setup(env, sweep=None, armed=None)
        settings.set_document(settings.document(), fetched_at=env.t - 100000)                    # настройки протухли: «выключено» не видно
        await S._watch_disabled(E)
        assert S.pass_state()["armed"] is False
        env.set_settings(mode="prod", pipelines=[PIPE_RETAIL], existing={"policy": "leave"})     # документ версии 1: блоков версии 2 он не читает
        await S._watch_disabled(E)
        assert S.pass_state()["armed"] is False
    run(go())


def test_disabling_in_the_middle_of_a_pass_abandons_it_and_a_new_pass_starts_from_zero(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 4)
        env.mp.setattr(S, "PASS_LIMIT", 2)
        real = S._judge
        seen = []

        async def judge(*a, **k):
            res = await real(*a, **k)
            seen.append(1)
            if len(seen) == 2:
                setup(env, sweep={**ON, "enabled": False}, armed=None)
                await S._watch_disabled(E)
            return res

        env.mp.setattr(S, "_judge", judge)
        out = await S.run_once(E)
        assert "приостановлен" in out["skipped"] and S.pass_state()["armed"] is True and S.pass_state()["active"] is False
        assert S.pass_state()["taken"] == 2                                                     # счётчики старого прохода видны до нового начала
        env.mp.setattr(S, "_judge", real)
        setup(env, armed=None)
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["outcomes"] == {"given": 2} and (st["armed"], st["taken"], st["examined"]) == (False, 2, 2)      # уже взятые журнал не повторил, счёт нового прохода с нуля
    run(go())


# =============================================================================================
# проход одним потоком
# =============================================================================================

def test_the_pass_takes_everything_above_every_limit_in_one_go(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 3}, armed=True)
        env.mp.setattr(S, "MAX_OPEN_PROPOSALS", 1)
        cands = make_deals(w, 25)
        out = await S.run_once(E)
        assert out["first_pass"] is True and out["outcomes"] == {"given": 25} and out["skipped"] is None
        st = S.pass_state()
        assert (st["armed"], st["active"], st["taken"], st["examined"]) == (False, False, 25, 25) and st["started_at"] and st["finished_at"]
        assert [o["owner"] for o in owners(25)] == ["ai"] * 25 and all(o["epoch"] == 1 for o in owners(25))
        assert len(w.requests) == 2 and w.requests[0]["body"]["limit"] == 50 and w.requests[0]["body"]["exclude"] == []        # вторая порция пуста
        assert len(w.requests[1]["body"]["exclude"]) == 25
        assert [r["deal_id"] for r in reversed(S.journal())] == [c["deal_id"] for c in cands]                                      # порядок панели
        out = await S.run_once(E)                                                                                                    # дальше обычный режим с `per_hour`
        assert not out.get("first_pass") and out["skipped"].startswith("лимит за час исчерпан (25 из 3)")
    run(go())


def test_the_pass_asks_the_panel_in_portions_with_a_growing_exclude(env, w):
    async def go():
        setup(env, armed=True)
        env.mp.setattr(S, "PASS_LIMIT", 4)
        make_deals(w, 10)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 10}
        assert [r["body"]["limit"] for r in w.requests] == [4, 4, 4, 4]
        assert [len(r["body"]["exclude"]) for r in w.requests] == [0, 4, 8, 10]
        assert all(r["body"]["limit"] <= 50 and set(r["body"]) == {"retail_pipelines", "other_pipelines", "ai_user_id", "other_days", "exclude", "limit"} for r in w.requests)
    run(go())


def test_the_pass_takes_deals_one_by_one_never_in_parallel(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 5)
        real = S._attempt
        depth = []
        peak = [0]

        async def attempt(*a, **k):
            depth.append(1)
            peak[0] = max(peak[0], len(depth))
            try:
                return await real(*a, **k)
            finally:
                depth.pop()

        env.mp.setattr(S, "_attempt", attempt)
        await S.run_once(E)
        assert peak[0] == 1 and len(owners(5)) == 5
    run(go())


def test_the_pass_ceiling_leaves_the_rest_to_the_ordinary_sweep(env, w):
    async def go():
        setup(env, armed=True)
        env.mp.setattr(S, "PASS_MAX", 5)
        make_deals(w, 12)
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["outcomes"] == {"given": 5} and (st["armed"], st["examined"], st["taken"]) == (False, 5, 5)
        out = await S.run_once(E)                                                                # обычный режим: бюджет часа 10, осталось 5, за прогон 2
        assert not out.get("first_pass") and out["outcomes"] == {"given": 2}
    run(go())


def test_an_empty_list_switches_the_pass_off_at_once(env, w):
    async def go():
        setup(env, armed=True)
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["first_pass"] and out["candidates"] == 0 and out["outcomes"] == {}
        assert (st["armed"], st["active"], st["taken"], st["examined"]) == (False, False, 0, 0) and st["finished_at"] is not None and st["started_at"] is not None
        assert len(w.requests) == 1
    run(go())


def test_only_deferred_deals_left_means_the_end(env, w):
    async def go():
        setup(env, armed=True)
        for i in range(3):
            w.deal(DEAL + i, waiting=False, out_ago=HOUR)                                       # молчат меньше порога: `too_early`
        w.deal(DEAL + 3)
        w.tasks[DEAL + 3] = [{"id": 1, "responsible_user_id": OLD_MANAGER}]                      # задача у сотрудника
        out = await S.run_once(E)
        assert out["outcomes"] == {"too_early": 3, "blocked_by_task": 1}
        st = S.pass_state()
        assert (st["armed"], st["taken"], st["examined"]) == (False, 0, 4) and len(w.requests) == 2
        assert all(store.get_owner(DEAL + i) is None for i in range(4))
        assert all(last_row(DEAL + i)["retry_at"] for i in range(4))                             # вернутся обычным подбором, когда срок пройдёт
    run(go())


def test_the_end_of_the_pass_writes_one_summary_line_to_the_log_and_the_chat(env, w, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        setup(env, armed=True)
        make_deals(w, 3)
        await S.run_once(E)
        lines = [r.getMessage() for r in caplog.records if "первый проход по старым сделкам закончен" in r.getMessage()]
        assert len(lines) == 1 and "просмотрено 3, взято 3" in lines[0] and "Тестовый Клиент" not in caplog.text
        assert len([r for r in caplog.records if "первый проход по старым сделкам начат" in r.getMessage()]) == 1
        assert len(env.alerts) == 1 and "первый проход" in env.alerts[0]
    run(go())


# =============================================================================================
# пилот: одна общая очередь
# =============================================================================================

def test_the_pilot_gets_one_queue_without_a_cap_and_the_chat_one_line(env, w):
    async def go():
        setup(env, review=True, armed=True)
        env.mp.setattr(S, "MAX_OPEN_PROPOSALS", 2)
        make_deals(w, 8)
        out = await S.run_once(E)
        assert out["outcomes"] == {"reassign_pending": 8} and A.count_open() == 8
        assert {r["source"] for r in A.journal()} == {"sweep"} and all(o is None for o in owners(8))
        assert len(env.alerts) == 1 and "первый проход" in env.alerts[0] and "ИИ просит разрешения" not in env.alerts[0]       # строки на каждое предложение нет
        assert "предложений ждёт решения в «Пилоте» 8" in env.alerts[0]
        out = await S.run_once(E)                                                                 # обычный режим снова держит потолок очереди
        assert not out.get("first_pass") and "в очереди пилота уже 8" in out["skipped"]
    run(go())


def test_a_proposal_of_the_ordinary_mode_still_writes_its_line(env, w):
    async def go():
        setup(env, review=True)
        w.deal()
        await S.run_once(E)
        assert len(env.alerts) == 1 and "ИИ просит разрешения" in env.alerts[0]
    run(go())


def test_many_open_proposals_are_swept_for_staleness_less_often(env):
    async def go():
        calls = []

        async def counting(eng, now):
            calls.append(now)

        env.mp.setattr(A, "sweep", counting)
        env.mp.setattr(A, "_last_sweep", 0.0)
        for i in range(25):
            A.create(deal_id=900 + i, from_id=1, from_name="x", to_id=2, to_name="y", chat={}, now=env.t)
        for sec in range(0, 25):
            await A.tick(E, env.t + sec)
        assert len(calls) <= 3                                                                   # раз в десять секунд, а не каждый проход таймеров
        calls.clear()
        with A._Db() as conn:
            conn.execute("DELETE FROM reassign_proposal WHERE deal_id>=905")                     # осталось пять: проверка каждый проход, как раньше
        env.mp.setattr(A, "_last_sweep", 0.0)
        for sec in range(5):
            await A.tick(E, env.t + 100 + sec)
        assert len(calls) == 5
    run(go())


# =============================================================================================
# остановка и продолжение
# =============================================================================================

def pause_after(env, n, action):
    """Подменить `_judge`: после n-го решения выполнить `action`. Возвращает настоящий `_judge`."""
    real = S._judge
    seen = []

    async def judge(*a, **k):
        res = await real(*a, **k)
        seen.append(1)
        if len(seen) == n:
            action()
        return res

    env.mp.setattr(S, "_judge", judge)
    return real


def test_a_closed_window_pauses_the_pass_and_the_next_window_continues_from_the_same_place(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 6)
        real = pause_after(env, 3, lambda: setup(env, work_hours=[], armed=None))
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["skipped"] == "проход приостановлен: сейчас вне рабочих часов"
        assert (st["armed"], st["active"], st["taken"], st["examined"]) == (True, True, 3, 3)
        started = st["started_at"]
        env.mp.setattr(S, "_judge", real)
        out = await S.run_once(E)                                                                # окна нет: прогон ничего не делает, проход стоит
        assert out["skipped"] == "сейчас вне рабочих часов" and S.pass_state()["taken"] == 3
        setup(env, armed=None)                                                                   # окно открылось
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["outcomes"] == {"given": 3} and (st["armed"], st["active"], st["taken"], st["examined"]) == (False, False, 6, 6)
        assert st["started_at"] == started                                                       # тот же проход, не новый
        assert all(o["owner"] == "ai" and o["epoch"] == 1 for o in owners(6))                    # никто не взят дважды
    run(go())


def test_the_kill_switch_stops_the_pass_and_it_goes_on_when_it_is_off(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 5)
        real = pause_after(env, 2, lambda: env.mp.setattr(cfg, "SWEEP_DISABLED", True))
        out = await S.run_once(E)
        assert "AI_SWEEP_DISABLED" in out["skipped"] and S.pass_state()["taken"] == 2 and S.pass_state()["active"] is True
        env.mp.setattr(S, "_judge", real)
        env.mp.setattr(cfg, "SWEEP_DISABLED", False)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 3} and S.pass_state()["taken"] == 5 and S.pass_state()["armed"] is False
    run(go())


def test_the_ai_service_going_down_pauses_the_pass(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 4)

        def die():
            env.health_ok = False
            env.mp.setattr(E, "_ai_ok_at", env.t - 1000)

        real = pause_after(env, 2, die)
        out = await S.run_once(E)
        assert out["skipped"] == "проход приостановлен: ИИ-сервис недоступен" and S.pass_state()["taken"] == 2
        env.mp.setattr(S, "_judge", real)
        env.health_ok = True
        env.mp.setattr(E, "_ai_ok_at", env.t)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 2} and S.pass_state()["armed"] is False
    run(go())


def test_a_restart_in_the_middle_continues_the_same_pass(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 6)
        real = pause_after(env, 4, lambda: setup(env, work_hours=[], armed=None))
        await S.run_once(E)
        before = S.pass_state()
        env.mp.setattr(S, "_judge", real)
        env.mp.setattr(S, "_task", None)                                                         # контейнер перезапустили: память пуста, база на месте
        env.mp.setattr(S, "_next_run_at", 0.0)
        env.mp.setattr(S, "_state", {"last_run_at": None, "last_candidates": None, "total_open": None, "last_result": ""})
        env.mp.setattr(S, "_disabled_noted", False)
        setup(env, armed=None)
        assert S.pass_state() == before and before["active"] is True
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["outcomes"] == {"given": 2} and (st["taken"], st["examined"], st["armed"]) == (6, 6, False) and st["started_at"] == before["started_at"]
        assert all(o["epoch"] == 1 for o in owners(6))
    run(go())


def test_two_failures_in_a_row_pause_the_pass_for_fifteen_minutes(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 5)
        for i in range(5):
            w.tasks[DEAL + i] = None                                                             # amo не отвечает по задачам
        out = await S.run_once(E)
        assert out["outcomes"] == {"amo_unavailable": 2} and "приостановлен на 15 мин" in out["skipped"]
        assert S._next_run_at >= env.t + S.RETRY_ERROR_S and S.pass_state()["active"] is True and S.pass_state()["armed"] is True
        env.mp.setattr(S, "_next_run_at", env.t + S.RETRY_ERROR_S)
        await S.tick(E, env.t + 100)
        await drain()
        assert len(w.requests) == 1                                                              # пауза: прогон не запускается
        w.tasks.clear()                                                                          # amo ожила
        env.t += S.RETRY_ERROR_S + 1
        await S.tick(E, env.t)
        await drain()
        st = S.pass_state()
        assert (st["armed"], st["taken"]) == (False, 5) and all(o["owner"] == "ai" for o in owners(5))
    run(go())


def test_a_silent_panel_leaves_the_pass_unstarted(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 2)
        w.status = 404
        out = await S.run_once(E)
        assert out["skipped"] == "панель не отдала кандидатов" and S.pass_state()["active"] is False and S.pass_state()["started_at"] is None
        w.status = 200
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 2} and S.pass_state()["armed"] is False
    run(go())


def test_a_candidate_the_panel_keeps_returning_does_not_loop_the_pass(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 3)
        w.raw_candidates = True                                                                  # панель вернула всё же в каждой порции (потолок `exclude`, сбой отбора)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 3} and len(w.requests) == 2 and S.pass_state()["armed"] is False
        assert all(o["epoch"] == 1 for o in owners(3))
    run(go())


def test_an_older_panel_with_a_lower_limit_narrows_the_portion_to_thirty(env, w):
    async def go():
        setup(env, armed=True)
        w.max_limit = 30
        make_deals(w, 3)
        out = await S.run_once(E)
        assert [r["body"]["limit"] for r in w.requests] == [50, 30, 30] and out["outcomes"] == {"given": 3}
        assert S._state["portion_limit"] == 30
    run(go())


# =============================================================================================
# те же защиты, что в обычном режиме
# =============================================================================================

def test_the_pass_keeps_every_protection_of_the_ordinary_mode(env, w):
    async def go():
        setup(env, armed=True, budget={"skip_from_rub": 1000})
        removed = w.deal(DEAL)
        owned = w.deal(DEAL + 1)
        rich = w.deal(DEAL + 2, price=5000)
        busy = w.deal(DEAL + 3)
        fine = w.deal(DEAL + 4)
        M.put(DEAL, M.KIND_REMOVED, env.t - HOUR)
        store.set_owner(DEAL + 1, store.OWNER_HUMAN, reason="manager_message", mode="full")
        w.tasks[DEAL + 3] = [{"id": 7, "responsible_user_id": OLD_MANAGER}]
        w.raw_candidates = True                                                                  # панель вернула всё без отбора: защиту держит сам движок
        out = await S.run_once(E)
        assert out["outcomes"] == {"marked": 1, "refused": 2, "blocked_by_task": 1, "given": 1}
        assert store.get_owner(DEAL) is None and store.get_owner(DEAL + 1)["owner"] == "human" and store.get_owner(DEAL + 2) is None
        assert store.get_owner(DEAL + 3) is None and store.get_owner(DEAL + 4)["owner"] == "ai"
        assert w.amo.patches == [(DEAL + 4, 11513202)] and env.tasks_open == [] and removed and owned and rich and busy and fine
        assert len(env.alerts) == 1 and "первый проход" in env.alerts[0]
    run(go())


def test_the_pass_calls_nobody_for_a_client_with_two_deals(env, w):
    async def go():
        setup(env, armed=True)
        first = w.deal(DEAL)
        w.deal(DEAL + 1)
        for ld in env.leads:
            ld["_q"] = first["chat_id"]
        for cand in w.candidates:
            cand["chat_id"] = first["chat_id"]
        out = await S.run_once(E)
        assert out["outcomes"] == {"needs_human": 2} and env.tasks_open == [] and env.notes == []
        assert len(env.alerts) == 1 and "первый проход" in env.alerts[0]                          # только итог прохода, ни алертов о дублях
    run(go())


def test_the_pass_goes_through_the_timer_loop(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 4)
        env.mp.setattr(S, "_next_run_at", 0.0)
        await env.advance(1)
        await drain()
        assert S.pass_state()["armed"] is False and S.pass_state()["taken"] == 4 and all(o["owner"] == "ai" for o in owners(4))
        block = E.status()["sweep"]["first_pass"]
        assert block["armed"] is False and block["active"] is False and block["taken"] == 4 and block["examined"] == 4
        assert block["started_at"].endswith("Z") and block["finished_at"].endswith("Z")
    run(go())


def test_the_pass_does_not_start_outside_the_window(env, w):
    async def go():
        setup(env, armed=True, work_hours=[])
        make_deals(w, 2)
        env.mp.setattr(S, "_next_run_at", 0.0)
        await env.advance(1)
        await drain()
        assert w.requests == [] and S.pass_state() == {"armed": True, "active": False, "started_at": None, "taken": 0, "examined": 0, "finished_at": None}
    run(go())


def test_the_status_slice_shows_the_first_pass(env, w):
    async def go():
        setup(env, armed=True)
        make_deals(w, 5)
        assert E.status()["sweep"]["first_pass"] == {"armed": True, "active": False, "started_at": None, "taken": 0, "examined": 0, "finished_at": None}
        pause_after(env, 3, lambda: setup(env, work_hours=[], armed=None))
        await S.run_once(E)
        mid = E.status()["sweep"]["first_pass"]
        assert (mid["armed"], mid["active"], mid["taken"], mid["examined"]) == (True, True, 3, 3) and mid["finished_at"] is None and mid["started_at"]
    run(go())


# =============================================================================================
# потолок прогона следует из per_hour
# =============================================================================================

@pytest.mark.parametrize("per_hour, run_cap, examine_cap", [(1, 2, 6), (10, 2, 6), (24, 2, 6), (25, 3, 9), (36, 3, 9), (37, 4, 12), (48, 4, 12), (49, 5, 15), (60, 5, 15)])
def test_the_run_ceiling_follows_per_hour(per_hour, run_cap, examine_cap):
    assert S.per_run(per_hour) == run_cap and S.max_examine(per_hour) == examine_cap


def test_a_big_per_hour_takes_more_than_two_per_run(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 60})
        make_deals(w, 12)
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 5}                                                   # ceil(60 / 12) = 5 за прогон, а не прежние 2
        out = await S.run_once(E)
        assert out["outcomes"] == {"given": 5}
        assert S.count_taken(env.t) == 10
    run(go())


def test_the_default_per_hour_still_takes_two_per_run(env, w):
    async def go():
        setup(env)
        make_deals(w, 6)
        assert (await S.run_once(E))["outcomes"] == {"given": 2}
    run(go())


def test_the_hourly_budget_stays_the_main_limit_with_a_bigger_run_ceiling(env, w):
    async def go():
        setup(env, sweep={**ON, "per_hour": 60})
        make_deals(w, 80)
        taken = 0
        for _ in range(14):
            taken += (await S.run_once(E))["outcomes"].get("given", 0)
        assert taken == 60 and S.count_taken(env.t) == 60                                        # 12 прогонов по 5 исчерпали час, дальше «лимит»
        assert "лимит за час исчерпан (60 из 60)" in (await S.run_once(E))["skipped"]
    run(go())


# =============================================================================================
# задачи amo: смотрим всегда, и в первом проходе тоже (решение Кати 24.11.2026)
# =============================================================================================

def test_a_task_blocks_the_deal_inside_the_pass_in_both_pools_and_the_pass_goes_on(env, w):
    async def go():
        setup(env, armed=True, pipelines_any=True)
        allow_other(env)
        w.deal(DEAL, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 1, pipeline=PIPE_OTHER)
        w.deal(DEAL + 2, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 3, pipeline=PIPE_OTHER)
        w.tasks[DEAL] = w.tasks[DEAL + 1] = [{"id": 1, "responsible_user_id": OLD_MANAGER, "text": "позвонить"}]
        w.tasks[DEAL + 3] = [{"id": 2, "responsible_user_id": AI_USER["id"]}]                  # только на пользователе ИИ: не мешает
        out = await S.run_once(E)
        st = S.pass_state()
        assert out["first_pass"] and out["outcomes"] == {"blocked_by_task": 2, "given": 2}
        assert (st["armed"], st["taken"], st["examined"]) == (False, 2, 4)                       # проход не встал на заблокированных и дошёл до конца
        assert sorted(w.tasks_calls) == [DEAL, DEAL + 1, DEAL + 2, DEAL + 3]                     # один живой запрос на сделку
        for deal in (DEAL, DEAL + 1):
            row = last_row(deal)
            assert row["outcome"] == "blocked_by_task" and abs(row["retry_at"] - (env.t + 6 * HOUR)) < 1 and store.get_owner(deal) is None
        assert store.get_owner(DEAL + 2)["owner"] == "ai" and store.get_owner(DEAL + 3)["owner"] == "ai"
        assert env.tasks_open == [] and w.amo.patches == [(DEAL + 2, 11513202), (DEAL + 3, 11513202)]
        assert DEAL in w.requests[-1]["body"]["exclude"] and DEAL + 1 in w.requests[-1]["body"]["exclude"]    # отложенные не возвращаются в порциях
    run(go())


def test_a_deal_with_a_task_comes_back_to_the_ordinary_sweep_after_six_hours(env, w):
    async def go():
        setup(env, armed=True)
        w.deal()
        w.tasks[DEAL] = [{"id": 1, "responsible_user_id": OLD_MANAGER}]
        await S.run_once(E)
        assert S.pass_state()["armed"] is False and store.get_owner(DEAL) is None
        w.tasks[DEAL] = []                                                                      # задачу закрыли
        env.t += 6 * HOUR + 1
        out = await S.run_once(E)
        assert not out.get("first_pass") and out["outcomes"] == {"given": 1}
    run(go())


def test_amo_silence_about_tasks_is_one_failure_in_the_pass_and_does_not_stop_it(env, w):
    async def go():
        setup(env, armed=True, pipelines_any=True)
        allow_other(env)
        w.deal(DEAL, pipeline=PIPE_OTHER)
        w.deal(DEAL + 1, pipeline=PIPE_RETAIL)
        w.deal(DEAL + 2, pipeline=PIPE_OTHER)
        w.tasks[DEAL] = None                                                                    # одна неудача подряд: пауза не нужна
        out = await S.run_once(E)
        assert out["outcomes"] == {"amo_unavailable": 1, "given": 2} and S.pass_state()["armed"] is False
        row = last_row(DEAL)
        assert row["outcome"] == "amo_unavailable" and abs(row["retry_at"] - (env.t + 15 * 60)) < 1 and store.get_owner(DEAL) is None
    run(go())


def test_two_amo_silences_about_tasks_in_a_row_pause_the_pass_in_the_other_pool_too(env, w):
    async def go():
        setup(env, armed=True, pipelines_any=True)
        allow_other(env)
        for i in range(4):
            w.deal(DEAL + i, pipeline=PIPE_OTHER)
            w.tasks[DEAL + i] = None
        out = await S.run_once(E)
        assert out["outcomes"] == {"amo_unavailable": 2} and "приостановлен на 15 мин" in out["skipped"]
        assert S.pass_state()["active"] is True and S._next_run_at >= env.t + S.RETRY_ERROR_S
    run(go())
