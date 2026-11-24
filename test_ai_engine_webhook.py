"""Путь по сообщению клиента (вебхук): открытая задача сотрудника и потеря сообщения в окне пропуска (решение Кати 24.11.2026).

Правило «задача»: при ПЕРВОМ взятии «существующей» сделки (создана до границы `existing.since`, режим «Полный») ИИ молчит, если на сделке открыта задача amo на любого сотрудника, кроме
пользователя ИИ (`ai_engine._gate_take`, `_task_guard`). Новые сделки правило не касается (автоматика ставит им задачу приёма, ИИ закрывает её сам: `release_intake`); сделку, которую ИИ уже ведёт,
кнопку «Дать ИИ» и перехват тоже. Живой запрос задач один; amo молчит: «задачи не проверить», а не «задач нет»; рубильник `AI_TASK_GUARD_DISABLED`; срез `GET /` показывает, включено ли оно.

Окно пропуска `S_SKIP`: сообщение клиента в окне больше не отбрасывается. Оно кладётся во входящие (повтор вебхука отсекается), в конце окна чат возвращается в разбор один раз, и сделка проходит
ВСЕ проверки заново; снова отказ: входящие сбрасываются и открывается новое окно. `awaiting` (предложение пилота) и `human` не меняются.
При ВРЕМЕННОМ отказе (задача сотрудника, «задачи не проверить», менеджер активен, ИИ-сервис не отвечал, сделку не нашли, авто-режим, смена ответственного не удалась, предложение пилота открыто или
отклонено) сохраняется и само сообщение, вызвавшее отказ; постоянный (фильтры, бюджет, товары, `leave`, нет пользователя ИИ, `AI_REASSIGN_DISABLED`, сделка не наша) сбрасывает его, как раньше.
Каждое сообщение пересматривается ровно один раз: отказ на пересмотре сбрасывает всё и открывает новое окно, дальше страхует подбор. Явно разрешённые контакты правилом задач не останавливаются.

Сети нет: подставной мир из test_ai_engine.py. Запуск: python -m pytest test_ai_engine_webhook.py -q
"""
import logging

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_filters as F
import ai_engine_settings as settings
import ai_engine_store as store
import ai_engine_tasks as T
from test_ai_engine import CHAN, CHAT, CHAT_KEY, DEAL, PHONE, env, run  # noqa: F401  (env - фикстура)
from test_ai_engine_approval import make_proposal
from test_ai_engine_filters import AI_USER, OLD_MANAGER, PILOT_LIST, Amo, client_writes, put_lead, set_full
from test_ai_engine_followup import drain
from test_ai_engine_give import give

HOUR = 3600


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    F._cache.clear()
    F._inflight.clear()
    F._warned.clear()
    settings.set_document(None)
    monkeypatch.setattr(F, "REASSIGN_DISABLED", False)
    monkeypatch.setattr(cfg, "TASK_GUARD_DISABLED", False)
    monkeypatch.setattr(E, "_last_mode_id", None)
    yield


def existing(env, policy="redistribute", quiet=15, review=False):
    """Полный режим, граница `existing.since` на сто секунд раньше «сейчас»: сделка, созданная раньше, существующая, позже новая."""
    set_full(env, existing={"policy": policy, "since": env.t - 100, "manager_quiet_min": quiet}, ai_user=AI_USER, review={"enabled": review})


def old_deal(env, **kw):
    return put_lead(env, created_at=int(env.t) - HOUR, responsible=kw.pop("responsible", OLD_MANAGER), **kw)


def new_deal(env, **kw):
    return put_lead(env, created_at=int(env.t) - 10, responsible=kw.pop("responsible", OLD_MANAGER), **kw)


def task(responsible=OLD_MANAGER, task_id=1, **kw):
    return {"id": task_id, "responsible_user_id": responsible, "text": "Связаться с клиентом", "deal_id": DEAL, **kw}


def intake(task_id=50):
    return {"id": task_id, "text": "Новый заказ с сайта", "task_type_id": 4232862, "created_by": 0, "responsible_user_id": OLD_MANAGER, "deal_id": DEAL}


def spy(env):
    """Считает живые запросы открытых задач (поверх подставного мира)."""
    calls: list[int] = []
    real = T._get_open

    async def counting(deal_id):
        calls.append(int(deal_id))
        return await real(deal_id)

    env.mp.setattr(T, "_get_open", counting)
    return calls


def resolves(env):
    calls: list[str] = []
    real = dedup.resolve

    async def counting(chat_id, phone, **kw):
        calls.append(chat_id)
        return await real(chat_id, phone, **kw)

    env.mp.setattr(dedup, "resolve", counting)
    return calls


def blank(env):
    return env.notes == [] and env.alerts == [] and env.turn_requests == []


# =============================================================================================
# правило «на сделке открыта задача сотрудника»
# =============================================================================================

def test_an_existing_deal_with_an_employee_task_is_not_taken_by_the_clients_message(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_open.append(task())
        calls = spy(env)
        await client_writes(env)
        s = store.get_session(CHAT_KEY)
        assert store.get_owner(DEAL) is None and blank(env) and len(env.tasks_open) == 1 and calls == [DEAL]     # один живой запрос, ничего не ушло
        assert s["state"] == "skip" and abs(s["skip_until"] - (env.t + cfg.TASK_RETRY_S)) < 5 and cfg.TASK_RETRY_S == 3600    # пересмотр не раньше чем через час
        assert len(store.pending_inbox(CHAT_KEY)) == 1                                                             # отказ временный: само сообщение ждёт пересмотра
        assert any("на сделке открыта задача сотрудника" in r.getMessage() for r in caplog.records)
    run(go())


def test_a_task_of_any_employee_blocks_and_the_ai_user_does_not(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_open.extend([task(AI_USER["id"], 1), task(None, 2)])                                             # задача без ответственного чужая
        await client_writes(env)
        assert store.get_owner(DEAL) is None
        env.tasks_open.clear()
        env.tasks_open.append(task(AI_USER["id"], 3))                                                              # только на пользователе ИИ: не мешает
        env.t += cfg.TASK_RETRY_S + 1
        await client_writes(env, "Есть новости?", mid="m-2")
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
    run(go())


def test_a_new_deal_is_not_blocked_and_the_intake_task_is_closed_by_the_ai(env):
    async def go():
        Amo(env)
        existing(env)
        new_deal(env)                                                                                              # создана после границы: новый лид
        env.tasks_open.extend([intake(), task(OLD_MANAGER, 2)])                                                    # задача приёма автоматики и задача менеджера
        calls = spy(env)
        await client_writes(env)
        await drain()
        assert store.get_owner(DEAL)["owner"] == "ai" and len(env.turn_requests) == 1
        assert env.tasks_open[0].get("is_completed") is True and not env.tasks_open[1].get("is_completed")      # приём закрыл `release_intake`, остальное не тронуто
        assert calls == [DEAL]                                                                                     # единственный запрос: `release_intake`, правило до него не дошло
    run(go())


def test_without_an_existing_block_the_rule_never_applies(env):
    async def go():
        Amo(env)
        set_full(env)                                                                                              # блока `existing` нет: «существующих» сделок нет
        old_deal(env)
        env.tasks_open.append(task())
        calls = spy(env)
        await client_writes(env)
        await drain()
        assert store.get_owner(DEAL)["owner"] == "ai" and calls == [DEAL]                                          # `release_intake`, не правило
    run(go())


def test_leave_refuses_an_existing_deal_before_the_task_check_is_ever_reached(env):
    async def go():
        Amo(env)
        existing(env, policy="leave")
        old_deal(env)
        calls = spy(env)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and blank(env) and calls == []                                        # отказ `leave` как раньше, задач не просим
    run(go())


def test_amo_silence_about_tasks_is_not_read_as_no_tasks(env, caplog):
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_down = True
        await client_writes(env)
        s = store.get_session(CHAT_KEY)
        assert store.get_owner(DEAL) is None and blank(env)
        assert s["state"] == "skip" and abs(s["skip_until"] - (env.t + cfg.TASK_UNCHECKED_RETRY_S)) < 5 and 60 <= cfg.TASK_UNCHECKED_RETRY_S <= 300
        assert any("задачи не проверить" in r.getMessage() for r in caplog.records)
        env.tasks_down = False
        env.t += cfg.TASK_UNCHECKED_RETRY_S + 1
        await client_writes(env, "Алло?", mid="m-2")                                                               # amo ожила, задач нет: берём
        assert store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_a_crash_of_the_task_check_is_a_refusal_not_a_take(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)

        async def boom(deal_id):
            raise RuntimeError("сбой")

        env.mp.setattr(T, "_get_open", boom)
        await client_writes(env)
        assert store.get_owner(DEAL) is None and blank(env) and store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_the_kill_switch_turns_off_only_this_rule_and_the_slice_shows_it(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_open.append(task())
        assert E.status()["task_guard"] == {"enabled": True, "retry_s": 3600}
        env.mp.setattr(cfg, "TASK_GUARD_DISABLED", True)
        assert E.status()["task_guard"] == {"enabled": False, "retry_s": 3600}
        calls = spy(env)
        await client_writes(env)
        await drain()
        assert store.get_owner(DEAL)["owner"] == "ai" and calls == [DEAL]                                          # задача менеджера не помешала; запрос только от `release_intake`
    run(go())


@pytest.mark.parametrize("value, expected", [(None, False), ("", False), ("0", False), ("true", False), ("1", True), (" 1 ", True)])
def test_the_kill_switch_is_on_only_when_it_equals_one(value, expected):
    import os
    import subprocess
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    env_vars = {k: v for k, v in os.environ.items() if k != "AI_TASK_GUARD_DISABLED"}
    if value is not None:
        env_vars["AI_TASK_GUARD_DISABLED"] = value
    out = subprocess.run([sys.executable, "-X", "utf8", "-c", "import ai_engine_config as c; print(c.TASK_GUARD_DISABLED)"],
                         cwd=here, env=env_vars, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.strip().splitlines()[-1] == str(expected)


def test_a_deal_the_ai_already_leads_is_not_checked_again(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        await client_writes(env)                                                                                   # задач нет: ИИ берёт сделку
        first = env.turn_requests[-1]
        await E.handle_result(first["turn_id"], DEAL, first["epoch"], "continue", None, None)
        env.tasks_open.append(task())                                                                              # потом менеджеру поставили задачу
        env.turn_requests.clear()
        calls = spy(env)
        await env.say(env.msg("А ещё вопрос", mid="m-2"))
        await env.advance(30)
        assert env.turn_requests and env.turn_requests[-1]["reason"] == "client_message" and calls == []           # сделку ведёт ИИ: ни запроса, ни отказа
    run(go())


def test_the_give_button_ignores_the_rule(env):
    async def go():
        existing(env)
        old_deal(env)
        env.tasks_open.append(task())
        code, out = await give()
        assert code == 200 and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_the_intercept_decision_never_asks_for_tasks(env):
    async def go():
        existing(env)
        lead = old_deal(env)
        calls = spy(env)
        decision = await F.decide_take(lead, mode_id="full", intercept=True)
        assert decision.allow and decision.existing is False and calls == []                                       # перехват: правила `existing` и задач нет
    run(go())


def test_an_existing_deal_with_a_task_does_not_make_a_proposal_either(env):
    async def go():
        Amo(env)
        existing(env, review=True)                                                                                 # пилот: вместо смены ответственного было бы предложение
        old_deal(env)
        env.tasks_open.append(task())
        await client_writes(env)
        import ai_engine_approval as A
        assert A.journal() == [] and store.get_owner(DEAL) is None and blank(env)
    run(go())


def test_the_sweep_asks_for_tasks_once_and_the_gate_does_not_repeat_it(env):
    """Подбор проверяет задачи сам и зовёт `_gate_take(tasks_checked=True)`: второго запроса на сделку нет."""
    async def go():
        Amo(env)
        existing(env)
        lead = old_deal(env)
        calls = spy(env)
        s = {"chat_key": CHAT_KEY, "chat_id": CHAT, "phone": PHONE, "chat_type": "telegram", "channel_id": CHAN, "contact_name": ""}
        env.tasks_open.append(task())
        res = await E._gate_take(s, dedup.Resolution(dedup.K_AI, deal=lead), "full", tasks_checked=True)
        assert res.kind == dedup.K_AI and calls == []                                                              # флаг: запроса нет
        res = await E._gate_take(s, dedup.Resolution(dedup.K_AI, deal=lead), "full")
        assert res.kind == dedup.K_REFUSED and calls == [DEAL]
    run(go())


# =============================================================================================
# сообщение клиента в окне пропуска
# =============================================================================================

def ids(chat_key=CHAT_KEY):
    return [r["message_id"] for r in store.pending_inbox(chat_key)]


async def window(env, mid="m-1"):
    """Существующая сделка с задачей менеджера: первое сообщение отказано (отказ временный), открыто окно в час. Возвращает конец окна."""
    Amo(env)
    existing(env)
    old_deal(env)
    env.tasks_open.append(task())
    await client_writes(env, mid=mid)
    s = store.get_session(CHAT_KEY)
    assert s["state"] == "skip" and ids() == [mid]                                                                 # временный отказ: само сообщение тоже ждёт пересмотра
    return s["skip_until"]


def test_a_message_in_the_window_is_kept_and_answered_when_the_window_ends(env):
    async def go():
        end = await window(env)
        calls = resolves(env)
        env.t += 600
        await env.say(env.msg("Есть новости?", mid="m-2"))
        assert ids() == ["m-1", "m-2"]                                                                             # раньше оба пропали бы
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "skip" and s["skip_until"] == end and s["next_at"] is None and s["note"] == ""
        await env.advance(120)
        assert calls == [] and env.turn_requests == []                                                             # до конца окна разбора нет
        env.tasks_open.clear()                                                                                     # менеджер закрыл задачу
        await env.advance(end - env.t + 1)
        await env.advance(15)  # запас на медленном диске: метки времени ходов ставит настоящие часы, ход уходит в следующем проходе
        assert calls == [CHAT] and store.get_owner(DEAL)["owner"] == "ai"
        turn = env.turn_requests[-1]
        assert turn["reason"] == "first_message" and [m["message_id"] for m in turn["messages"]] == ["m-1", "m-2"] and turn["up_to_message_id"] == "m-2"
        assert store.get_session(CHAT_KEY)["state"] in ("in_turn", "ai") and store.get_session(CHAT_KEY)["note"] == ""     # пометка пересмотра снята
    run(go())


def test_the_message_that_caused_a_temporary_refusal_is_reviewed_once_even_when_nobody_wrote_in_the_window(env):
    async def go():
        end = await window(env)
        calls = resolves(env)
        env.tasks_open.clear()
        await env.advance(end - env.t + 1)
        await env.advance(15)
        assert calls == [CHAT] and store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1"]
    run(go())


def test_a_task_that_hangs_for_days_costs_one_review_and_then_silence_until_a_new_message(env):
    """Задача висит, клиент пишет раз: через час ровно один пересмотр (и один запрос задач), дальше тишина, пока клиент не напишет снова."""
    async def go():
        end = await window(env)
        calls = resolves(env)
        tasks_calls = spy(env)
        await env.advance(end - env.t + 1)                                                                         # пересмотр: задача на месте
        assert calls == [CHAT] and tasks_calls == [DEAL]
        s = store.get_session(CHAT_KEY)
        assert ids() == [] and s["state"] == "skip" and s["note"] == "" and abs(s["skip_until"] - (env.t + cfg.TASK_RETRY_S)) < 5
        assert store.get_owner(DEAL) is None and blank(env)
        for _ in range(30):                                                                                        # дни тишины: ни разборов, ни запросов в amo
            await env.advance(3600)
        assert calls == [CHAT] and tasks_calls == [DEAL] and store.due_sessions(env.t + 10 ** 6) == []
        await env.say(env.msg("Есть новости?", mid="m-2"))                                                         # новое сообщение: новая сессия, пометки пересмотра нет
        assert store.get_session(CHAT_KEY)["state"] == "collecting" and ids() == ["m-2"] and store.get_session(CHAT_KEY)["note"] == ""
    run(go())


def test_a_message_after_a_finished_review_starts_its_own_single_review(env):
    async def go():
        end = await window(env)
        calls = resolves(env)
        tasks_calls = spy(env)
        await env.advance(end - env.t + 1)                                                                         # первый пересмотр: отказ, всё сброшено, окно снова час
        end2 = store.get_session(CHAT_KEY)["skip_until"]
        await env.say(env.msg("Ещё раз", mid="m-3"))                                                               # окно ещё идёт: сообщение сохраняется
        assert ids() == ["m-3"] and store.get_session(CHAT_KEY)["state"] == "skip"
        await env.advance(end2 - env.t + 1)                                                                        # второе окно: второй пересмотр, уже для m-3
        assert calls == [CHAT, CHAT] and tasks_calls == [DEAL, DEAL] and ids() == [] and store.get_owner(DEAL) is None
        for _ in range(10):
            await env.advance(3600)
        assert calls == [CHAT, CHAT]                                                                               # снова тишина
    run(go())


def test_a_second_refusal_drops_all_the_messages_and_opens_one_new_window_without_a_loop(env):
    async def go():
        end = await window(env)
        calls = resolves(env)
        await env.say(env.msg("Есть новости?", mid="m-2"))
        await env.say(env.msg("Алло", mid="m-3"))
        assert ids() == ["m-1", "m-2", "m-3"]
        await env.advance(end - env.t + 1)                                                                         # задача всё ещё открыта
        assert calls == [CHAT]                                                                                     # один пересмотр на окно
        s = store.get_session(CHAT_KEY)
        assert ids() == [] and s["state"] == "skip" and abs(s["skip_until"] - (env.t + cfg.TASK_RETRY_S)) < 5
        assert store.get_owner(DEAL) is None and blank(env)
        for _ in range(5):
            await env.advance(30)
        assert calls == [CHAT]                                                                                     # новых разборов нет, пока нет новых сообщений
        await env.say(env.msg("Ещё раз", mid="m-4"))                                                               # новое окно: сообщение снова сохраняется
        assert ids() == ["m-4"]
    run(go())


def test_a_repeated_webhook_of_the_same_message_is_kept_once(env):
    async def go():
        await window(env)
        for _ in range(3):
            await env.say(env.msg("Есть новости?", mid="m-2"))
        await env.say(env.msg("Здравствуйте", mid="m-1"))                                                          # и повтор самого отказанного сообщения
        assert ids() == ["m-1", "m-2"]
    run(go())


def test_the_window_review_re_checks_the_manager_quiet_time(env):
    async def go():
        Amo(env)
        existing(env, quiet=15)
        old_deal(env)
        last = env.t - 300
        F.note_manager_out(CHAT_KEY, last)                                                                         # менеджер писал клиенту пять минут назад
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        assert store.get_owner(DEAL) is None and s["state"] == "skip" and abs(s["skip_until"] - (last + 15 * 60)) < 5   # окно до конца тишины менеджера
        assert ids() == ["m-1"]                                                                                    # менеджер активен: отказ временный, сообщение ждёт
        end = s["skip_until"]
        env.t += 100
        await env.say(env.msg("Здравствуйте, вы тут?", mid="m-2"))
        await env.advance(end - env.t + 1)                                                                         # менеджер больше не писал: тишина прошла
        await env.advance(15)  # запас на медленном диске: метки времени ходов ставит настоящие часы, ход уходит в следующем проходе
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1", "m-2"]
    run(go())


def test_the_manager_writing_in_the_window_postpones_the_deal_again(env):
    async def go():
        Amo(env)
        existing(env, quiet=15)
        old_deal(env)
        F.note_manager_out(CHAT_KEY, env.t - 300)
        await client_writes(env, mid="m-1")
        end = store.get_session(CHAT_KEY)["skip_until"]
        await env.say(env.msg("Вы тут?", mid="m-2"))
        F.note_manager_out(CHAT_KEY, end - 10)                                                                     # менеджер ответил в самом конце окна
        await env.advance(end - env.t + 1)
        assert store.get_owner(DEAL) is None and ids() == [] and env.turn_requests == []                           # повторный отказ на пересмотре: сброс
        assert store.get_session(CHAT_KEY)["state"] == "skip" and store.get_session(CHAT_KEY)["skip_until"] > end
    run(go())


def test_an_attachment_in_the_window_is_kept_with_its_caption(env):
    async def go():
        end = await window(env)
        await env.say(env.msg("Вот скрин оплаты", mid="img-1", typ="image"))
        rows = store.pending_inbox(CHAT_KEY)
        assert [(r["message_id"], r["msg_type"], r["text"]) for r in rows][1:] == [("img-1", "image", "Вот скрин оплаты")]
        assert store.get_session(CHAT_KEY)["has_media"] == 0                                                       # картинка не «медиа»: ИИ получает строку-пометку
        env.tasks_open.clear()
        await env.advance(end - env.t + 1)
        await env.advance(15)  # запас на медленном диске: метки времени ходов ставит настоящие часы, ход уходит в следующем проходе
        texts = [m["text"] for m in env.turn_requests[-1]["messages"]]
        assert len(texts) == 2 and "картинку" in texts[1] and "Вот скрин оплаты" in texts[1]
    run(go())


def test_a_geo_message_in_the_window_marks_the_chat_and_goes_to_a_human_like_any_first_message(env):
    async def go():
        end = await window(env)
        await env.say(env.msg("", mid="geo-1", typ="geo"))
        assert store.get_session(CHAT_KEY)["has_media"] == 1 and ids() == ["m-1", "geo-1"]
        env.tasks_open.clear()
        await env.advance(end - env.t + 1)
        assert store.get_owner(DEAL)["owner"] == "human" and env.turn_requests == [] and env.alerts                  # как обычное первое сообщение с геометкой
    run(go())


def test_a_message_right_after_the_window_keeps_the_media_flag_of_a_saved_one(env):
    """Окно кончилось, а таймер ещё не вернул чат в разбор: новое сообщение открывает новую сессию, и сохранённая в окне геометка не должна потерять пометку «медиа»."""
    async def go():
        end = await window(env)
        await env.say(env.msg("", mid="geo-1", typ="geo"))
        env.tasks_open.clear()
        env.t = end + 0.5
        await env.say(env.msg("Здравствуйте?", mid="m-3"))
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "collecting" and s["has_media"] == 1 and ids() == ["m-1", "geo-1", "m-3"] and s["note"] == "review"   # этот разбор и есть единственный пересмотр
        await env.advance(61)
        assert store.get_owner(DEAL)["owner"] == "human" and env.turn_requests == [] and env.alerts
    run(go())


def test_the_cheap_checks_of_a_new_session_apply_in_the_window(env):
    async def go():
        await window(env)
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: True)                                                 # чат ведёт авто-режим
        await env.say(env.msg("Привет", mid="m-2"))
        assert ids() == ["m-1"]
        env.mp.setattr(E, "_autopilot_busy", lambda *a, **k: False)
        env.mp.setattr(settings, "phone_may_match", lambda *a, **k: False)                                         # контакт вне списка режима
        await env.say(env.msg("Привет ещё раз", mid="m-3"))
        assert ids() == ["m-1"]
    run(go())


def test_the_give_button_after_a_window_does_not_lose_the_saved_messages(env):
    async def go():
        await window(env)
        await env.say(env.msg("Есть новости?", mid="m-2"))
        code, out = await give()                                                                                   # человек нажал «Дать сделку ИИ» в окне: правило задач не действует
        assert code == 200 and store.get_owner(DEAL)["owner"] == "ai"
        await env.advance(30)
        assert len(env.turn_requests) == 1
        turn = env.turn_requests[-1]
        assert turn["event"]["type"] == "deal_given" and [m["message_id"] for m in turn["messages"]] == ["m-1", "m-2"]      # сообщения идут в ход передачи: не теряются и не дублируются
        assert store.pending_inbox(CHAT_KEY) == [] or all(r["consumed_turn"] for r in store.pending_inbox(CHAT_KEY))
    run(go())


def test_an_awaiting_chat_and_a_human_chat_are_not_changed(env):
    async def go():
        _, _, row = await make_proposal(env)                                                                       # предложение пилота ждёт человека
        assert store.get_session(CHAT_KEY)["state"] == "awaiting"
        await env.say(env.msg("Алло?", mid="m-2"))
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "awaiting" and s["next_at"] is None and [r["text"] for r in store.pending_inbox(CHAT_KEY)] == ["Здравствуйте", "Алло?"]
        assert store.due_sessions(env.t + 10 * HOUR) == []
        store.put_session(CHAT_KEY, state=store.S_HUMAN, skip_until=None)
        store.discard_pending(CHAT_KEY)
        await env.say(env.msg("И ещё", mid="m-3"))
        assert store.get_session(CHAT_KEY)["state"] == "human" and store.pending_inbox(CHAT_KEY) == [] and row is not None   # `human` по-прежнему игнорирует
    run(go())


def test_a_restart_in_the_window_loses_and_duplicates_nothing(env):
    async def go():
        end = await window(env)
        await env.say(env.msg("Есть новости?", mid="m-2"))
        store.init()                                                                                               # контейнер перезапустили: база та же, память пуста
        env.mp.setattr(E, "_lock", None)
        assert ids() == ["m-1", "m-2"]
        assert store.due_sessions(end - 1) == [] and [s["chat_key"] for s in store.due_sessions(end + 1)] == [CHAT_KEY]
        await env.say(env.msg("Есть новости?", mid="m-2"))                                                         # вебхук после рестарта пришёл повторно
        assert len(store.pending_inbox(CHAT_KEY)) == 2
        env.tasks_open.clear()
        await env.advance(end - env.t + 1)
        await env.advance(15)  # запас на медленном диске: метки времени ходов ставит настоящие часы, ход уходит в следующем проходе
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1", "m-2"] and len(env.turn_requests) == 1
    run(go())


def test_the_review_waits_for_a_client_who_is_still_writing(env):
    async def go():
        end = await window(env)
        env.tasks_open.clear()
        env.t = end - 3
        await env.say(env.msg("Есть новости?", mid="m-2"))
        calls = resolves(env)
        await env.advance(4)                                                                                       # окно кончилось, клиент написал три секунды назад
        assert calls == [] and store.get_session(CHAT_KEY)["state"] == "collecting" and env.turn_requests == []
        await env.advance(10)
        assert calls == [CHAT] and store.get_owner(DEAL)["owner"] == "ai"
    run(go())


def test_due_sessions_returns_skipped_chats_only_with_messages_and_a_finished_window(env):
    store.put_session(CHAT_KEY, channel_id=CHAN, chat_id=CHAT, state=store.S_SKIP, skip_until=env.t + 100)
    assert store.due_sessions(env.t + 200) == []                                                                   # окно кончилось, но входящих нет: будить нечего
    store.add_inbox("x-1", CHAT_KEY, "слово", "text", "2026-11-24T10:00:00Z", env.t)
    assert store.due_sessions(env.t + 50) == []                                                                    # входящие есть, окно идёт
    assert [s["chat_key"] for s in store.due_sessions(env.t + 200)] == [CHAT_KEY]
    store.discard_pending(CHAT_KEY)
    assert store.due_sessions(env.t + 200) == []


# =============================================================================================
# какие отказы временные (сообщение сохраняется), а какие постоянные (сбрасывается, как раньше)
# =============================================================================================

def test_a_permanent_refusal_drops_the_trigger_message_but_keeps_the_window_ones_for_one_review(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env, price=5000)
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, budget={"skip_from_rub": 1000})
        await client_writes(env, mid="m-1")
        end = store.get_session(CHAT_KEY)["skip_until"]
        assert store.get_owner(DEAL) is None and abs(end - (env.t + cfg.SKIP_TTL_S)) < 5 and ids() == []          # бюджет: окно десять минут, само сообщение сброшено
        assert store.due_sessions(env.t + 10 ** 6) == []                                                           # без сообщений в окне пересмотра нет
        await env.say(env.msg("Подскажите", mid="m-2"))
        calls = resolves(env)
        await env.advance(end - env.t + 1)
        assert calls == [CHAT] and store.get_owner(DEAL) is None and ids() == []                                  # проверки те же, отказ тот же, один пересмотр
    run(go())


def test_filters_and_the_reassign_flag_are_permanent(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env, status="Не оплачен")
        set_full(env, existing={"policy": "redistribute", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER, filters={"payment_status": ["paid"]})
        await client_writes(env, mid="m-1")
        assert store.get_owner(DEAL) is None and ids() == [] and store.get_session(CHAT_KEY)["state"] == "skip"   # фильтры
        env.t += cfg.SKIP_TTL_S + 1
        existing(env)
        env.mp.setattr(F, "REASSIGN_DISABLED", True)                                                               # аварийный флаг смены ответственного
        await client_writes(env, "Ещё раз", mid="m-2")
        assert store.get_owner(DEAL) is None and ids() == []
    run(go())


def test_leave_for_an_existing_deal_is_permanent(env):
    async def go():
        Amo(env)
        existing(env, policy="leave")
        old_deal(env)
        await client_writes(env, mid="m-1")
        assert store.get_owner(DEAL) is None and ids() == [] and store.due_sessions(env.t + 10 ** 6) == []
    run(go())


def test_tasks_unchecked_keeps_the_message_and_the_short_window_is_reviewed_once(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_down = True
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        assert ids() == ["m-1"] and abs(s["skip_until"] - (env.t + cfg.TASK_UNCHECKED_RETRY_S)) < 5               # окно 120 с, сообщение сохранено
        env.tasks_down = False                                                                                     # amo ожила, задач нет
        await env.advance(s["skip_until"] - env.t + 1)
        await env.advance(15)
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1"]
    run(go())


def test_the_ai_service_being_down_at_entry_keeps_the_message_and_calls_people_once(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.tasks_open.clear()
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        assert store.get_owner(DEAL) is None and ids() == ["m-1"] and abs(s["skip_until"] - (env.t + 300)) < 5
        alerts_before = len(env.alerts)
        assert alerts_before == 1
        env.health_ok = True
        await env.advance(s["skip_until"] - env.t + 1)
        await env.advance(15)
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1"]
    run(go())


def test_the_ai_service_still_down_on_the_review_drops_the_message_and_does_not_call_people_again(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        env.health_ok = False
        E._ai_ok_at = env.t - 1000
        await client_writes(env, mid="m-1")
        end = store.get_session(CHAT_KEY)["skip_until"]
        assert len(env.alerts) == 1 and ids() == ["m-1"]
        env.t = max(env.t, end) + 700                                                                              # дедупликация алерта (10 минут) за это время истекла бы
        E._alert_seen.clear()
        E._ai_ok_at = env.t - 1000
        await env.advance(1)
        assert len(env.alerts) == 1 and ids() == [] and store.get_owner(DEAL) is None                              # пересмотр: сервис всё ещё лежит, людей второй раз не зовём, всё сброшено
        assert store.get_session(CHAT_KEY)["state"] == "skip" and store.get_session(CHAT_KEY)["note"] == ""
    run(go())


def test_the_autopilot_busy_with_the_chat_keeps_the_message(env):
    async def go():
        Amo(env)
        existing(env)
        old_deal(env)
        busy = {"on": True}
        env.mp.setattr(E, "_autopilot_busy", lambda chat_id, phone, deal_id=None: busy["on"] and deal_id is not None)     # сделку ведёт авто-режим (заказ с сайта): видно, только когда сделка найдена
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        assert store.get_owner(DEAL) is None and ids() == ["m-1"] and s["state"] == "skip"
        busy["on"] = False
        await env.advance(s["skip_until"] - env.t + 1)
        await env.advance(15)
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1"]
    run(go())


def test_a_failed_change_of_the_responsible_keeps_the_message_and_is_reviewed_once(env):
    async def go():
        amo = Amo(env, ok=False)                                                                                   # amo не приняла смену ответственного
        existing(env)
        old_deal(env)
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        first = len(amo.patches)
        assert store.get_owner(DEAL) is None and ids() == ["m-1"] and s["state"] == "skip" and first >= 1
        await env.advance(s["skip_until"] - env.t + 1)                                                             # пересмотр: снова не принято, всё сброшено
        assert len(amo.patches) == 2 * first and ids() == [] and store.get_owner(DEAL) is None
        for _ in range(5):
            await env.advance(600)
        assert len(amo.patches) == 2 * first                                                                       # без новых сообщений третьей попытки нет
    run(go())


def test_a_deal_that_never_appears_keeps_the_message_for_the_review(env):
    async def go():
        env.set_settings()
        await env.say(env.msg("Здравствуйте", mid="m-1"))
        for _ in range(12):                                                                                        # поиск сделки идёт десять минут с повтором раз в минуту
            await env.advance(61)
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "skip" and ids() == ["m-1"] and store.get_owner(DEAL) is None                         # не нашли: отказ временный, сообщение сохранено
        env.lead()                                                                                                 # сделка появилась
        await env.advance(s["skip_until"] - env.t + 1)
        await env.advance(80)
        assert store.get_owner(DEAL)["owner"] == "ai" and [m["message_id"] for m in env.turn_requests[-1]["messages"]] == ["m-1"]
    run(go())


def test_an_open_proposal_without_an_awaiting_chat_keeps_the_message_and_awaiting_is_untouched(env):
    async def go():
        Amo(env)
        existing(env, review=True)                                                                                 # пилот
        old_deal(env)
        import ai_engine_approval as A
        row = A.create(deal_id=DEAL, from_id=OLD_MANAGER, from_name="Иванова Мария", to_id=AI_USER["id"], to_name="Гладков Александер",
                       chat={"chat_key": CHAT_KEY, "chat_id": CHAT, "chat_type": "telegram", "channel_id": CHAN}, now=env.t, source="sweep")
        assert row is not None
        await client_writes(env, mid="m-1")
        s = store.get_session(CHAT_KEY)
        assert s["state"] == "skip" and ids() == ["m-1"] and store.get_owner(DEAL) is None and blank(env)          # предложение открыто, чат не `awaiting`: временный отказ
        await env.advance(s["skip_until"] - env.t + 1)                                                             # пересмотр: предложение всё ещё открыто, всё сброшено
        assert ids() == [] and store.get_session(CHAT_KEY)["state"] == "skip"
    run(go())


def test_an_explicitly_allowed_contact_is_exempt_from_the_task_rule(env):
    """Явно разрешённый контакт (пилотный список при `restricted`) правилом задач не останавливается (решение Кати 24.11.2026); контакты без явного разрешения блокируются (первый тест файла)."""
    async def go():
        Amo(env)
        set_full(env, contacts=PILOT_LIST, existing={"policy": "leave", "since": env.t - 100, "manager_quiet_min": 15}, ai_user=AI_USER)
        old_deal(env)
        env.tasks_open.append(task())
        calls = spy(env)
        await client_writes(env, mid="m-1")
        await drain()
        assert store.get_owner(DEAL)["owner"] == "ai" and calls == [DEAL]                                          # взята вопреки задаче менеджера; единственный запрос: `release_intake`
        assert not env.tasks_open[0].get("is_completed")                                                           # чужая задача не тронута
    run(go())
