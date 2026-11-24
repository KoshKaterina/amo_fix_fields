"""«Стоп»/«старт» примечанием и закреплённая заметка «ИИ ведёт сделку» (`ai_engine_pause`, решение Кати 24.11.2026).

Проверяем то, ошибка в чём означает сообщение клиенту от ИИ после «стоп», молчащего ИИ после «старт» или неправду в карточке:

  • заметка закреплена ДО первого хода (до генерации ответа), второй раз не пишется, уходит с открепом, когда сделка ушла от ИИ;
  • «стоп»: ход снят, владелец человек, новые сообщения клиента ИИ не берёт, закреплена заметка паузы;
  • «старт»: ИИ снова ведёт, ход-событие `dialog_returned` с новой отсечкой `manager_cutoff`, заметка паузы откреплена;
  • повтор опроса и повтор команды не исполняются дважды, старые примечания при первом запуске не исполняются;
  • команда только от человека и только целым словом; «старт» при выключенном ИИ и при недоступном сервисе.

Сети нет: amo подставной (примечания, закрепление, список примечаний), опрос зовётся руками (`run_once`), время ручное.
"""
import asyncio
import datetime

import pytest

import ai_engine as E
import ai_engine_config as cfg
import ai_engine_pause as P
import ai_engine_store as store
from test_ai_engine import CHAT_KEY, DEAL, env, run  # noqa: F401 - фикстура env

MANAGER = 7001


class Amo:
    """Подставной amo модуля паузы: примечания модуля, закреплённые, лента примечаний людей для опроса."""

    def __init__(self, env, mp):
        self.env = env
        self.notes: list[dict] = []          # примечания, записанные модулем: {"id", "deal", "text", "turns_before"}
        self.pinned: set[int] = set()
        self.feed: list[dict] = []           # примечания amo, которые видит опрос (команды людей и прочее)
        self.down = False
        self.next_id = 500000

        async def add_note(deal_id, text):
            if self.down:
                return None
            self.next_id += 1
            self.notes.append({"id": self.next_id, "deal": int(deal_id), "text": text, "turns_before": len(env.turn_requests)})
            return self.next_id

        async def pin(note_id, on):
            if self.down:
                return False
            (self.pinned.add if on else self.pinned.discard)(int(note_id))
            return True

        async def since(ts):
            if self.down:
                return None
            return sorted((n for n in self.feed if n["updated_at"] >= ts), key=lambda n: n["updated_at"])

        async def user_name(uid):
            return {MANAGER: "Иванов Иван"}.get(int(uid), f"сотрудник {uid}")

        async def no_tick(eng, now):
            return None

        mp.setattr(P, "_amo_add_note", add_note)
        mp.setattr(P, "_amo_pin", pin)
        mp.setattr(P, "_amo_notes_since", since)
        mp.setattr(P, "_user_name", user_name)
        mp.setattr(P, "tick", no_tick)        # опрос гоняем руками: фоновая задача сделала бы порядок событий случайным

    def command(self, text, deal=DEAL, by=MANAGER, at=None):
        at = int(self.env.t if at is None else at)
        self.next_id += 1
        self.feed.append({"id": self.next_id, "entity_id": deal, "note_type": "common", "created_by": by, "created_at": at, "updated_at": at,
                          "params": {"text": text}})
        return self.next_id

    def pinned_texts(self, deal=DEAL):
        return [n["text"] for n in self.notes if n["id"] in self.pinned and n["deal"] == deal]

    def texts(self, deal=DEAL):
        return [n["text"] for n in self.notes if n["deal"] == deal]


@pytest.fixture
def amo(env, monkeypatch):
    monkeypatch.setattr(cfg, "PAUSE_NOTES_ENABLED", True)
    return Amo(env, monkeypatch)


async def poll(env):
    await P.run_once(E)
    await asyncio.sleep(0.02)                 # отмена хода у сервиса уходит фоновой задачей


def _iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================================
# разбор команды
# =============================================================================================

@pytest.mark.parametrize("text,cmd", [
    ("стоп", "stop"), ("Стоп", "stop"), ("СТОП!", "stop"), ("стоп.", "stop"), ("«стоп»", "stop"), ("  стоп ии ", "stop"), ("ИИ, стоп", "stop"),
    ("stop", "stop"), ("старт", "start"), ("Старт ИИ", "start"), ("start", "start"),
    ("стоп-кран", None), ("стоп, подожди меня", None), ("клиент просил стоп", None), ("стопка", None), ("", None), (None, None),
])
def test_parse_command(text, cmd):
    assert P.parse_command(text) == cmd


# =============================================================================================
# заметка «ИИ ведёт сделку»
# =============================================================================================

def test_pin_note_goes_before_first_turn_and_only_once(env, amo):
    async def go():
        turn = await env.start_turn()
        assert amo.pinned_texts() == ["ИИ-менеджер: " + P.TEXT_AI]
        assert amo.notes[0]["turns_before"] == 0                     # заметка записана до того, как ход ушёл сервису
        await E.handle_result(turn["turn_id"], DEAL, turn["epoch"], "continue", None, None)
        await env.say(env.msg("ещё вопрос"))
        await env.advance(30)
        assert len(env.turn_requests) == 2
        assert len(amo.notes) == 1                                  # второй ход заметку не пишет
    run(go())


def test_pin_is_unpinned_when_manager_takes_the_deal(env, amo):
    async def go():
        await env.start_turn()
        assert amo.pinned_texts()
        await env.say(env.msg("Беру на себя", echo=True, author="Иванов Иван"))
        assert store.get_owner(DEAL)["owner"] == "human"
        await poll(env)
        assert amo.pinned_texts() == []
        assert P.pin_get(DEAL, P.KIND_AI) is None
    run(go())


def test_amo_down_does_not_hold_the_turn_and_reconcile_pins_later(env, amo):
    async def go():
        amo.down = True
        await env.start_turn()                                      # ход ушёл без заметки
        assert amo.notes == []
        amo.down = False
        await poll(env)
        assert amo.notes == []                                      # пауза после сбоя: amo не дёргаем на каждом проходе
        env.t += P.PIN_FAIL_PAUSE_S + 1
        await poll(env)
        assert amo.pinned_texts() == ["ИИ-менеджер: " + P.TEXT_AI]
    run(go())


def test_reply_instead_of_manager_gets_no_pin(env, amo):
    async def go():
        await P.ensure_ai_pin(E, DEAL, {"deal_id": DEAL, "reason": "intercept_reply"})
        assert amo.notes == []
    run(go())


# =============================================================================================
# «стоп» и «старт»
# =============================================================================================

def test_stop_releases_the_deal_and_client_messages_wait_for_start(env, amo):
    async def go():
        await poll(env)                                             # первый запуск ставит курсор
        turn = await env.start_turn()
        env.t += 5
        amo.command("Стоп")
        await poll(env)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["reason"]) == ("human", "paused")
        assert env.cancels == [turn["turn_id"]]
        code, body = E.decide_permit(DEAL, owner["epoch"], turn["turn_id"], turn["up_to_message_id"])
        assert body["allowed"] is False
        assert len(amo.pinned_texts()) == 1 and "ИИ на паузе" in amo.pinned_texts()[0] and "Иванов Иван" in amo.pinned_texts()[0]
        assert P.pause_get(DEAL) is not None
        await env.say(env.msg("Алло, есть кто?"))
        await env.advance(120)
        assert len(env.turn_requests) == 1                          # клиент написал на паузе: ИИ молчит
    run(go())


def test_start_returns_the_deal_with_new_cutoff_and_answers(env, amo):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        env.t += 600
        await env.say(env.msg("Менеджер, я тут", echo=False))
        await env.say(env.msg("Сейчас подскажу", echo=True, author="Иванов Иван"))
        env.t += 60
        start_at = env.t
        amo.command("старт")
        await poll(env)
        owner = store.get_owner(DEAL)
        assert owner["owner"] == "ai"
        assert P.pause_get(DEAL) is None
        await env.advance(1)
        ev = env.turn_requests[-1]
        assert ev["reason"] == "event" and ev["event"]["type"] == "dialog_returned" and ev["epoch"] == owner["epoch"]
        assert ev["manager_cutoff"] == _iso(start_at)               # сообщения менеджера за паузу ИИ видит, но они его не останавливают
        pinned = amo.pinned_texts()
        assert len(pinned) == 1 and pinned[0].startswith("ИИ-менеджер: ИИ снова ведёт сделку") and "Иванов Иван" in pinned[0]
    run(go())


def test_commands_are_executed_once(env, amo):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        await poll(env)
        env.t += 5
        amo.command("стоп")                                         # второй «стоп» на паузе: заметку не дублирует
        await poll(env)
        assert sum("ИИ на паузе" in t for t in amo.texts()) == 1
        events = [r["event"] for r in P.journal()]
        assert events.count("stop") == 1 and events.count("stop_repeat") == 1
    run(go())


def test_first_run_ignores_old_notes_and_robots_and_long_texts(env, amo):
    async def go():
        await env.start_turn()
        amo.command("стоп", at=env.t - 1000)                        # до выката
        await poll(env)                                             # первый запуск: курсор на «сейчас»
        env.t += 5
        amo.command("стоп", by=0)                                   # примечание интеграции, не человека
        amo.command("Клиент сказал стоп, перезвоню завтра")
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "ai"
        assert P.pause_get(DEAL) is None
    run(go())


def test_stop_on_a_deal_ai_never_led_keeps_ai_out_until_start(env, amo):
    async def go():
        await poll(env)
        env.lead()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        owner = store.get_owner(DEAL)
        assert (owner["owner"], owner["reason"]) == ("human", "paused")
        await env.say(env.msg("Здравствуйте"))
        await env.advance(120)
        assert env.turn_requests == []                              # ИИ в сделку на паузе не входит
        env.t += 5
        amo.command("старт")
        await poll(env)
        assert any("Пауза снята" in t for t in amo.texts())
        await env.say(env.msg("Есть кто?"))
        await env.advance(120)
        assert env.turn_requests, "после «старт» ИИ входит по обычным правилам"
    run(go())


def test_start_when_ai_is_off_is_refused_with_a_note(env, amo):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        env.set_settings(mode="off", pipelines=[])
        env.t += 5
        amo.command("старт")
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "human"
        assert P.pause_get(DEAL) is None
        assert any("выключен" in t for t in amo.texts())
        assert amo.pinned_texts() == []
    run(go())


def test_start_retries_while_ai_service_is_down(env, amo, monkeypatch):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        up = {"ok": False}

        async def ai_is_up():
            return up["ok"]

        monkeypatch.setattr(E, "_ai_is_up", ai_is_up)
        env.t += 5
        amo.command("старт")
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "human" and P.pause_get(DEAL) is not None
        env.t += 300                                                 # курсор не ушёл за невыполненный «старт»
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "human"
        up["ok"] = True
        env.t += 15
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "ai" and P.pause_get(DEAL) is None
    run(go())


def test_start_gives_up_after_retry_window(env, amo, monkeypatch):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)

        async def ai_is_up():
            return False

        monkeypatch.setattr(E, "_ai_is_up", ai_is_up)
        env.t += 5
        amo.command("старт")
        await poll(env)
        env.t += P.START_RETRY_S + 1
        await poll(env)
        assert any("ИИ-сервис недоступен" in t for t in amo.texts())
        assert P.pause_get(DEAL) is None
    run(go())


def test_status_block(env, amo):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        block = E.status()
        b = P.status_block(env.t)
        assert b["enabled"] is True and b["paused"] == 1 and b["pinned"]["pause"] == 1 and b["pinned"]["ai"] == 0
        assert b["events_24h"].get("stop") == 1
        assert block["pause"]["paused"] == 1
    run(go())


def test_stop_after_return_from_the_panel_is_a_new_stop(env, amo):
    async def go():
        await poll(env)
        await env.start_turn()
        env.t += 5
        amo.command("стоп")
        await poll(env)
        code, body = await E.return_dialog(DEAL)                    # вернули ИИ кнопкой в панели, не «старт»
        assert code == 200
        await env.advance(1)                                        # ход-событие: перед ним снова заметка «ИИ ведёт сделку», пауза снята
        assert P.pause_get(DEAL) is None
        assert any(t == "ИИ-менеджер: " + P.TEXT_AI for t in amo.pinned_texts())
        env.t += 5
        amo.command("стоп")
        await poll(env)
        assert store.get_owner(DEAL)["owner"] == "human"
        assert sum("ИИ на паузе" in t for t in amo.texts()) == 2
    run(go())
