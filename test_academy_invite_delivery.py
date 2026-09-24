import asyncio

import academy_invite_delivery as mod


def run(coro):
    return asyncio.run(coro)


def payload(**overrides):
    data = {
        "cuid": "7hw4.ddv",
        "first_name": "Екатерина",
        "Регистрация на мероприятие": "Практикум октябрь 2026",
    }
    data.update(overrides)
    return data


def test_message_uses_first_names_and_approved_copy(monkeypatch):
    monkeypatch.setattr(mod, "ACADEMY_MANAGER_FIRST_NAME", "Артем")
    text = mod._message(payload(), "https://t.me/+one-use")
    assert text == (
        "Здравствуйте, Екатерина!\n"
        "Меня зовут Артем, менеджер академии Sunscrypt.\n\n"
        "Добавляйтесь в чат практикума по ссылке: https://t.me/+one-use\n\n"
        "Если у Вас остались какие-то вопросы или нужна будет помощь - обращайтесь, я на связи!"
    )


def test_process_is_idempotent(monkeypatch, tmp_path):
    monkeypatch.setattr(mod, "ACADEMY_BOTHELP_CLIENT_ID", "id")
    monkeypatch.setattr(mod, "ACADEMY_BOTHELP_CLIENT_SECRET", "secret")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_SENT_PATH", str(tmp_path / "sent.json"))
    lead = {
        "id": 10,
        "custom_fields_values": [
            {"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK, "values": [{"value": "https://t.me/+one-use"}]},
        ],
    }
    calls = []

    async def get_lead(*_args, **_kwargs):
        return lead

    async def request(method, path, *, body, content_type):
        calls.append((method, path, body, content_type))
        return True

    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod, "_bothelp_request", request)

    assert run(mod.process(payload(), 10)) == "sent"
    assert run(mod.process(payload(), 10)) == "already_sent"
    assert [call[0] for call in calls] == ["PATCH", "POST"]


def test_process_ignores_non_practicum(monkeypatch):
    monkeypatch.setattr(mod, "ACADEMY_BOTHELP_CLIENT_ID", "id")
    monkeypatch.setattr(mod, "ACADEMY_BOTHELP_CLIENT_SECRET", "secret")
    assert run(mod.process(payload(**{"Регистрация на мероприятие": "Конференция"}), 10)) == "not_practicum"
