"""Тесты фильтра «не требует ответа». Кейсы взяты из ЖИВОГО корпуса wazzup_message
(992 сообщения, 30.06-03.08.2026) и из правок Кати на разборе 03.08.2026.

Запуск: python3 test_sla_filter.py   (или python3 -m pytest test_sla_filter.py -q)
"""
import sla_filter as F


def _msg(text=None, type_="text"):
    return {"text": text, "type": type_, "isEcho": False, "status": "inbound"}


# --- ГЛУШИМ ----------------------------------------------------------------

def test_unsupported_type_is_silent():
    """«Вам что-то прислали. Такой формат не поддерживается» = реакция/стикер/кружок.
    Решение Кати 03.08.2026: глушим. Ловим по type, не по тексту."""
    m = _msg("Вам что-то прислали. Такой формат сообщений пока не поддерживается. "
             "Попросите собеседника отправить информацию текстом.", "unsupported")
    ok, reason = F.is_closing_message(m)
    assert ok is True
    assert "unsupported" in reason


def test_emoji_only():
    """Из корпуса: входящее «🤝» — реакция без текста."""
    for t in ("🤝", "👍", "❤️", "😊😊", "🔥 "):
        ok, reason = F.is_closing_message(_msg(t))
        assert ok is True, t
        assert reason == "эмодзи", t


def test_punctuation_only_is_not_muted():
    """Чистая пунктуация без эмодзи — непонятный сигнал, безопаснее разбудить."""
    for t in ("))", "...", "!!!"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is False, t


def test_thanks_variants():
    for t in ("Спасибо", "спасибо", "Благодарю", "И спасибо.", "Спасибо! Хорошего дня",
              "Нашла. Благодарю", "а, хорошо, спасибо)", "Благодарю вас"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_polite_refusal():
    """Ответ на кнопку бота «Нужна помощь?» — 8 раз в корпусе."""
    for t in ("Нет, благодарю", "Нет, спасибо", "Нет , спасибо"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_confirmation():
    """Ответ на кнопку бота «Вы оформили заказ №…, всё верно?» — 16 раз в корпусе."""
    for t in ("Да, всё верно", "Да, все верно", "Да,все верно", "Да, верно."):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_short_agreement():
    for t in ("Ок", "Хорошо", "Отлично", "Понял", "Ясно", "Взаимно", "Се четко",
              "Хорошо, жду заказ"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_thanks_with_tail_by_words():
    """Правка 06.08.2026 по корпусу 28.07–06.08: та же благодарность с хвостом.
    Точным совпадением фразы не ловилось — теперь разбираем по словам."""
    for t in ("Ок, спасибо", "Ок спасибо", "Спасибо. Понял", "Понял, благодарю",
              "супер спасибо", "супер, благодарю", "а ну отлично", "Принял",
              "Супер", "Спасибо, будем иметь ввиду", "Нет, спасибо, все ок",
              "Большое спасибо", "Нет, благодарю, хорошего дня!"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_emoji_glued_to_phrase():
    """Смайлик в конце не должен ломать совпадение: за 9 дней мимо фильтра прошли
    «Спасибо 🙏», «Понял. 🫡», «Спасибо. 🤝»."""
    for t in ("Спасибо 🙏", "Понял. 🫡", "Спасибо. 🤝", "Хорошо 👍"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is True, t


def test_deleted_and_system_types():
    """Клиент удалил сообщение / служебная запись мессенджера — отвечать не на что."""
    for typ in ("deleted", "system"):
        ok, reason = F.is_closing_message(_msg("", typ))
        assert ok is True, typ
        assert typ in reason


def test_bot_command():
    """«/start» — команда телеграм-бота, а не человек."""
    ok, _ = F.is_closing_message(_msg("/start"))
    assert ok is True
    ok, _ = F.is_closing_message(_msg("/start нужен кошелёк"))
    assert ok is False


# --- НЕ ГЛУШИМ (правки Кати: новый факт или действие = алерт) ---------------

def test_podyehal_needs_attention():
    """Правка Кати 03.08.2026: «Подъехал» ТРЕБУЕТ внимания — клиент у дверей."""
    ok, _ = F.is_closing_message(_msg("Подъехал"))
    assert ok is False


def test_facts_and_actions_need_alert():
    cases = [
        "На месте",                      # клиент приехал
        "Оплачено",                      # надо проверить платёж (5 раз в корпусе)
        "Буду минут через 25-30",        # договорённость по времени
        "Заберите пжл с сдека ваш кошелек",
        "Лучше верните деньги.",         # конфликт
        "Не нужен кошелек",              # отказ от заказа
        "Спасибо, написал",              # благодарность + новый факт
        "Здравствуйте, оплату произвел",
    ]
    for t in cases:
        ok, reason = F.is_closing_message(_msg(t))
        assert ok is False, f"{t!r} не должно глушиться (получено: {reason})"


def test_greetings_are_openers_not_closers():
    """Приветствие — начало разговора, а не конец. Обязательно алертим."""
    for t in ("Здравствуйте", "Добрый день", "Добрый вечер!", "Здравствуйте 👋",
              "Доброе утро", "Егор , добрый день",
              # ловушка разбора по словам: оба слова хвостовые, ядра нет → алерт
              "Доброго дня", "Доброго вечера"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is False, t


def test_choices_in_dialog_need_alert():
    """«Да» на «1 набор?» и «1» из меню — за ними ждут продолжения."""
    for t in ("Да", "1", "Да, расскажите", "Да, ещё нужна помощь"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is False, t


def test_questions_need_alert():
    for t in ("Паспорт нужно с собой брать?", "Получили мой платеж?",
              "Нужна ссылка для оплаты банковской картой",
              "Здравствуйте, в Чите у вас обменник есть?", "?"):
        ok, _ = F.is_closing_message(_msg(t))
        assert ok is False, t


def test_media_without_text_is_not_closing():
    """Фото/голосовое/видео без подписи — клиент что-то прислал, надо смотреть."""
    for typ in ("image", "audio", "video", "document"):
        ok, _ = F.is_closing_message(_msg(None, typ))
        assert ok is False, typ
        ok, _ = F.is_closing_message(_msg("", typ))
        assert ok is False, typ


def test_long_text_never_closing():
    ok, _ = F.is_closing_message(_msg(
        "Добрый день. Я сейчас был в пункте выдачи СДЭК они сказали что с почтой "
        "России не сотрудничают, что делать?"))
    assert ok is False


def test_garbage_input():
    assert F.is_closing_message(None)[0] is False
    assert F.is_closing_message({})[0] is False
    assert F.is_closing_message({"text": None})[0] is False


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    ok_n = 0
    for fn in fns:
        try:
            fn()
            print(f"✅ {fn.__name__}")
            ok_n += 1
        except Exception:
            print(f"❌ {fn.__name__}")
            traceback.print_exc()
    print(f"\n{ok_n}/{len(fns)} прошли")
