"""Юнит-тест lead_status_store: память о прошлом этапе сделки.

Закрывает гейт «вебхук про переход или просто про обновление» — тот самый, из-за
которого 05.10.2026 всплеск вебхуков по старым закрытым сделкам создал 208 задач
office_transfer за минуту при 11 реальных переходах и заморозил дорожку `amo`.

⚠️ Путь к базе ставим ДО импорта модуля и через setdefault — грабля 2 из
knowledge/amo-fix-fields-testy-grabli.md: путь резолвится на импорте, и первый
импортировавший выбирает его за всю сессию pytest. Без этого дефолт /app/var/...
на Windows превращается в C:\\app\\var\\... и тест пишет мимо временной папки.

Запуск: python test_lead_status_store.py  или  python -m pytest test_lead_status_store.py -q
"""
import os
import tempfile

os.environ.setdefault(
    "LEAD_STATUS_DB_PATH",
    os.path.join(tempfile.mkdtemp(), "lead_status_test.sqlite3"),
)

import lead_status_store as S  # noqa: E402

S.init()

_next_id = [700000]


def _lead() -> int:
    """Свежий id на каждый тест: база одна на весь прогон, строки не делим."""
    _next_id[0] += 1
    return _next_id[0]


def test_first_sighting_counts_as_transition():
    """Сделку видим впервые — считаем переходом. Осознанно консервативно: база
    пустеет на каждом рестарте, и пропустить настоящий перенос в Офис дороже,
    чем поставить лишнюю задачу, которую погасит гейт по closed_at."""
    lid = _lead()
    assert S.note_and_changed(lid, 143) is True
    assert S.last_seen(lid) == 143


def test_same_status_again_is_not_transition():
    """⚠️ Ядро правки. Сделка УЖЕ лежала на 143, прилетел вебхук про поле, тег
    или примечание — это не переход, задачу ставить нельзя."""
    lid = _lead()
    S.note_and_changed(lid, 143)
    assert S.note_and_changed(lid, 143) is False
    assert S.note_and_changed(lid, 143) is False


def test_flood_of_repeats_gives_one_transition():
    """Всплеск 05.10: по одной сделке прилетало по 3-4 вебхука в одну секунду.
    Переходом должен считаться ровно один."""
    lid = _lead()
    verdicts = [S.note_and_changed(lid, 143) for _ in range(6)]
    assert verdicts.count(True) == 1, verdicts
    assert verdicts[0] is True


def test_real_status_change_is_a_transition():
    lid = _lead()
    S.note_and_changed(lid, 83537718)
    assert S.note_and_changed(lid, 143) is True
    assert S.last_seen(lid) == 143


def test_reopen_and_close_again_is_a_transition():
    """Сделку переоткрыли и закрыли снова — это НОВЫЙ перенос, его терять нельзя.
    Поэтому статус пишем на каждый вебхук, а не только на 142/143."""
    lid = _lead()
    assert S.note_and_changed(lid, 143) is True           # закрылась
    assert S.note_and_changed(lid, 143) is False          # её потрогали
    assert S.note_and_changed(lid, 83537718) is True      # переоткрыли
    assert S.note_and_changed(lid, 143) is True           # закрылась снова


def test_two_leads_do_not_share_memory():
    a, b = _lead(), _lead()
    S.note_and_changed(a, 143)
    assert S.note_and_changed(b, 143) is True   # у b своя история
    assert S.note_and_changed(a, 143) is False


def test_garbage_input_counts_as_transition():
    """Гейт обязан пропускать при сомнении."""
    assert S.note_and_changed(None, 143) is True
    assert S.note_and_changed(123, None) is True
    assert S.note_and_changed("abc", "def") is True


def test_broken_store_fails_open():
    """Сломанная база не должна молча остановить переносы в Офис: при любой
    ошибке хранилища гейт отвечает «переход» и работа идёт как раньше."""
    lid = _lead()
    S.note_and_changed(lid, 143)
    real = S.DB_PATH
    S.DB_PATH = os.path.join(real, "nope", "cannot", "lead_status.sqlite3")
    try:
        assert S.note_and_changed(lid, 143) is True
        assert S.last_seen(lid) is None
    finally:
        S.DB_PATH = real
    # база цела, прошлое значение на месте
    assert S.last_seen(lid) == 143


def test_prune_drops_only_stale_rows():
    fresh, stale = _lead(), _lead()
    S.note_and_changed(fresh, 143)
    S.note_and_changed(stale, 143)
    with S._conn() as conn:
        conn.execute(
            "UPDATE lead_status SET seen_at = 1 WHERE lead_id = ?", (stale,)
        )
    removed = S.prune()
    assert removed >= 1
    assert S.last_seen(stale) is None
    assert S.last_seen(fresh) == 143


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("все тесты lead_status_store прошли")
