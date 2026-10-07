"""Тесты общих мелочей из `shared/` (без сети и прода)."""

import datetime as dt
import json
import os

import pytest
from shared.files import read_json, write_json_atomic
from shared.phones import digits_only, ru_8_to_7
from shared.timez import MSK, msk_date, now_msk, to_msk

# ── московское время ───────────────────────────────────────────────────────


def test_msk_eto_plyus_tri():
    assert MSK.utcoffset(None) == dt.timedelta(hours=3)


def test_sovpadaet_s_zoneinfo_posle_perehoda_2014():
    """Два файла в репозитории берут `ZoneInfo("Europe/Moscow")`.

    Для любой даты после 26.10.2014 результат обязан совпадать - иначе перевод тех файлов на
    `shared.timez` изменил бы поведение, а он задуман как безопасный.
    """
    zoneinfo = pytest.importorskip("zoneinfo", reason="нет модуля zoneinfo")
    try:
        moscow = zoneinfo.ZoneInfo("Europe/Moscow")
    except Exception as exc:                                        # noqa: BLE001
        pytest.skip(f"в окружении нет базы часовых поясов: {type(exc).__name__}")
    for moment in (
        dt.datetime(2014, 10, 27, tzinfo=dt.timezone.utc),          # первый день после перехода
        dt.datetime(2026, 1, 15, 9, 30, tzinfo=dt.timezone.utc),    # зима
        dt.datetime(2026, 7, 15, 9, 30, tzinfo=dt.timezone.utc),    # лето: перевода часов нет
    ):
        assert moment.astimezone(MSK).replace(tzinfo=None) == \
            moment.astimezone(moscow).replace(tzinfo=None), moment


def test_now_msk_s_poyasom():
    assert now_msk().tzinfo is not None


def test_naivnoe_vremya_schitaem_utc():
    """Так ведут себя метки от amoCRM; поведение названо в шапке модуля."""
    naive = dt.datetime(2026, 10, 7, 12, 0)
    assert to_msk(naive).hour == 15


def test_vremya_s_poyasom_perevoditsya_a_ne_podmenyaetsya():
    aware = dt.datetime(2026, 10, 7, 12, 0, tzinfo=dt.timezone.utc)
    assert to_msk(aware).hour == 15
    assert to_msk(aware).utcoffset() == dt.timedelta(hours=3)


def test_msk_date_eto_moskovskiy_den_a_ne_utc():
    """Ночь по Москве - это уже следующий день, хотя по UTC ещё предыдущий."""
    late = dt.datetime(2026, 10, 7, 22, 30, tzinfo=dt.timezone.utc)   # 01:30 мск восьмого
    assert msk_date(late) == dt.date(2026, 10, 8)


# ── атомарная запись ───────────────────────────────────────────────────────


def test_zapis_i_chtenie(tmp_path):
    path = str(tmp_path / "state.json")
    write_json_atomic(path, {"a": 1})
    assert read_json(path) == {"a": 1}


def test_katalog_sozdaetsya(tmp_path):
    path = str(tmp_path / "var" / "deep" / "state.json")
    write_json_atomic(path, [1, 2])
    assert read_json(path) == [1, 2]


def test_vremennyy_fayl_ne_ostaetsya(tmp_path):
    path = str(tmp_path / "state.json")
    write_json_atomic(path, {"a": 1})
    assert not os.path.exists(f"{path}.tmp")


def test_vremennyy_fayl_ryadom_s_celevym(tmp_path):
    """⚠️ Через границу файловых систем os.replace теряет атомарность - см. шапку модуля."""
    path = str(tmp_path / "state.json")
    assert os.path.dirname(f"{path}.tmp") == os.path.dirname(path)


def test_perezapis_ne_teryaet_staroe_pri_otkaze(tmp_path):
    """Отказ на сериализации не должен портить уже лежащий файл."""
    path = str(tmp_path / "state.json")
    write_json_atomic(path, {"ok": True})

    class _NotSerializable:
        pass

    with pytest.raises(TypeError):
        write_json_atomic(path, {"bad": _NotSerializable()})
    assert read_json(path) == {"ok": True}, "старое значение обязано уцелеть"


def test_bitogo_fayla_dostatochno_dlya_default(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{это не json", encoding="utf-8")
    assert read_json(str(path), default=[]) == []


def test_net_fayla_otdaem_default(tmp_path):
    assert read_json(str(tmp_path / "нет.json"), default={"x": 1}) == {"x": 1}


def test_ensure_ascii_po_umolchaniyu_kak_v_kopiyah(tmp_path):
    """Копии пишут `json.dump(data, f)`, то есть с экранированием. Молча менять нельзя."""
    path = tmp_path / "state.json"
    write_json_atomic(str(path), {"имя": "Катя"})
    raw = path.read_text(encoding="utf-8")
    assert "\\u" in raw, raw
    assert json.loads(raw) == {"имя": "Катя"}


def test_ensure_ascii_vyklyuchaetsya_yavno(tmp_path):
    path = tmp_path / "state.json"
    write_json_atomic(str(path), {"имя": "Катя"}, ensure_ascii=False)
    assert "Катя" in path.read_text(encoding="utf-8")


# ── телефоны: только примитивы ─────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("+7 (916) 123-45-67", "79161234567"),
        ("8 916 123 45 67", "89161234567"),
        ("", ""),
        (None, ""),
        ("без цифр", ""),
    ],
)
def test_digits_only(raw, expected):
    assert digits_only(raw) == expected


def test_vosmerka_v_semerku_tolko_dlya_odinnadcati_cifr():
    assert ru_8_to_7("89161234567") == "79161234567"


@pytest.mark.parametrize("digits", ["8612345678901", "8612", "79161234567", ""])
def test_drugaya_dlina_ne_trogaetsya(digits):
    """⚠️ У международных номеров ведущая восьмёрка значит другое (86 это Китай)."""
    assert ru_8_to_7(digits) == digits
