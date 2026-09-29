"""Тесты выбора этапа в приёмнике BotHelp (academy_bothelp_upsert._target_status).

Почему эти тесты появились 29.09.2026. Блок перехода в сценарии BotHelp перенесли на старт,
и стало видно: webhook несёт ПРОФИЛЬ подписчика целиком, а не «что изменилось». На повторном
/start приезжают те же поля, что человек заполнял неделю назад. Отсюда два класса поломок,
каждый закрыт тестом ниже:

  • откат внутри бот-этапов - частичный профиль тащил сделку с «Анкеты пройденной» назад;
  • прыжок через воронку - старый флаг практикума в карточке уводил сделку в «Записан
    на практикум» на голом /start, хотя этот этап по договорённости ставит менеджер.

Запуск: python3 -m pytest test_academy_bothelp_upsert.py -q
"""

import os
import sys

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import academy_bothelp_upsert as up  # noqa: E402

INBOUND = up.STATUS_INBOUND
BOT = up.STATUS_BOT_STARTED
QUEST = up.STATUS_QUESTIONNAIRE
QUEST_DONE = up.STATUS_QUESTIONNAIRE_DONE
PRACTICUM = up.STATUS_RECORDED_PRACTICUM
WAITLIST = 70070966  # «Лист ожидания» — вне бот-этапов


def _profile(experience="", capital="", purpose="", event="", action=""):
    return {
        "опыт_в_инвестициях": experience,
        "размер_капитала": capital,
        "зачем_капитал": purpose,
        "Регистрация на мероприятие": event,
        "действие менеджера": action,
    }


# ── старт: создать и не двигать ──────────────────────────────────────────────

def test_sdelki_net_pustoy_profil_daet_bot_zapushchen():
    assert up._target_status(_profile(), None) == BOT


def test_tolko_chto_sozdannuyu_sdelku_ne_dvigaem():
    """Сделка родилась на «Боте запущен» — старый профиль не тащит её в середину воронки."""
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, BOT, just_created=True) is None


def test_start_po_sdelke_na_bote_zapushchennom_nichego_ne_menyaet():
    assert up._target_status(_profile(), BOT) is None


def test_vhodyashchiy_lid_podnimaetsya_do_bota():
    assert up._target_status(_profile(), INBOUND) == BOT


# ── только вперёд ────────────────────────────────────────────────────────────

def test_chastichnyy_profil_ne_tashchit_anketu_nazad():
    """Главный кейс: пришёл ОДИН ответ, а сделка уже на «Анкете пройденной»."""
    assert up._target_status(_profile(experience="с нуля"), QUEST_DONE) is None


def test_polnyy_profil_ne_otkatyvaet_s_ankety_proydennoy():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, QUEST_DONE) is None


def test_vpered_po_ankete_rabotaet():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, BOT) == QUEST_DONE


def test_odin_otvet_podnimaet_s_bota_na_prohodit_anketu():
    assert up._target_status(_profile(experience="с нуля"), BOT) == QUEST


# ── практикум ────────────────────────────────────────────────────────────────

def test_staryy_flag_praktikuma_ne_dvigaet_sdelku():
    """Человек записался на прошлой неделе, сегодня просто нажал /start."""
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=False) is None


def test_novyy_flag_praktikuma_dvigaet():
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=True) == PRACTICUM


def test_praktikum_iz_deystviya_klienta_tozhe_lovitsya():
    payload = _profile(action="записаться на практикум")
    assert up._target_status(payload, BOT, practicum_is_new=True) == PRACTICUM


def test_staryy_flag_praktikuma_ne_meshaet_ankete():
    """Флаг старый, но человек прямо сейчас отвечает на анкету — анкета едет."""
    payload = _profile(experience="с нуля", event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=False) == QUEST


# ── защищённые этапы ─────────────────────────────────────────────────────────

def test_zapisan_na_praktikum_ne_trogaem():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, PRACTICUM) is None


def test_list_ozhidaniya_ne_trogaem():
    assert up._target_status(_profile(experience="с нуля"), WAITLIST) is None


def test_praktikum_s_zashchishchennogo_etapa_ne_stavitsya():
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, WAITLIST, practicum_is_new=True) is None


# ── вспомогательное ──────────────────────────────────────────────────────────

def test_wants_practicum_chitaet_oba_polya_i_registr():
    assert up._wants_practicum("ПРАКТИКУМ октябрь", "") is True
    assert up._wants_practicum("", "Записаться на Практикум") is True
    assert up._wants_practicum("Конференция", "написать менеджеру") is False
    assert up._wants_practicum(None, None) is False
