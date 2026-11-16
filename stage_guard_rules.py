"""Правила сторожа перемещений сделок - чистая логика без сети (постановка Кати 13.11.2026).

Зачем отдельный файл. Сторож (`stage_guard.py`) читает журнал событий amo и решает по
каждому переходу этапа: законно, нарушение (откатить + уведомить) или повод только
уведомить. Само решение - таблица правил, её удобно проверять тестами без amo и Телеграма.

Что сторожим. Те же правила, что держит виджет F5 «Контроль перемещений» в интерфейсе amo
(настроен 13.11.2026). Пока виджет жив, людям такие переходы просто недоступны, и сторож
молчит. Если виджет отвалится (истечёт оплата, сломается обновлением amo, слетит доступ, как
29.06.2026), правила держит сервер: нарушение откатывается в течение минуты.

  G5  межворонковый ход. Из ОП розницы, TangemShop, ОПТ руками - только внутри своей воронки.
      Переносы между воронками делает робот по УР/ЗИН. Касается и офиса (решение Кати 13.11).
  G6  онлайн-МОП в воронке Офис. Можно только «Предзаказ оплачен» → «Сделать накладную»,
      «Оформить доставку», «Закрыто и не реализовано». Всё остальное в Офисе и из Офиса - нет.
  G2  «Успешно реализовано» Офиса - только офис, Артём (ОПТ) и исключённые (Гладков, Перфилов).
  G7  сделку вернули из Офиса в ОП розницу или TangemShop - это НЕ запрет, только уведомление.

Кто не судится никогда:
  • created_by = 0 - робот (Цифровая воронка, боты amo);
  • исключённые пользователи (по умолчанию Гладков 11513202 и Перфилов 6653350). ⚠️ Токен
    нашей интеграции выписан на Гладкова: всё, что делает сервис, в журнале amo подписано им.
    Поэтому собственный откат сторожа в журнале не виден как нарушение - карусели нет.

Офис внутри воронки Офис не ограничен (решение Кати 13.11.2026, пункт 8 исходного плана снят).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# ─────────────────────────────── воронки и этапы ───────────────────────────────

PIPELINE_RETAIL = 10593102      # ОП розница
PIPELINE_TANGEMSHOP = 9822330   # TangemShop
PIPELINE_OPT = 10131762         # ОПТ
PIPELINE_OFFICE = 9421022       # Офис

STATUS_WON = 142
STATUS_LOST = 143

# Офис
STATUS_PREORDER_PAID = 83953914     # Предзаказ оплачен
STATUS_MAKE_WAYBILL = 75426822      # Сделать накладную
STATUS_ARRANGE_DELIVERY = 75426826  # Оформить доставку

# Из каких воронок руками в другие воронки нельзя (G5).
LOCKED_SOURCE_PIPELINES = frozenset({PIPELINE_RETAIL, PIPELINE_TANGEMSHOP, PIPELINE_OPT})

# Куда возврат из Офиса - повод уведомить (G7).
OFFICE_RETURN_TARGETS = frozenset({PIPELINE_RETAIL, PIPELINE_TANGEMSHOP})

# Единственный разрешённый онлайн-МОПу ход в Офисе (G6): из «Предзаказ оплачен» сюда.
MOP_OFFICE_ALLOWED_TARGETS = frozenset({STATUS_MAKE_WAYBILL, STATUS_ARRANGE_DELIVERY, STATUS_LOST})

STATUS_WAYBILL_READY = 75426874     # Готова накладная

# Ходы онлайн-МОПа в Офисе, которые сторож пока не судит (решение Кати 16.11.2026).
# «Готова накладная → Сделать накладную» сразу после своего УР: по словам Кати, баг amo,
# ход обратим - в чат ОП не шлём. Дубли накладных СДЭК от этого хода - отдельная задача.
MOP_OFFICE_IGNORED_MOVES = frozenset({(STATUS_WAYBILL_READY, STATUS_MAKE_WAYBILL)})

ROBOT_USER_ID = 0


def _ids(env: str, default: str) -> frozenset[int]:
    raw = os.getenv(env, default)
    out = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return frozenset(out)


@dataclass(frozen=True)
class Config:
    exempt: frozenset[int]          # не судим никогда (кроме робота - он вне правил всегда)
    online_mops: frozenset[int]     # Игорь, Егор, «красный телефон»
    office_won_allowed: frozenset[int]  # кому можно в УР Офиса (G2), сверх исключённых

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            exempt=_ids("STAGE_GUARD_EXEMPT_USERS", "11513202,6653350"),
            online_mops=_ids("STAGE_GUARD_ONLINE_MOPS", "9291546,13929334,14310230"),
            # Кирилл Полесский, Екатерина Зубалий (офис), Артём Коннов (ОПТ).
            office_won_allowed=_ids("STAGE_GUARD_OFFICE_WON_USERS", "13946318,13963494,13822630"),
        )


# ─────────────────────────────── переход и решение ───────────────────────────────


@dataclass(frozen=True)
class Move:
    event_id: str
    lead_id: int
    user_id: int
    created_at: int
    from_pipeline: int
    from_status: int
    to_pipeline: int
    to_status: int


@dataclass(frozen=True)
class Verdict:
    rule: str        # G5 | G6 | G2 | G7
    action: str      # rollback | notify
    reason: str      # человеческим языком, для текста уведомления


def parse_event(ev: dict) -> Move | None:
    """Событие `lead_status_changed` из `/api/v4/events` → Move. Чужое/битое → None."""
    if ev.get("type") not in (None, "lead_status_changed"):
        return None
    try:
        before = (ev.get("value_before") or [{}])[0].get("lead_status") or {}
        after = (ev.get("value_after") or [{}])[0].get("lead_status") or {}
        return Move(
            event_id=str(ev["id"]),
            lead_id=int(ev["entity_id"]),
            user_id=int(ev.get("created_by") or 0),
            created_at=int(ev.get("created_at") or 0),
            from_pipeline=int(before["pipeline_id"]),
            from_status=int(before["id"]),
            to_pipeline=int(after["pipeline_id"]),
            to_status=int(after["id"]),
        )
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        return None


def judge(m: Move, cfg: Config) -> Verdict | None:
    """Законно → None. Иначе одно решение, самое строгое правило выигрывает.

    Порядок: G6 (онлайн-МОП и Офис) раньше G5, чтобы уведомление называло понятную причину;
    G7 проверяется последним и только если ход вообще разрешён.
    """
    if m.user_id == ROBOT_USER_ID or m.user_id in cfg.exempt:
        return None
    if (m.from_pipeline, m.from_status) == (m.to_pipeline, m.to_status):
        return None

    touches_office = PIPELINE_OFFICE in (m.from_pipeline, m.to_pipeline)

    # G6: онлайн-МОП и воронка Офис.
    if m.user_id in cfg.online_mops and touches_office:
        if (m.from_pipeline == m.to_pipeline == PIPELINE_OFFICE
                and (m.from_status, m.to_status) in MOP_OFFICE_IGNORED_MOVES):
            return None
        allowed = (
            m.from_pipeline == PIPELINE_OFFICE and m.from_status == STATUS_PREORDER_PAID
            and m.to_pipeline == PIPELINE_OFFICE and m.to_status in MOP_OFFICE_ALLOWED_TARGETS
        )
        if not allowed:
            return Verdict("G6", "rollback", "онлайн-менеджеру в Офисе можно только перевести "
                           "«Предзаказ оплачен» в «Сделать накладную», «Оформить доставку» или «Закрыто»")

    # G5: из розницы, TangemShop, ОПТ - только внутри своей воронки.
    if m.from_pipeline in LOCKED_SOURCE_PIPELINES and m.to_pipeline != m.from_pipeline:
        return Verdict("G5", "rollback", "переносить сделку в другую воронку руками нельзя - "
                       "это делает автоматика по «Успешно реализовано» или «Закрыто»")

    # G2: УР Офиса.
    if (m.to_pipeline == PIPELINE_OFFICE and m.to_status == STATUS_WON
            and m.user_id not in cfg.office_won_allowed):
        return Verdict("G2", "rollback", "закрыть сделку Офиса как успешную может только офис")

    # G7: вернули из Офиса в ОП - не запрет, только уведомление.
    if m.from_pipeline == PIPELINE_OFFICE and m.to_pipeline in OFFICE_RETURN_TARGETS:
        return Verdict("G7", "notify", "сделку вернули из Офиса")

    return None
