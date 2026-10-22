"""Routes verified against live CRM on 2026-10-18; only Mini App entries change."""
import hashlib

ENTRIES = {'consultations-free','consultations-course','consultations-setup','consultations-setup30','consultations-individual',
           'cooperation-wholesale','cooperation-affiliate','cooperation-media',
           'waitlist-club','waitlist-vladimir','academy-base','academy-pro','academy-individual','shop-callback','shop-consultation','shop-unavailable','quiz-result'}

# Воронка «Лист ожидания» (10611934): у каждой формы свой этап (Катя 22.10.2026). Раньше обе
# клали сделку на «Товар» (83669950), и уведомление про клуб и консультацию не от чего было
# ждать. «Товар» остаётся за остальными путями (ЗНР, «Нет в наличии», ручной ввод).
# Номера сверены живым чтением amo 22.10.2026.
WAITLIST_PIPELINE_ID = 10611934
WAITLIST_STAGES = {
    'waitlist-club': (89550306, 'Клуб'),                      # «Клуб»
    'waitlist-vladimir': (89550310, 'Консультация Владимира'),  # «Консультация Владимир»
}


def is_miniapp(payload):
    return (payload.get('context') or {}).get('entry', '') in {'miniapp-' + entry for entry in ENTRIES}

def route(cfg, payload, entry_map):
    entry = (payload.get('context') or {}).get('entry', '')
    if not is_miniapp(payload):
        return cfg
    key = entry.removeprefix('miniapp-')
    target = dict(cfg)
    if key in WAITLIST_STAGES:
        status_id, label = WAITLIST_STAGES[key]
        target.update(pipeline_id=WAITLIST_PIPELINE_ID,status_id=status_id,source='Телеграм приложение: ' + label)
        target.pop('responsible_user_id',None)
    elif key in {'academy-base', 'academy-pro', 'academy-individual', 'consultations-course'}:
        target.update(pipeline_id=8642410,status_id=87654850,source='Телеграм приложение: ' + {'academy-base':'Инвестиции в DeFi — BASE', 'academy-pro':'Инвестиции в DeFi — PRO', 'academy-individual':'Высшая лига', 'consultations-course':'Вопрос об обучении в Академии'}[key])
        target.pop('responsible_user_id',None)
    elif key in entry_map:
        target.update(entry_map[key])
    if not target['source'].startswith('Телеграм приложение'):
        target['source'] = 'Телеграм приложение: ' + target['source'].removeprefix('Форма: ')
    target['tags'] = list(dict.fromkeys([*(tag for tag in (target.get('tags') or []) if tag != 'Mini App'), 'Телеграм приложение']))
    return target

def delivery_event(payload):
    """Safe atomic outbox record, used by site_form_store.mark_done."""
    if not is_miniapp(payload): return None
    return {'event':'service_form_crm_delivered',
            'entry':payload['context']['entry'].removeprefix('miniapp-'),
            'dedup_id':hashlib.sha256(payload['submission_id'].encode()).hexdigest()}


LEAD_LABELS = {
    'academy-base': 'Академия — BASE',
    'academy-pro': 'Академия — PRO',
    'academy-individual': 'Академия — Высшая лига',
    'consultations-course': 'Академия — вопрос об обучении',
    'consultations-free': 'Бесплатная консультация',
    'consultations-setup30': 'Настройка кошелька — 30 минут',
    'consultations-setup': 'Настройка кошелька',
    'consultations-individual': 'Индивидуальная консультация',
    'cooperation-wholesale': 'Оптовая закупка',
    'cooperation-affiliate': 'Реферальная программа',
    'cooperation-media': 'Медиа / проект',
    'waitlist-club': 'Клуб — лист ожидания',
    'waitlist-vladimir': 'Консультация Владимира — лист ожидания',
    'shop-callback': 'Вопрос / обратный звонок',
    'shop-consultation': 'Консультация по кошельку',
    'shop-unavailable': 'Нет в наличии',
    'quiz-result': 'Подбор кошелька',
}


def miniapp_lead_name(payload):
    entry = (payload.get('context') or {}).get('entry', '').removeprefix('miniapp-')
    if not is_miniapp(payload) or entry not in LEAD_LABELS:
        return None
    name = ' '.join(str((payload.get('contact') or {}).get('name') or 'Без имени').split())
    return f"{LEAD_LABELS[entry]} — {name}"[:255]
