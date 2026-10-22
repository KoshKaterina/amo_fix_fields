"""Routes verified against live CRM on 2026-10-18; only Mini App entries change."""
import hashlib

ENTRIES = {'consultations-free','consultations-course','consultations-setup','consultations-setup30','consultations-individual',
           'cooperation-wholesale','cooperation-affiliate','cooperation-media',
           'waitlist-club','waitlist-vladimir','academy-base','academy-pro','academy-individual','shop-callback','shop-consultation','shop-unavailable','quiz-result'}

def is_miniapp(payload):
    return (payload.get('context') or {}).get('entry', '') in {'miniapp-' + entry for entry in ENTRIES}

def route(cfg, payload, entry_map):
    entry = (payload.get('context') or {}).get('entry', '')
    if not is_miniapp(payload):
        return cfg
    key = entry.removeprefix('miniapp-')
    target = dict(cfg)
    if key in {'waitlist-club','waitlist-vladimir'}:
        target.update(pipeline_id=10611934,status_id=83669950,source='Телеграм приложение: ' + ('Клуб' if key == 'waitlist-club' else 'Консультация Владимира'))
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


def academy_lead_name(payload):
    labels = {'academy-base': 'BASE', 'academy-pro': 'PRO', 'academy-individual': 'Высшая лига',
              'consultations-course': 'вопрос об обучении'}
    entry = (payload.get('context') or {}).get('entry', '').removeprefix('miniapp-')
    if not is_miniapp(payload) or entry not in labels:
        return None
    name = ' '.join(str((payload.get('contact') or {}).get('name') or 'Без имени').split())
    return f"Академия — {labels[entry]} — {name}"[:255]
