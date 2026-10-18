"""Routes verified against live CRM on 2026-10-18; only Mini App entries change."""
import hashlib

ENTRIES = {'consultations-free','consultations-course','consultations-setup','consultations-individual',
           'cooperation-wholesale','cooperation-affiliate','cooperation-media',
           'waitlist-club','waitlist-vladimir','academy-individual','shop-callback','shop-consultation'}

def is_miniapp(payload):
    return (payload.get('context') or {}).get('entry', '') in {'miniapp-' + entry for entry in ENTRIES}

def route(cfg, payload, entry_map):
    entry = (payload.get('context') or {}).get('entry', '')
    if not is_miniapp(payload):
        return cfg
    key = entry.removeprefix('miniapp-')
    target = dict(cfg)
    if key in {'waitlist-club','waitlist-vladimir'}:
        target.update(pipeline_id=10611934,status_id=83669950,source='Mini App: ' + ('Клуб' if key == 'waitlist-club' else 'Консультация Владимира'))
        target.pop('responsible_user_id',None)
    elif key == 'academy-individual':
        target.update(pipeline_id=8642410,status_id=87654850,source='Mini App: Высшая лига')
        target.pop('responsible_user_id',None)
    elif key in entry_map:
        target.update(entry_map[key])
    target['tags'] = list(dict.fromkeys([*(target.get('tags') or []),'Mini App']))
    return target

def delivery_event(payload):
    """Safe atomic outbox record, used by site_form_store.mark_done."""
    if not is_miniapp(payload): return None
    return {'event':'service_form_crm_delivered',
            'entry':payload['context']['entry'].removeprefix('miniapp-'),
            'dedup_id':hashlib.sha256(payload['submission_id'].encode()).hexdigest()}
