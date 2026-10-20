"""No live imports, credentials, networks, CRM writes or shared monkeypatches."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import checkout_consent_sync as sync

NOW = datetime.fromisoformat('2026-10-20T15:00:00+03:00')
AT = NOW - timedelta(hours=1)
OID = '00000000-0000-4000-8000-000000000001'


def order(yes=True, at=AT):
    value = (f'ПДн: да, реклама (email/SMS/мессенджеры/звонки): {"да" if yes else "нет"}, '
             f'дата: {at.isoformat()}, версия ПДн: policy-123456abcdef, версия рекламы: {sync.VERSION}')
    return {'id': OID, 'attributes': [{'id': sync.MS_FIELD, 'value': value}]}


def contact(values=None):
    return {'id': 123, 'custom_fields_values': [
        {'field_id': k, 'values': [{'value': v}]} for k, v in (values or {}).items()]}


class Fake:
    def __init__(self, source=None, person=None):
        self.source = source or order()
        self.person = person or contact()
        self.link = 123
        self.changed = set()
        self.writes = []
        self.write_exception = False
        self.silent_commit = False
        self.contact_reads = 0
        self.concurrent_change = None

    async def order(self, oid):
        return deepcopy(self.source)

    async def linked_contact(self, oid):
        return self.link

    async def contact(self, cid):
        self.contact_reads += 1
        if self.contact_reads == 2 and self.concurrent_change:
            self.person = self.concurrent_change
        return deepcopy(self.person)

    async def changes_after(self, cid, at):
        return self.changed

    async def patch_contact(self, cid, fields):
        self.writes.append((cid, fields))
        if self.write_exception and not self.silent_commit:
            raise TimeoutError()
        values = {x['field_id']: x['values'][0]['value'] for x in self.person['custom_fields_values']}
        values.update(fields)
        self.person = contact(values)
        if self.write_exception:
            raise TimeoutError()
        return True


class ParserTests(unittest.TestCase):
    def test_both_choices_and_timestamp(self):
        self.assertTrue(sync.parse_order(order(), NOW).marketing)
        self.assertFalse(sync.parse_order(order(False), NOW).marketing)
        self.assertEqual(sync.parse_order(order(), NOW).at, AT)

    def test_absent_duplicate_bad_version_wrong_channel_no_pdn_and_future(self):
        bad = [order(at=AT-timedelta(days=5)), order(at=NOW+timedelta(minutes=2)), {'id': OID}, {'attributes': [None]}]
        duplicate = order(); duplicate['attributes'] *= 2; bad.append(duplicate)
        for old, new in [('ПДн: да', 'ПДн: нет'), ('email/SMS/мессенджеры/звонки','email'),
                         (sync.VERSION,'checkout-email-v2-2026-10-08'), ('+03:00',''),
                         ('policy-123456abcdef','unknown'), ('реклама (','Реклама (')]:
            payload = order(); payload['attributes'][0]['value'] = payload['attributes'][0]['value'].replace(old,new)
            bad.append(payload)
        for payload in bad:
            with self.subTest(payload=payload): self.assertIsNone(sync.parse_order(payload, NOW))

    def test_contract_from_actual_woo_producer(self):
        # One field from the previously verified Woo→MS producer, no hand-reconstructed serialization.
        fixture = Path(__file__).parent/'test-fixtures'/'checkout-consent-producer.json'
        for row in json.loads(fixture.read_text()):
            payload = {'attributes': [{'id': sync.MS_FIELD, 'value': row['expected']}]}
            evidence = sync.parse_order(payload, NOW + timedelta(days=5))
            self.assertIsNotNone(evidence)
            snap = row['meta_data'][0]['value']
            self.assertEqual(evidence.marketing, snap['marketing']=='yes')
            self.assertEqual(evidence.pd_version, snap['pdn_version'])


class PlannerTests(unittest.TestCase):
    def test_new_yes_populates_only_six_existing_fields(self):
        fields = sync.plan(contact(), sync.parse_order(order(), NOW))
        self.assertEqual(set(fields), set(sync.FIELDS))
        self.assertEqual(fields[sync.AD[0]], 'Да')

    def test_unchecked_does_not_revoke_or_populate_advertising(self):
        evidence = sync.parse_order(order(False), NOW)
        for values in ({}, {sync.AD[0]:'Да'}, {sync.AD[0]:'Нет'}):
            result = sync.plan(contact(values), evidence)
            self.assertTrue(set(result).issubset(sync.PD))

    def test_newer_refusal_date_and_equal_timestamp_preserved(self):
        for at in (AT, NOW):
            old = contact(dict(zip(sync.AD, ('Нет','another-source',at.isoformat()))))
            self.assertFalse(set(sync.plan(old,sync.parse_order(order(), NOW))).intersection(sync.AD))

    def test_old_refusal_may_be_replaced_by_new_explicit_choice(self):
        old = contact(dict(zip(sync.AD, ('Нет','another-source',(AT-timedelta(days=1)).isoformat()))))
        self.assertEqual(sync.plan(old,sync.parse_order(order(), NOW))[sync.AD[0]],'Да')

    def test_unknown_or_missing_dates_are_not_overwritten(self):
        for value in ('', 'garbage', '20/10/1791 blah', '20/10/1791'):
            old=contact(dict(zip(sync.AD, ('Нет','legacy',value))))
            self.assertFalse(set(sync.plan(old,sync.parse_order(order(), NOW))).intersection(sync.AD))

    def test_same_day_legacy_date_preserved_but_previous_day_allows(self):
        for date,allowed in [('20/10/2026',False),('19/10/2026',True),('20.10.2026',False)]:
            old=contact(dict(zip(sync.AD,('Нет','legacy',date))))
            self.assertEqual(bool(set(sync.plan(old,sync.parse_order(order(),NOW))).intersection(sync.AD)),allowed)

    def test_cleared_fields_with_later_history_do_not_reenable(self):
        fields=sync.plan(contact(),sync.parse_order(order(),NOW),{sync.AD[0]})
        self.assertFalse(set(fields).intersection(sync.AD))
        self.assertEqual(set(fields),set(sync.PD))

    def test_duplicate_contact_values_fail_closed(self):
        person=contact({sync.AD[0]:'Да'});person['custom_fields_values']*=2
        with self.assertRaises(sync.UncertainRead): sync.plan(person,sync.parse_order(order(),NOW))


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store=sync.Store(':memory:')
        self.io=Fake()
        self.evidence=sync.parse_order(self.io.source,NOW)
        self.store.enqueue(OID,self.evidence)

    def tearDown(self): self.store.db.close()

    async def work(self, dry=False):
        return await sync.process_job(self.store.active()[0], self.store, self.io, dry_run=dry, now=NOW)

    async def test_apply_only_contact_and_idempotent_repeat(self):
        self.assertEqual(await self.work(),'applied')
        self.assertEqual(len(self.io.writes),1)
        self.store.enqueue(OID,self.evidence)
        self.assertEqual(self.store.active(),[])

    async def test_dry_run_never_writes(self):
        self.assertEqual(await self.work(True),'would_patch')
        self.assertEqual(self.io.writes,[])

    async def test_delayed_crm_link_kept_and_retried(self):
        self.io.link=None
        self.assertEqual(await self.work(),'awaiting_crm_link')
        self.io.link=123
        self.assertEqual(await self.work(),'applied')

    async def test_concurrent_read_change_not_overwritten(self):
        self.io.concurrent_change=contact({sync.AD[0]:'Нет'})
        self.assertEqual(await self.work(),'contact_changed_retry')
        self.assertEqual(self.io.writes,[])

    async def test_timeout_is_parked_and_not_retried(self):
        self.io.write_exception=True
        self.assertEqual(await self.work(),'parked')
        self.assertEqual(await self.work(),'parked')
        self.assertEqual(len(self.io.writes),1)

    async def test_timeout_after_commit_is_reconciled_without_second_patch(self):
        self.io.write_exception=True;self.io.silent_commit=True
        self.assertEqual(await self.work(),'parked')
        self.assertEqual(await self.work(),'reconciled')
        self.assertEqual(len(self.io.writes),1)

    async def test_changed_order_replaces_queued_snapshot(self):
        self.io.source=order(False)
        self.assertEqual(await self.work(),'superseded')
        self.assertFalse(self.io.writes)

    async def test_later_ad_event_keeps_only_pdn_patch(self):
        self.io.changed={sync.AD[0]}
        self.assertEqual(await self.work(),'applied')
        self.assertEqual(set(self.io.writes[0][1]),set(sync.PD))

    async def test_read_failure_does_not_make_write_or_consume_job(self):
        async def fail(cid,at): raise sync.UncertainRead('unavailable')
        self.io.changes_after=fail
        with self.assertRaises(sync.UncertainRead): await self.work()
        self.assertFalse(self.io.writes)
        self.assertEqual(self.store.active()[0]['state'],'pending')

    async def test_scan_failure_does_not_advance_cursor(self):
        class Bad:
            async def orders(self,start,end,offset): raise sync.UncertainRead('unavailable')
        with self.assertRaises(sync.UncertainRead): await sync.scan(self.store,Bad(),AT,NOW)
        self.assertIsNone(self.store.db.execute('SELECT * FROM cursor').fetchone())

    async def test_scan_limit_does_not_advance_cursor(self):
        class Busy:
            async def orders(self,start,end,offset): return [order()]*100
        with self.assertRaises(sync.UncertainRead): await sync.scan(self.store,Busy(),AT,NOW,max_pages=1)
        self.assertIsNone(self.store.db.execute('SELECT * FROM cursor').fetchone())

    async def test_pending_jobs_are_fair_and_do_not_starve_after_twenty_missing_links(self):
        store=sync.Store(':memory:')
        for n in range(30): store.enqueue(f'{n:032x}',self.evidence)
        first={row['order_id'] for row in store.active()}
        second={row['order_id'] for row in store.active()}
        self.assertEqual(len(first),20)
        self.assertEqual(len(second-first),10)
        store.db.close()

    async def test_successful_scan_persists_cursor_and_enqueues_only_valid_evidence(self):
        class Good:
            async def orders(self,start,end,offset): return [order(),{'id':OID,'attributes':[]}]
        self.assertEqual(await sync.scan(self.store,Good(),AT,NOW),2)
        self.assertEqual(self.store.db.execute('SELECT at FROM cursor').fetchone()['at'],NOW.isoformat())
        self.assertEqual(len(self.store.active()),1)

    async def test_persistent_ledger_survives_new_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=str(Path(tmp)/'state.sqlite3')
            first=sync.Store(path);first.enqueue(OID,self.evidence);first.db.close()
            second=sync.Store(path)
            self.assertEqual(len(second.active()),1)
            second.db.close()


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    def runtime(self):
        # Lazy runtime construction: no imports of credential-owning modules in tests.
        obj=sync.Runtime.__new__(sync.Runtime)
        obj.order_field=576689
        async def nop(): pass
        obj.pace=nop
        return obj

    async def test_patch_is_allowlisted_and_single_attempt(self):
        obj=self.runtime();calls=[]
        class Api:
            BASE_URL='https://example.invalid'
            headers={}
            async def submit_request(self,*args,**kw):
                calls.append((args,kw))
                return type('Response',(),{'status_code':500})()
        obj.api=Api()
        self.assertFalse(await obj.patch_contact(123,{sync.AD[0]:'Да'}))
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][0][0],'PATCH')
        self.assertEqual(calls[0][1]['json_body']['custom_fields_values'][0]['field_id'],sync.AD[0])
        with self.assertRaises(ValueError): await obj.patch_contact(123,{578315:'old lead field'})
        self.assertEqual(len(calls),1)

    async def test_strict_uuid_matching_ignores_fuzzy_results(self):
        obj=self.runtime()
        class Amo:
            async def find_leads_by_query(self,q,limit):
                return [{'id':1,'order':'wrong'}, {'id':2,'order':OID}]
            def get_custom_field_value(self,row,fid): return row['order']
            async def get_lead_full(self,lid,with_):
                self.assert_id=lid
                return {'id':lid,'order':OID,'_embedded':{'contacts':[{'id':123}]}}
        obj.amo=Amo()
        self.assertEqual(await obj.linked_contact(OID),123)
        self.assertEqual(obj.amo.assert_id,2)

    async def test_different_contacts_for_duplicate_leads_rejected(self):
        obj=self.runtime()
        class Amo:
            async def find_leads_by_query(self,q,limit): return [{'id':1,'order':OID},{'id':2,'order':OID}]
            def get_custom_field_value(self,row,fid): return row['order']
            async def get_lead_full(self,lid,with_):
                return {'id':lid,'order':OID,'_embedded':{'contacts':[{'id':lid}]}}
        obj.amo=Amo()
        with self.assertRaises(sync.UncertainRead): await obj.linked_contact(OID)

    async def test_unavailable_search_is_not_no_contact(self):
        obj=self.runtime()
        class Amo:
            async def find_leads_by_query(self,q,limit): return None
        obj.amo=Amo()
        with self.assertRaises(sync.UncertainRead): await obj.linked_contact(OID)

    async def test_history_filters_use_documented_array_and_validate_entity(self):
        obj=self.runtime();seen=[]
        class Amo:
            async def _do_get(self,path,params):
                seen.extend(params)
                return {'_embedded':{'events':[
                    {'type':f'custom_field_{sync.AD[0]}_value_changed','entity_type':'contact','entity_id':123,'created_at':int(NOW.timestamp())},
                    {'type':f'custom_field_{sync.PD[0]}_value_changed','entity_type':'contact','entity_id':456,'created_at':int(NOW.timestamp())}]}}
        obj.amo=Amo()
        self.assertEqual(await obj.changes_after(123,AT),{sync.AD[0]})
        self.assertEqual(len([x for x in seen if x[0]=='filter[type][]']),6)
        self.assertIn(('filter[entity_id][]','123'),seen)

    async def test_incomplete_event_history_fails_closed(self):
        obj=self.runtime()
        class Amo:
            async def _do_get(self,path,params): return {'_links':{'next':{'href':'more'}}}
        obj.amo=Amo()
        with self.assertRaises(sync.UncertainRead): await obj.changes_after(123,AT)

    async def test_wrong_contact_field_type_rejected(self):
        obj=self.runtime()
        class Amo:
            async def _do_get(self,path): return {'id':sync.PD[0],'type':'checkbox'}
        obj.amo=Amo()
        with self.assertRaises(sync.UncertainRead): await obj.validate_fields()


if __name__=='__main__': unittest.main()
