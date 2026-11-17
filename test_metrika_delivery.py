"""Delivery failure/restart tests: no live clients or application startup."""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import metrika_client as client
import metrika_sync as sync
from shared.metrika_delivery import DeliveryStore


def receipt(uid='u1', status='PASSED', count=1):
    return {'uploading_id': uid, 'api_validation_status': status, 'elements_count': count}


@pytest.fixture
def delivery(tmp_path, monkeypatch):
    store = DeliveryStore(tmp_path / 'delivery.sqlite3')
    monkeypatch.setattr(sync, '_delivery', store)
    monkeypatch.setattr(sync, '_counter_id', 123)
    monkeypatch.setattr(sync, '_enabled', True)
    return store


def row(**kwargs):
    return dict(id=42, order_status='PAID', revenue=5000, client_ids='private-client',
                create_date_time='2026-11-01 10:00:00',
                update_date_time='2026-11-01 11:00:00', **kwargs)


def test_metadata_survives_restart_and_is_counter_scoped(tmp_path):
    path = tmp_path / 'store.sqlite3'
    a = DeliveryStore(path)
    a.put(1, 42, 43, 'digest', 'pending', 'u1', 1)
    b = DeliveryStore(path)
    assert b.get(1, 42)['lead_id'] == 43
    assert b.pending(2) == []
    assert b.get(2, 42) is None
    b.confirm(1, 42, 'old-upload')
    assert b.pending(1)
    b.confirm(1, 42, 'u1')
    assert b.pending(1) == []


@pytest.mark.parametrize('value', [None, {}, receipt(status='FAILED'),
    receipt(uid='other'), receipt(count=0), receipt(status='PROCESSING')])
def test_only_exact_passed_receipt_confirms(value):
    assert not sync._validated(value, 'u1')


def test_http_success_without_validation_remains_pending(delivery, monkeypatch):
    upload = AsyncMock(return_value={'uploading': receipt(status='PROCESSING')})
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    assert asyncio.run(sync._deliver(row(), 42)) == 'pending'
    assert delivery.pending(123)
    assert asyncio.run(sync._deliver(row(), 42)) == 'pending'
    assert upload.await_count == 1
    assert b'private-client' not in delivery.path.read_bytes()


def test_failed_response_never_enters_dedup_cache(delivery, monkeypatch):
    monkeypatch.setattr(client, 'upload_simple_order', AsyncMock(return_value={
        'uploading': receipt(status='FAILED')}))
    with pytest.raises(client.MetrikaError):
        asyncio.run(sync._deliver(row(), 42))
    assert delivery.get(123, 42)['state'] == 'failed'


def test_transport_failure_persisted_before_network(delivery, monkeypatch):
    async def fail(*args):
        assert delivery.get(123, 42)['state'] == 'pending'
        raise client.MetrikaError('network failed')
    monkeypatch.setattr(client, 'upload_simple_order', fail)
    with pytest.raises(client.MetrikaError):
        asyncio.run(sync._deliver(row(), 42))
    assert delivery.pending(123)[0]['uploading_id'] is None


def test_missing_receipt_id_not_success(delivery, monkeypatch):
    monkeypatch.setattr(client, 'upload_simple_order', AsyncMock(return_value={}))
    with pytest.raises(client.MetrikaError):
        asyncio.run(sync._deliver(row(), 42))
    assert delivery.pending(123)


def test_validated_state_dedups_across_restart_but_changed_date_does_not(delivery, monkeypatch):
    upload = AsyncMock(return_value={'uploading': receipt()})
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    assert asyncio.run(sync._deliver(row(), 42)) == 'validated'
    monkeypatch.setattr(sync, '_delivery', DeliveryStore(delivery.path))
    assert asyncio.run(sync._deliver(row(), 42)) == 'unchanged'
    newer = row()
    newer['update_date_time'] = '2026-11-01 12:00:00'
    assert asyncio.run(sync._deliver(newer, 42)) == 'validated'
    assert upload.await_count == 2


def test_stale_pending_retries_fresh_state_same_order_id(delivery, monkeypatch):
    delivery.put(123, 42, 42, 'old-digest', 'pending', None, 0)
    upload = AsyncMock(return_value={'uploading': receipt()})
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    current = row()
    current.update(order_status='CANCELLED', revenue=0)
    assert asyncio.run(sync._deliver(current, 42)) == 'validated'
    assert upload.call_args.args[1] == current


def test_pending_receipt_ack_does_not_ack_newer_upload(delivery, monkeypatch):
    delivery.put(123, 42, 42, 'digest', 'pending', 'new', 0)
    monkeypatch.setattr(client, 'get_upload_receipts', AsyncMock(return_value={'old': receipt('old')}))
    asyncio.run(sync._check_pending_receipts())
    assert delivery.pending(123)


def test_ack_pending_receipt(delivery, monkeypatch):
    delivery.put(123, 42, 42, 'digest', 'pending', 'u1', 0)
    monkeypatch.setattr(client, 'get_upload_receipts', AsyncMock(return_value={'u1': receipt()}))
    asyncio.run(sync._check_pending_receipts())
    assert delivery.get(123, 42)['state'] == 'validated'


def test_receipt_lookup_preserves_representative_context_and_exact_id():
    async def run():
        seen = []
        def handler(request):
            seen.append(request)
            return httpx.Response(200, json={'uploadings': [receipt('other'), receipt('u1')]})
        c = client.MetrikaClient('https://api-metrika.yandex.net', 'secret', 'represented')
        c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            assert await c.get_upload_receipts(123, {'u1'}) == {'u1': receipt('u1')}
            assert seen[0].url.params['ulogin'] == 'represented'
            assert seen[0].url.params['source'] == 'API'
            assert seen[0].headers['Authorization'] == 'OAuth secret'
        finally:
            await c.aclose()
    asyncio.run(run())


def test_receipt_lookup_pages_without_confirming_missing_ids():
    async def run():
        calls = []
        def handler(request):
            calls.append(request)
            rows = [dict(receipt(str(i)), datetime='2026-11-02 10:00:00') for i in range(1000)]
            return httpx.Response(200, json={'uploadings': rows if len(calls) == 1 else [receipt('wanted')]})
        c = client.MetrikaClient('https://api-metrika.yandex.net', 'secret')
        c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            assert 'wanted' in await c.get_upload_receipts(123, {'wanted', 'missing'})
            assert len(calls) == 2
            assert calls[1].url.params['datetime_offset'] == '2026-11-02 10:00:00'
        finally:
            await c.aclose()
    asyncio.run(run())


def setup_lead(monkeypatch, pipeline, status):
    lead = dict(id=42, pipeline_id=pipeline, status_id=status, created_at=1793000000,
                updated_at=1793000100, price=4900)
    monkeypatch.setattr(sync, 'METRIKA_SINCE_TS', None)
    monkeypatch.setattr(sync.migration_freeze, 'skip', AsyncMock(return_value=False))
    monkeypatch.setattr(sync, 'is_tangemshop_lead', lambda ld: False)
    monkeypatch.setattr(sync, '_cf', lambda ld, field: '100' if field == sync.FIELD_SITE_ORDER_NUMBER else '')
    monkeypatch.setattr(sync, '_contact_info', AsyncMock(return_value=(123, None, None)))
    return lead


def test_waitlist_cancellation_requires_verified_retail_history(delivery, monkeypatch):
    lead = setup_lead(monkeypatch, sync.PIPELINE_WAITLIST, sync.STATUS_CLOSED_LOST)
    monkeypatch.setattr(sync.amo_service, '_do_get', AsyncMock(return_value={'_embedded': {'events': [
        {'entity_id': 42, 'value_before': [{'lead_status': {'pipeline_id': sync.PIPELINE_CLEVER_MAIN}}]}]}}))
    upload = AsyncMock(return_value={'uploading': receipt()})
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    assert asyncio.run(sync.process_sync({'lead_id': 42}, lead)) == 'validated'
    sent = upload.call_args.args[1]
    assert sent['id'] == 42 and sent['order_status'] == 'CANCELLED' and sent['revenue'] == 0


@pytest.mark.parametrize('data', [{}, {'_embedded': {'events': [{'entity_id': 99,
    'value_before': [{'lead_status': {'pipeline_id': sync.PIPELINE_CLEVER_MAIN}}]}]}}])
def test_other_waitlist_orders_are_not_exported(delivery, monkeypatch, data):
    lead = setup_lead(monkeypatch, sync.PIPELINE_WAITLIST, sync.STATUS_CLOSED_LOST)
    monkeypatch.setattr(sync.amo_service, '_do_get', AsyncMock(return_value=data))
    upload = AsyncMock()
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    assert asyncio.run(sync.process_sync({'lead_id': 42}, lead)) is None
    upload.assert_not_awaited()


def test_tangemshop_is_excluded_even_with_delivery_history(delivery, monkeypatch):
    lead = setup_lead(monkeypatch, sync.PIPELINE_WAITLIST, sync.STATUS_CLOSED_LOST)
    delivery.put(123, 42, 42, 'old', 'validated', 'old-upload', 0)
    monkeypatch.setattr(sync, 'is_tangemshop_lead', lambda ld: True)
    upload = AsyncMock()
    monkeypatch.setattr(client, 'upload_simple_order', upload)
    asyncio.run(sync.process_sync({'lead_id': 42}, lead))
    upload.assert_not_awaited()


def test_reconcile_retries_outside_window_and_does_not_write_other_systems(delivery, monkeypatch):
    import woo_status_sync
    import insales_status_sync
    # A previous attempt predates the 14-day window; ack it before refreshing CRM.
    delivery.put(123, 42, 42, 'old', 'pending', 'u1', 0)
    monkeypatch.setattr(client, 'get_upload_receipts', AsyncMock(return_value={'u1': receipt()}))
    monkeypatch.setattr(sync.amo_service, 'get_leads_updated_since', AsyncMock(return_value=[]))
    lead = dict(id=42, pipeline_id=sync.PIPELINE_WAITLIST, status_id=sync.STATUS_CLOSED_LOST)
    read = AsyncMock(return_value=lead)
    monkeypatch.setattr(sync.amo_service, 'get_lead_full', read)
    process = AsyncMock(return_value='failed')
    monkeypatch.setattr(sync, 'process_sync', process)
    monkeypatch.setattr(sync, '_last_reconcile_ts', 12345)
    monkeypatch.setattr(sync, '_reconcile_lock', asyncio.Lock())
    for module in (woo_status_sync, insales_status_sync):
        monkeypatch.setattr(module, 'is_enabled', lambda: True)
    woo = AsyncMock()
    insales = AsyncMock()
    monkeypatch.setattr(woo_status_sync, 'process_sync', woo)
    monkeypatch.setattr(insales_status_sync, 'process_lead', insales)
    asyncio.run(sync.reconcile_window())
    read.assert_awaited_once()
    process.assert_awaited_once()
    assert sync._last_reconcile_ts == 12345
    assert sync._last_run['outcomes'] == {'failed': 1}
    woo.assert_not_awaited()
    insales.assert_not_awaited()


def test_waitlist_fetch_failure_keeps_window_and_finishes_health(delivery, monkeypatch):
    async def fetch(pipeline, *args, **kwargs):
        if pipeline == sync.PIPELINE_WAITLIST:
            raise RuntimeError('source unavailable')
        return []
    monkeypatch.setattr(sync.amo_service, 'get_leads_updated_since', fetch)
    monkeypatch.setattr(sync, '_last_reconcile_ts', 12345)
    monkeypatch.setattr(sync, '_reconcile_lock', asyncio.Lock())
    asyncio.run(sync.reconcile_window())
    assert sync._last_reconcile_ts == 12345
    assert sync._last_run['fetch_failed'] is True
    assert sync._last_run['running'] is False


def test_canonical_lookup_failure_cannot_create_another_order_id(delivery, monkeypatch):
    monkeypatch.setattr(sync, '_cf', lambda *args: 'ms-uuid')
    monkeypatch.setattr(sync.amo_service, 'find_leads_by_query', AsyncMock(return_value=None))
    with pytest.raises(client.MetrikaError):
        asyncio.run(sync._resolve_clever({'id': 42}))
