import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch
import metrika_payment_evidence as m


class EvidenceTest(unittest.TestCase):
    def setUp(self):
        self.order = {"id": 100, "total": "5000.00", "status": "completed", "date_paid_gmt": "2026-11-01T12:00:00", "transaction_id": ""}
    def note(self, amount="5000", marker="amo-42-123", ts=1000):
        return {"id": 1,"created_at":ts,"params":{"text":f"Оплата подтверждена Ozon (webhook): {amount} ₽. Сделка уже не на этапе оплаты.\nextId {marker}"}}
    def test_completed_is_not_payment(self):
        self.assertIsNone(m.website_receipt(self.order))
    def test_transaction_and_payment_date_confirm_original_total(self):
        self.order['transaction_id']='tx'
        r=m.website_receipt(self.order)
        self.assertEqual(r.original_total,'5000.00')
        self.assertEqual(r.source,'woo_transaction')
    def test_refunded_transaction_not_paid(self):
        self.order.update(transaction_id='tx',status='refunded')
        self.assertIsNone(m.website_receipt(self.order))
    def test_transaction_without_paid_date_not_paid(self):
        self.order.update(transaction_id='tx',date_paid_gmt=None)
        self.assertIsNone(m.website_receipt(self.order))
    def test_manager_full_receipt_confirms_original_order(self):
        self.assertEqual(m.manager_receipt(self.order,[self.note('6374')],42).original_total,'5000.00')
    def test_manager_deposit_not_full_payment(self):
        self.assertIsNone(m.manager_receipt(self.order,[self.note('2500')],42))
    def test_duplicate_callback_does_not_double_deposit(self):
        self.assertIsNone(m.manager_receipt(self.order,[self.note('2500'),self.note('2500')],42))
    def test_two_distinct_receipts_cover_original(self):
        notes=[self.note('2500','amo-42-1'),self.note('2500','amo-42-2')]
        self.assertIsNotNone(m.manager_receipt(self.order,notes,42))
    def test_other_lead_receipt_rejected(self):
        self.assertIsNone(m.manager_receipt(self.order,[self.note(marker='amo-99-1')],42))
    def test_freeform_paid_text_not_confirmation(self):
        self.assertIsNone(m.manager_receipt(self.order,[{'params':{'text':'Клиент оплатил, закрыть'}}],42))
    def test_refunded_manager_receipt_rejected(self):
        self.order['status']='refunded'
        self.assertIsNone(m.manager_receipt(self.order,[self.note()],42))
    def test_receipt_survives_process_memory_and_is_scoped_to_original(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(m,'_STORE',Path(directory)/'evidence.sqlite3'):
            m.remember(42,100,m.Evidence('woo_transaction',1000,'5000'))
            with patch.object(m.amo_service,'get_custom_field_value',return_value='100'):
                self.assertEqual(m.cached({'id':42}).original_total,'5000')
                self.assertIsNone(m.cached({'id':99}))
            with patch.object(m.amo_service,'get_custom_field_value',return_value='101'):
                self.assertIsNone(m.cached({'id':42}))

    def test_nonfinite_and_negative_amounts_rejected(self):
        for x in ['NaN','Infinity','-2',None]:self.assertIsNone(m.money(x))


class SyncTest(unittest.IsolatedAsyncioTestCase):
    async def test_status_updated_on_same_id_without_woo_write(self):
        import metrika_sync as s
        lead={'id':42,'pipeline_id':s.PIPELINE_CLEVER_MAIN,'status_id':999,'created_at':1793000000,'updated_at':1793000100,'price':9900}
        evidence=m.Evidence('woo_transaction',1793000050,'5000.00')
        s._last_sent.clear()
        with patch.object(s,'_enabled',True), patch.object(s,'_counter_id',93845803), patch.object(s,'METRIKA_SINCE_TS',None), patch.object(s.migration_freeze,'skip',AsyncMock(return_value=False)), patch.object(s,'is_tangemshop_lead',return_value=False), patch.object(s,'_cf',side_effect=lambda ld,f:'онлайн'), patch.object(s,'_contact_info',AsyncMock(return_value=(123,None,None))), patch.object(s.metrika_payment_evidence,'for_lead',AsyncMock(return_value=evidence)), patch.object(s.metrika_client,'upload_simple_order',AsyncMock(return_value={})) as upload:
            await s.process_sync({'lead_id':42},lead)
            await s.process_sync({'lead_id':42},lead)
            self.assertEqual(upload.await_count,1)
            row=upload.call_args.args[1]
            self.assertEqual(row['id'],42)
            self.assertEqual(row['order_status'],'PAID')
            self.assertEqual(row['revenue'],'5000.00')
    async def test_cancelled_order_not_resurrected_by_receipt(self):
        import metrika_sync as s
        lead={'id':43,'pipeline_id':s.PIPELINE_CLEVER_MAIN,'status_id':s.STATUS_CLOSED_LOST,'created_at':1793000000,'updated_at':1793000100,'price':9900}
        s._last_sent.clear()
        with patch.object(s,'_enabled',True), patch.object(s,'_counter_id',93845803), patch.object(s,'METRIKA_SINCE_TS',None), patch.object(s.migration_freeze,'skip',AsyncMock(return_value=False)), patch.object(s,'is_tangemshop_lead',return_value=False), patch.object(s,'_cf',return_value='онлайн'), patch.object(s,'_contact_info',AsyncMock(return_value=(123,None,None))), patch.object(s.metrika_payment_evidence,'cached',return_value=m.Evidence('woo_transaction',1000,'5000')), patch.object(s.metrika_payment_evidence,'for_lead',AsyncMock()) as evidence, patch.object(s.metrika_client,'upload_simple_order',AsyncMock(return_value={})) as upload:
            await s.process_sync({'lead_id':43},lead)
            evidence.assert_not_awaited()
            self.assertEqual(upload.call_args.args[1]['order_status'],'CANCELLED')
            self.assertEqual(upload.call_args.args[1]['revenue'],0)

if __name__=='__main__':unittest.main()
