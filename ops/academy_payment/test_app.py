"""Payment invariants, without external APIs or production credentials."""
import asyncio
import hashlib
import importlib.util
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

spec = importlib.util.spec_from_file_location("academy_payment_app", Path(__file__).with_name("app.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PaymentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        module.DB_PATH = Path(self.tmp.name) / "payments.sqlite3"
        module.ACCESS, module.SECRET = "test-access", "test-secret"
        module.NOTIFY_SECRET, module.SESSION_SECRET = "test-notify", "test-session"
        module.lock = asyncio.Lock()
        module.init_db()
        self.client = self.new_client()
        self.mock = AsyncMock(return_value={"order": {"item": {"payLink": "https://checkout.ozon.ru/pay/test"}}, "paymentDetails": {"paymentId": "test-id", "sbp": {"payload": "https://qr.nspk.ru/TEST?type=02&sum=1000000&cur=RUB"}}})
        self.patch = patch.object(module, "create_ozon", self.mock)
        self.patch.start()

    def new_client(self):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=module.app), base_url=module.ORIGIN)

    async def asyncTearDown(self):
        self.patch.stop()
        await self.client.aclose()
        self.tmp.cleanup()

    async def enter(self, client=None):
        client = client or self.client
        r = await client.get(module.PREFIX + "/deposit-10000")
        token = re.search(r'<meta name="csrf" content="([a-f0-9]+)"', r.text).group(1)
        return {"Origin": module.ORIGIN, "X-Academy-CSRF": token}

    async def issue(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 200)
        return headers

    def row(self):
        return module.payment(self.client.cookies[module.COOKIE])

    def signed(self, row, amount="1000000", status="Completed", attempt=False):
        d = {"amount": amount, "currencyCode": "643", "status": status,
             "extTransactionID": row["ext_id"], "extOrderID": row["order_ext_id"],
             "orderID": "bank-order", "transactionID": "bank-transaction"}
        string = (f"{module.ACCESS}|bank-order|bank-transaction|{row['order_ext_id']}|{amount}|643|{module.NOTIFY_SECRET}"
                  if attempt else f"{module.ACCESS}|||{row['ext_id']}|{amount}|643|{module.NOTIFY_SECRET}")
        d["requestSign"] = hashlib.sha256(string.encode()).hexdigest()
        return d

    async def test_preview_get_does_not_issue_invoice(self):
        await self.enter()
        self.mock.assert_not_awaited()
        self.assertIsNone(self.row())

    async def test_amount_is_fixed_despite_client_body(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers, json={"amount": 1})
        self.assertEqual(r.status_code, 200)
        body = self.mock.call_args.args[0]
        self.assertEqual(body["amount"]["value"], "1000000")
        self.assertEqual(body["payType"], "SBP")
        self.assertNotIn("order", body)
        self.assertNotIn("amo-", body["extId"])

    async def test_refresh_and_concurrent_requests_reuse_one_bill(self):
        headers = await self.enter()
        rs = await asyncio.gather(*[self.client.post(module.PREFIX + "/create", headers=headers) for _ in range(3)])
        self.assertEqual([r.status_code for r in rs], [200, 200, 200])
        self.mock.assert_awaited_once()

    async def test_distinct_visitors_get_distinct_bills(self):
        await self.issue()
        async with self.new_client() as other:
            headers = await self.enter(other)
            r = await other.post(module.PREFIX + "/create", headers=headers)
            self.assertEqual(r.status_code, 200)
        a, b = [call.args[0] for call in self.mock.call_args_list]
        self.assertNotEqual(a["extId"], b["extId"])
        self.assertNotIn("order", a)
        self.assertNotIn("order", b)

    async def test_foreign_origin_and_wrong_csrf_fail_before_bank(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers={**headers, "Origin": "https://evil.invalid"})
        self.assertEqual(r.status_code, 403)
        r = await self.client.post(module.PREFIX + "/create", headers={**headers, "X-Academy-CSRF": "bad"})
        self.assertEqual(r.status_code, 403)
        self.mock.assert_not_awaited()

    async def test_timeout_is_durable_and_never_retried(self):
        self.mock.side_effect = httpx.ReadTimeout("ambiguous")
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 502)
        module.init_db()  # simulated restart, state survives
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 409)
        self.mock.assert_awaited_once()

    async def test_untrusted_redirect_is_rejected(self):
        self.mock.return_value = {"order": {"payLink": "https://evil.invalid/"}}
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 502)

    async def test_checkout_without_sbp_is_never_a_fallback(self):
        self.mock.return_value = {"order": {"item": {"payLink": "https://checkout.ozon.ru/order/test"}}}
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 502)

    async def test_sbp_wins_over_checkout_and_amount_is_encoded(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertTrue(r.json()["url"].startswith("https://qr.nspk.ru/"))
        self.assertEqual(module.safe_link({"paymentDetails": {"sbp": {"payload": "https://qr.nspk.ru/OPAQUE"}}}), "https://qr.nspk.ru/OPAQUE")
        for url in ("https://qr.nspk.ru/T?type=02&sum=100&cur=RUB", "https://qr.nspk.ru.evil.invalid/T?type=02&sum=1000000&cur=RUB", "https://qr.nspk.ru/T?type=01&sum=1000000&cur=RUB"):
            with self.assertRaises(ValueError):
                module.safe_link({"paymentDetails": {"sbp": {"payload": url}}})

    async def test_legacy_unpaid_checkout_session_gets_new_sbp_cookie(self):
        headers = await self.issue()
        old = self.client.cookies[module.COOKIE]
        with module.db() as conn:
            conn.execute("UPDATE payments SET pay_link='https://checkout.ozon.ru/order/old' WHERE session=?", (module.session_hash(old),))
        self.client.cookies.clear()
        self.client.cookies.set(module.LEGACY_COOKIE, old, domain="team.sunscrypt.ru", path=module.PREFIX)
        await self.enter()
        self.assertNotEqual(self.client.cookies[module.COOKIE], old)

    async def test_legacy_paid_session_stays_paid(self):
        await self.issue()
        old = self.client.cookies[module.COOKIE]
        await self.client.post(module.PREFIX + "/notify", json=self.signed(self.row()))
        self.client.cookies.clear()
        self.client.cookies.set(module.LEGACY_COOKIE, old, domain="team.sunscrypt.ru", path=module.PREFIX)
        r = await self.client.get(module.PREFIX + "/deposit-10000")
        self.assertIn("Предоплата получена", r.text)
        self.assertEqual(self.client.cookies[module.COOKIE], old)

    async def test_unsigned_and_wrong_amount_notifications_do_not_mark_paid(self):
        await self.issue()
        d = self.signed(self.row()); d["requestSign"] = "bad"
        self.assertEqual((await self.client.post(module.PREFIX + "/notify", json=d)).status_code, 401)
        d = self.signed(self.row(), amount="100")
        self.assertEqual((await self.client.post(module.PREFIX + "/notify", json=d)).status_code, 422)
        self.assertEqual(self.row()["status"], "pending")

    async def test_both_signed_notification_formats_are_idempotent(self):
        await self.issue()
        for attempt in (False, True):
            d = self.signed(self.row(), attempt=attempt)
            self.assertEqual((await self.client.post(module.PREFIX + "/notify", json=d)).status_code, 200)
        self.assertEqual(self.row()["status"], "paid")
        paid_at = self.row()["paid_at"]
        d = self.signed(self.row())
        await self.client.post(module.PREFIX + "/notify", json=d)
        self.assertEqual(self.row()["paid_at"], paid_at)

    async def test_unsigned_order_id_cannot_select_another_bill(self):
        await self.issue()
        d = self.signed({"ext_id": "foreign-payment", "order_ext_id": "foreign-order"})
        d["extOrderID"] = self.row()["order_ext_id"]
        self.assertEqual((await self.client.post(module.PREFIX + "/notify", json=d)).status_code, 200)
        self.assertEqual(self.row()["status"], "pending")

    async def test_return_url_never_claims_success_without_confirmation(self):
        await self.issue()
        r = await self.client.get(module.PREFIX + "/return")
        self.assertIn("Проверяем оплату", r.text)
        self.assertFalse((await self.client.get(module.PREFIX + "/status")).json()["paid"])

    async def test_paid_session_cannot_create_a_second_payment(self):
        headers = await self.issue()
        await self.client.post(module.PREFIX + "/notify", json=self.signed(self.row()))
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertTrue(r.json()["paid"])
        self.mock.assert_awaited_once()

    async def test_form_fallback_and_security_headers(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers={"Origin": module.ORIGIN}, data={"csrf": headers["X-Academy-CSRF"]})
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["Cache-Control"], "no-store")
        self.assertIn("frame-ancestors 'none'", r.headers["Content-Security-Policy"])

    async def test_rate_limit_is_persistent(self):
        now = int(module.time.time())
        with module.db() as conn:
            for i in range(300):
                conn.execute("INSERT INTO payments(session,ext_id,order_ext_id,status,created,ip_hash) VALUES(?,?,?,?,?,?)",
                             (str(i), f"ext-{i}", f"order-{i}", "pending", now, "other-ip"))
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 429)
        self.mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
