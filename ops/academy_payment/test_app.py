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
        order = body["order"]
        self.assertEqual(order["mode"], "MODE_SHORTENED")
        self.assertEqual(order["amount"]["value"], "1000000")
        self.assertEqual(order["extId"], self.row()["order_ext_id"])
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
        self.assertNotEqual(a["order"]["extId"], b["order"]["extId"])

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

    async def test_checkout_without_qr_or_qr_without_checkout_is_rejected(self):
        for value in ({"order": {"item": {"payLink": "https://checkout.ozon.ru/order/test"}}},
                      {"paymentDetails": {"paymentId": "p", "sbp": {"payload": "https://qr.nspk.ru/TEST"}}}):
            self.mock.return_value = value
            async with self.new_client() as other:
                headers = await self.enter(other)
                r = await other.post(module.PREFIX + "/create", headers=headers)
                self.assertEqual(r.status_code, 502)

    async def test_phone_link_is_checkout_and_desktop_gets_qr(self):
        headers = await self.enter()
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        d = r.json()
        self.assertEqual(d["url"], "https://checkout.ozon.ru/pay/test")
        self.assertTrue(d["qr"].startswith('<svg xmlns="http://www.w3.org/2000/svg"'))
        self.assertIn('aria-label="QR-код для оплаты"', d["qr"])
        self.assertNotIn("qr.nspk.ru", d["qr"])  # the code is drawn, the link itself is not in the page
        row = self.row()
        self.assertEqual((row["with_order"], row["qr_payload"], row["pay_link"]),
                         (1, "https://qr.nspk.ru/TEST?type=02&sum=1000000&cur=RUB", "https://checkout.ozon.ru/pay/test"))
        # reopening the page reuses the same bill and answer
        r2 = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r2.json(), d)
        self.mock.assert_awaited_once()

    async def test_qr_and_checkout_links_are_validated(self):
        self.assertEqual(module.safe_payload({"paymentDetails": {"sbp": {"payload": "https://qr.nspk.ru/OPAQUE"}}}), "https://qr.nspk.ru/OPAQUE")
        for url in ("https://qr.nspk.ru/T?type=02&sum=100&cur=RUB", "https://qr.nspk.ru.evil.invalid/T?type=02&sum=1000000&cur=RUB", "https://qr.nspk.ru/T?type=01&sum=1000000&cur=RUB"):
            with self.assertRaises(ValueError):
                module.safe_payload({"paymentDetails": {"sbp": {"payload": url}}})
        self.assertEqual(module.safe_checkout({"order": {"payLink": "https://checkout.ozon.ru/order/x"}}), "https://checkout.ozon.ru/order/x")
        for url in ("http://checkout.ozon.ru/order/x", "https://checkout.ozon.ru.evil.invalid/x", "https://user@checkout.ozon.ru/x", "https://checkout.ozon.ru/", "https://qr.nspk.ru/T"):
            with self.assertRaises(ValueError):
                module.safe_checkout({"order": {"item": {"payLink": url}}})

    async def test_legacy_unpaid_session_gets_new_cookie(self):
        for name in module.LEGACY_COOKIES:
            async with self.new_client() as c:
                headers = await self.enter(c)
                await c.post(module.PREFIX + "/create", headers=headers)
                old = c.cookies[module.COOKIE]
                with module.db() as conn:
                    conn.execute("UPDATE payments SET pay_link='https://qr.nspk.ru/OLD',with_order=0 WHERE session=?", (module.session_hash(old),))
                c.cookies.clear()
                c.cookies.set(name, old, domain="team.sunscrypt.ru", path=module.PREFIX)
                await self.enter(c)
                self.assertNotEqual(c.cookies[module.COOKIE], old)

    async def test_legacy_paid_session_stays_paid(self):
        for name in module.LEGACY_COOKIES:
            async with self.new_client() as c:
                headers = await self.enter(c)
                await c.post(module.PREFIX + "/create", headers=headers)
                old = c.cookies[module.COOKIE]
                await c.post(module.PREFIX + "/notify", json=self.signed(module.payment(old)))
                c.cookies.clear()
                c.cookies.set(name, old, domain="team.sunscrypt.ru", path=module.PREFIX)
                r = await c.get(module.PREFIX + "/deposit-10000")
                self.assertIn("Предоплата получена", r.text)
                self.assertEqual(c.cookies[module.COOKIE], old)

    async def test_pending_sbp_only_bill_is_not_handed_out(self):
        headers = await self.issue()
        with module.db() as conn:
            conn.execute("UPDATE payments SET pay_link='https://qr.nspk.ru/OLD',with_order=0,qr_payload=NULL")
        r = await self.client.post(module.PREFIX + "/create", headers=headers)
        self.assertEqual(r.status_code, 409)
        self.mock.assert_awaited_once()

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
        self.assertEqual(r.headers["Location"], "https://checkout.ozon.ru/pay/test")
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


    # --- 26.11.2026: confirmation by getOrderStatus, desktop QR, phone to Ozon page --------

    def order(self, **over):
        d = {"id": "ozon-order", "extId": self.row()["order_ext_id"], "status": "STATUS_PAID", "isTestMode": False,
             "originalAmount": {"currencyCode": "643", "value": "1000000"}}
        d.update(over)
        return d

    async def test_status_asks_ozon_order_and_marks_paid(self):
        await self.issue()
        status = AsyncMock(return_value=self.order())
        with patch.object(module, "order_status_ozon", status):
            self.assertTrue((await self.client.get(module.PREFIX + "/status")).json()["paid"])
            self.assertTrue((await self.client.get(module.PREFIX + "/status")).json()["paid"])
        status.assert_awaited_once_with(self.row()["order_ext_id"])
        self.assertEqual(self.row()["status"], "paid")
        r = await self.client.get(module.PREFIX + "/deposit-10000")
        self.assertIn("Предоплата получена", r.text)

    async def test_status_not_paid_for_foreign_test_or_wrong_amount(self):
        await self.issue()
        for over in ({"status": "STATUS_PAYMENT_PENDING"}, {"extId": "DEFI4-10000_foreign"}, {"isTestMode": True},
                     {"originalAmount": {"currencyCode": "643", "value": "100"}},
                     {"originalAmount": {"currencyCode": "840", "value": "1000000"}}, {"id": ""}):
            with module.db() as conn:
                conn.execute("UPDATE payments SET checked_at=NULL")
            with patch.object(module, "order_status_ozon", AsyncMock(return_value=self.order(**over))):
                self.assertFalse((await self.client.get(module.PREFIX + "/status")).json()["paid"], over)
        self.assertEqual(self.row()["status"], "pending")

    async def test_status_asks_bank_at_most_every_ten_seconds_and_survives_errors(self):
        await self.issue()
        status = AsyncMock(side_effect=httpx.ConnectError("down"))
        with patch.object(module, "order_status_ozon", status):
            for _ in range(5):
                r = await self.client.get(module.PREFIX + "/status")
                self.assertEqual((r.status_code, r.json()["paid"]), (200, False))
        status.assert_awaited_once()

    async def test_page_and_script_have_no_sbp_word_and_split_desktop_phone(self):
        r = await self.client.get(module.PREFIX + "/deposit-10000")
        self.assertNotIn("СБП", r.text)
        self.assertIn('<section id="qr" class="box" hidden', r.text)
        # Оформление как у своей страницы оплаты amo: логотип, блок, реквизиты получателя.
        self.assertIn('aria-label="Sunscrypt"', r.text)
        self.assertIn('<div id="paid" class="box paid" hidden>', r.text)
        self.assertIn("ИНН 773423812500", r.text)
        self.assertEqual(r.text.count("Предоплата 10&nbsp;000&nbsp;₽</h1>"), 1)
        self.assertNotIn('id="start-title">Предоплата 10 000 ₽<', r.text)
        self.assertIn("Наведите камеру телефона на QR-код или отсканируйте его в приложении банка.", r.text)
        js = (await self.client.get(module.PREFIX + "/payment.js")).text
        self.assertIn("matchMedia('(hover:hover) and (pointer:fine)')", js)
        self.assertIn("if(desktop&&d.qr){showQr(d.qr);return;}", js)
        self.assertIn("window.location.replace(d.url)", js)
        self.assertIn("visibilitychange", js)

    def test_embedded_qr_generator_matches_pay_qr(self):
        import sys
        parents = Path(__file__).resolve().parents
        root = parents[2] if len(parents) > 2 else parents[-1]
        if not (root / "pay_qr.py").exists():
            self.skipTest("pay_qr.py рядом нет (проверка в образе без репозитория)")
        sys.path.insert(0, str(root))
        try:
            import pay_qr
        finally:
            sys.path.remove(str(root))
        for data in ("https://qr.nspk.ru/AD10107AGA7BIIL69PAA6FVRC08GR4LE", "https://qr.nspk.ru/" + "A1" * 120, ""):
            self.assertEqual(module.qr_matrix(data), pay_qr.matrix(data))
            self.assertEqual(module.qr_svg(data, "t"), pay_qr.svg(data, "t"))


if __name__ == "__main__":
    unittest.main()
