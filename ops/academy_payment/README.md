# Academy deposit payment redirect

One shared URL: `https://team.sunscrypt.ru/academy-pay/deposit-10000`.
The external Academy form collects contacts. This service collects no contacts,
does not create Woo orders, does not write CRM, and does not import CRM workers.

A browser receives an opaque HttpOnly cookie, then same-origin POST creates one
Ozon invoice for 1,000,000 kopecks (10,000 RUB). Preview GET requests cannot create
an invoice. Concurrent/repeated requests from the same browser reuse its pending
invoice. Different browsers get different payment/order IDs. Invoice attempts
persist in SQLite before the API call; uncertain API results are never retried
automatically. Pending invoices expire after 24 hours. Cookie isolation and rate
limits are persistent; rate limits are 20 invoices/IP/hour and 300 total/10 min.

Ozon `createPayment`, SHA256 signatures, amount units and shortened orders follow
the existing `ozon_invoice.py` and Woo `sunscrypt-sbp` modules. Payment redirect
URLs must be HTTPS on exactly `qr.nspk.ru`, with dynamic QR type 02,
amount 1000000 and currency RUB. Only paymentDetails.sbp.payload is used.
Ozon checkout/card/installment URLs are never a fallback. Signed callbacks must confirm
the exact issued invoice, amount and currency. A self-integration signature
does not cover `extOrderID`, so that variant selects bills by `extTransactionID`
only. Return navigation never constitutes proof of payment.

Fiscalization remains disabled, matching the existing shortened-order payment
modules. Receipt handling and matching external form contacts with bank payments
are outside this narrowly scoped redirect. Do not claim they are automated.

Runtime: isolated container `academy-deposit-link`, listening only on
`127.0.0.1:8040`; code `/opt/academy-deposit-link/app.py`, persistent data
`/opt/academy-deposit-link/data`. Required environment variables are the three
existing `OZON_PAY_*_KEY` names plus a fresh `ACADEMY_PAYMENT_SESSION_SECRET`.
Reuse only the named Ozon credentials from the running integration, never the
full CRM environment. Environment file must be root-only and excluded from Git.

Tests: `python3 -m unittest discover -s ops/academy_payment -p test_app.py`.

Deployment uses the already running integration image as a dependency runtime,
but overrides its workdir/entrypoint and never starts `webhooks.py`. Mount only
this module read-only and its isolated data directory. No CRM config/worker
changes or restarts are needed. Public routing is limited to `/academy-pay/`.

Rollback: remove the dedicated nginx include, validate and reload nginx, then
stop only `academy-deposit-link`. Preserve its SQLite data for reconciliation.

SBP-only update, 2026-10-14: cookie name changed to
`academy_sbp_deposit_session` so previously cached checkout URLs cannot be
reused. Confirmed paid legacy sessions retain their paid guard; pending
legacy sessions get a new SBP invoice. Legacy checkout invoices stay in the
audit database and their callbacks remain recognized. Previously opened
bank checkout windows were not revoked by this code update: close them and
reopen the common Team URL.
