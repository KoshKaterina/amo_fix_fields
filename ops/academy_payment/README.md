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
the existing `ozon_invoice.py` and Woo `sunscrypt-sbp` modules. Since 26.11.2026 (see below)
the request creates an SBP payment together with a shortened Ozon order (MODE_SHORTENED,
amount fixed at 1000000, payType SBP). The response must carry both links:
`order.item.payLink` — HTTPS on exactly `checkout.ozon.ru` (phones are sent there), and
`paymentDetails.sbp.payload` — HTTPS on exactly `qr.nspk.ru`; if amount/currency/type query
parameters are present they must be 1000000/RUB/02 (opaque NSPK URLs without parameters are
accepted: the amount is fixed in the authenticated request). The NSPK link is never shown as
a link: the desktop page draws it as a QR code. Missing or foreign links fail the bill (502),
nothing is retried automatically. Signed callbacks must confirm the exact issued invoice,
amount and currency. A self-integration signature does not cover `extOrderID`, so that variant
selects bills by `extTransactionID` only. Return navigation never constitutes proof of payment.

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
`academy_sbp_only_session` so previously cached checkout URLs cannot be
reused. Confirmed paid legacy sessions retain their paid guard; pending
legacy sessions get a new SBP invoice. Legacy checkout invoices stay in the
audit database and their callbacks remain recognized. Previously opened
bank checkout windows were not revoked by this code update: close them and
reopen the common Team URL.

Security maintenance (2026-10-22): use the tested sanitized CRM dependency image, with no .env backup files in its final filesystem or ancestor layers. Keep the payment app bind-mounted read-only, its existing UID65534 and current /data. JSON logs rotate at 10 MiB with five files. Dependency validation runs test_app.py with --network none, no production environment and temporary SQLite only. A live GET of the public HTML must not execute its JavaScript or POST /create; issuing a real payment is not a health check. Preserve the current payment ledger on rollback.

Desktop QR and Ozon payment page, 2026-11-26 (Katya's decision; supersedes the SBP-only
update of 2026-10-14). NSPK moved `qr.nspk.ru` to a Russian Trusted (Минцифры) certificate:
Chrome, Samsung Browser and Safari refuse to open it, so the SBP-only redirect stopped working.
Now, as with amo invoices and the site: a desktop browser (`(hover:hover) and (pointer:fine)`)
stays on this page and sees our QR code (generator embedded in `app.py`, a copy of `pay_qr.py`
from the repository root — the container mounts only this file and the image has no QR
library; `test_app.py` checks they match); phones and tablets go straight to the Ozon payment
page, where the buyer may also choose a card (higher fee, accepted). No word «СБП» is shown.
Confirmation: the signed callback as before, plus `GET /academy-pay/status` asks Ozon
`getOrderStatus` by the order extId at most once per 10 s per bill — only our order, not a
test, exactly 1000000 kopecks in 643/RUB, status `STATUS_PAID` counts as paid (the callback for
an Ozon order may not arrive; `getPaymentDetails` does not see such payments). The desktop
page polls every 4 s for 3 minutes, then every 10 s up to 20 minutes, and immediately when the
tab becomes visible again. The cookie was renamed to `academy_pay_session`, so pending SBP-only
bills get a new bill with an order; paid sessions under `academy_sbp_only_session` and
`academy_deposit_session` keep their paid guard. New columns (`qr_payload`, `with_order`,
`checked_at`) are added by an additive migration at start-up; old rows keep `with_order=0`.
Without JavaScript the form posts and is redirected (303) to the Ozon payment page.

Update: replace `/opt/academy-deposit-link/app.py` in place with `cp` (the file is bind-mounted;
`mv` would detach the mount) and `docker restart academy-deposit-link`. No image, nginx or
environment change. Package and rollback: workspace `tmp/deploy-academy-pay-qr/`.
