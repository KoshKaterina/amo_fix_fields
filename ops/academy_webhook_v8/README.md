# Academy webhook v8 spare (8022)

Prepared deploy only. The scripts do not alter nginx, stop v7, or send a webhook/client message.

Run `deploy-spare-8022.sh` from the repository root only after the live whitelist sends are complete.
It builds an overlay from the exact current v7 image, clones runtime environment values in memory
through `/var/run/docker.sock`, unions (never truncates) the legacy sent ledgers, and starts the spare
on `127.0.0.1:8022`.

The runtime image intentionally has no pytest dependency. The deploy check installs pinned pytest
only into the spare container's ephemeral `/tmp/academy-test-run` and runs the four targeted suites
with that directory on `PYTHONPATH`; it does not modify the image or production site-packages.

The spare has sending enabled for exact Academy channel
`782075b4-137e-43b2-839e-8ff21232d7df` / `79250833349`, but remains fail-closed because production
currently has no Wazzup v2 `client_access_token` and the review file is not created automatically.
Only a completed official exact-channel export or a short-lived reviewed permit can clear delivery.

Before a later nginx switch, verify the same host files are used by main and webhook:

- main: `/app/var/academy/academy_invite_outbox.sqlite3` and review/legacy siblings;
- webhook: `/app/var/academy_invite_outbox.sqlite3` and review/legacy siblings.

If the spare is rejected before cutover, run `rollback-spare-8022.sh`; v7 and nginx require no rollback.
