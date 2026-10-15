# Academy deduplication guard

Production-only guard for open deals in amoCRM pipeline `8642410` (Academy).

- Primary trigger: dedicated amoCRM `add_lead` / `update_lead` webhook. The server queues only lead IDs, waits 60 seconds for Wazzup/BotHelp fields, then starts one deduplication cycle. Repeated events and cycles are coalesced within the same 60-second window.
- Fallback: one full audit per day via `academy-dedup.timer` at about 03:30 UTC; it is no longer a five-minute poll.
- Batch limit: 10 duplicate groups per cycle.
- Identity keys: contact ID, normalized phone, BotHelp ID, Telegram ID, or normalized Telegram username (leading `@` ignored).
- Scope: only open deals inside Academy. Closed deals (`142`, `143`) and other pipelines are never merged.
- Survivor: `Не трогать этих клиентов!!!` first, otherwise the rightmost stage; then human-work score and oldest card.
- Responsible during a merge: Artem wins if present; otherwise an existing Academy manager is preserved; two Academy managers or an outside owner are resolved deterministically within the Academy team.
- Webinar rule: if any merged deal was at `Оставил заявку с Веба`, the survivor gets tag `оставил заявку с веба` and an open task with the same text for its responsible owner; `Присутствовали на вебе` adds tag `был на вебе 1`.
- Safety stops: any MoySklad order or payment link, a deal younger than 60 seconds, identity mismatch, stage/pipeline drift, or API failure.
- Audit: each attempted group has preflight backup, progress, result and post-verification under `/var/lib/academy-dedup/runs/`. Post-verification checks every preserved custom field, linked contact, stage, chosen owner, and normalized deal name.
- OAuth token is read from `/opt/integrations/amo_fix_fields/.env` and is passed through the process environment into a curl variable, never in process arguments.
- Webhook receiver: `academy-dedup-webhook.service`, localhost `127.0.0.1:8031`; nginx exposes one secret exact path with access logging disabled. The amoCRM subscription is recorded at `/var/lib/academy-dedup/webhook-subscription.json`.
- Any failed cycle or webhook receiver crash triggers a throttled Telegram alert for `@koshkatarina` through `academy-dedup-alert@.service`.

Operational commands:

```sh
systemctl status academy-dedup.timer
systemctl start academy-dedup.service
journalctl -u academy-dedup.service --since today
```

Deployment completed on 2026-10-08. Initial normalized audit found 40 missed username pairs; all 40 were merged and the final live audit returned zero open duplicate groups.
