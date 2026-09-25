ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG SOURCE_COMMIT
LABEL sunscrypt.component="academy-webhook" \
      sunscrypt.source_commit="${SOURCE_COMMIT}" \
      sunscrypt.release="v8-history-safe"

WORKDIR /app
COPY academy_bothelp_upsert.py academy_intent_alert.py academy_invite_delivery.py ./
COPY academy_invite_link.py academy_webhook_app.py waybill_config.py webhooks.py ./
COPY test_academy_bothelp_upsert.py test_academy_intent_alert.py ./
COPY test_academy_invite_delivery.py test_academy_invite_link.py ./

