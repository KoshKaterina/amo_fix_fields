#!/usr/bin/env bash
set -euo pipefail

# Exact rollback before cutover: current v7/nginx were never changed.
REMOTE="${ACADEMY_DEPLOY_REMOTE:-root@85.193.91.169}"
NEW="amo-academy-webhook-v8-history-safe"
ssh "$REMOTE" bash -s -- "$NEW" <<'REMOTE_ROLLBACK'
set -euo pipefail
NEW="$1"
if docker inspect "$NEW" >/dev/null 2>&1; then
  docker rm -f "$NEW"
fi
echo "Spare removed. Active v7 and nginx were not modified."
REMOTE_ROLLBACK

