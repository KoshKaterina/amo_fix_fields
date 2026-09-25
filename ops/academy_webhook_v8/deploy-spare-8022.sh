#!/usr/bin/env bash
set -euo pipefail

# PREPARED ONLY: run manually from the repository root after the live whitelist
# sends have finished. This script never changes nginx and never stops v7.
REMOTE="${ACADEMY_DEPLOY_REMOTE:-root@85.193.91.169}"
OLD="amo-academy-webhook-v7-assignee"
NEW="amo-academy-webhook-v8-history-safe"
HOST_PORT="8022"
SHARED_HOST_DIR="/opt/integrations/amo_fix_fields/var/academy"
COMMIT="$(git rev-parse HEAD)"
OVERLAY="/opt/integrations/amo_fix_fields/deploy-overlays/academy-webhook-v8-${COMMIT}"
IMAGE="amo-fix-fields:academy-webhook-v8-${COMMIT}"

FILES=(
  academy_bothelp_upsert.py academy_intent_alert.py academy_invite_delivery.py
  academy_invite_link.py academy_webhook_app.py waybill_config.py webhooks.py
  test_academy_bothelp_upsert.py test_academy_intent_alert.py
  test_academy_invite_delivery.py test_academy_invite_link.py
)
for file in "${FILES[@]}"; do
  test -f "$file"
done
test -f ops/academy_webhook_v8/Dockerfile

# Code only. No env/secrets are copied through ssh or written to an env file.
tar -cf - "${FILES[@]}" ops/academy_webhook_v8/Dockerfile | \
  ssh "$REMOTE" "mkdir -p '$OVERLAY' && tar -xf - -C '$OVERLAY' --strip-components=0"

ssh "$REMOTE" bash -s -- "$OLD" "$NEW" "$IMAGE" "$OVERLAY" "$COMMIT" "$HOST_PORT" "$SHARED_HOST_DIR" <<'REMOTE_BUILD'
set -euo pipefail
OLD="$1"; NEW="$2"; IMAGE="$3"; OVERLAY="$4"; COMMIT="$5"; HOST_PORT="$6"; SHARED_HOST_DIR="$7"
test "$(docker inspect -f '{{.State.Running}}' "$OLD")" = "true"
if docker inspect "$NEW" >/dev/null 2>&1; then
  echo "Refusing: spare container already exists: $NEW" >&2
  exit 2
fi
BASE_ID="$(docker inspect -f '{{.Image}}' "$OLD")"
BASE_TAG="amo-fix-fields:academy-webhook-v8-base-${COMMIT}"
docker image tag "$BASE_ID" "$BASE_TAG"
docker build \
  --build-arg "BASE_IMAGE=$BASE_TAG" \
  --build-arg "SOURCE_COMMIT=$COMMIT" \
  -f "$OVERLAY/ops/academy_webhook_v8/Dockerfile" \
  -t "$IMAGE" "$OVERLAY"
mkdir -p "$SHARED_HOST_DIR"

# Union the legacy ledgers after the live whitelist batch. Sources are preserved;
# destination is replaced atomically and never truncated to a subset.
python3 - "$SHARED_HOST_DIR/academy_invite_sent.json" \
  /opt/integrations/amo_fix_fields/var/academy_invite_sent.json \
  "$SHARED_HOST_DIR/academy_invite_sent.json" <<'PY'
import json, os, pathlib, sys, tempfile
destination = pathlib.Path(sys.argv[1])
values = set()
for raw in sys.argv[2:]:
    path = pathlib.Path(raw)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        continue
    if not isinstance(data, list):
        raise SystemExit(f"invalid legacy ledger format: {path}")
    values.update(str(item) for item in data)
destination.parent.mkdir(parents=True, exist_ok=True)
fd, tmp = tempfile.mkstemp(prefix=destination.name + ".", dir=destination.parent)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(sorted(values), stream, ensure_ascii=False)
        stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, destination)
finally:
    if os.path.exists(tmp): os.unlink(tmp)
PY

# Clone runtime Env in memory through the Docker Engine API. Secrets are neither
# printed nor written to a temporary env file. Only non-secret Academy overrides
# are changed for the spare.
python3 - "$OLD" "$NEW" "$IMAGE" "$HOST_PORT" "$SHARED_HOST_DIR" <<'PY'
import http.client, json, socket, sys, urllib.parse

old_name, new_name, image, host_port, shared = sys.argv[1:]

class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self): super().__init__("localhost")
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")

def request(method, path, body=None):
    conn = UnixHTTPConnection()
    payload = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    conn.request(method, path, body=payload, headers=headers)
    response = conn.getresponse()
    data = response.read()
    if response.status >= 300:
        raise SystemExit(f"Docker API {method} {path}: HTTP {response.status}")
    return json.loads(data) if data else {}

old = request("GET", f"/containers/{urllib.parse.quote(old_name, safe='')}/json")
env = list(old["Config"].get("Env") or [])
overrides = {
    "ACADEMY_INVITE_SEND_ENABLED": "1",
    "ACADEMY_INVITE_WAZZUP_CHANNEL_ID": "782075b4-137e-43b2-839e-8ff21232d7df",
    "ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID": "79250833349",
    "ACADEMY_INVITE_OUTBOX_PATH": "/app/var/academy_invite_outbox.sqlite3",
    "ACADEMY_INVITE_HISTORY_REVIEW_PATH": "/app/var/academy_invite_history_reviews.json",
    "ACADEMY_INVITE_SENT_PATH": "/app/var/academy_invite_sent.json",
    "ACADEMY_INVITE_HISTORY_START_AT": "2017-01-01T00:00:00.000Z",
}
for key, value in overrides.items():
    env = [item for item in env if not item.startswith(key + "=")]
    env.append(f"{key}={value}")
config = {
    "Image": image,
    "Env": env,
    "Cmd": old["Config"].get("Cmd"),
    "Entrypoint": old["Config"].get("Entrypoint"),
    "WorkingDir": old["Config"].get("WorkingDir") or "/app",
    "User": old["Config"].get("User") or "",
    # Do not clone Compose ownership labels: the spare is intentionally outside
    # the current service lifecycle until an explicit cutover.
    "Labels": {"sunscrypt.spare": "8022", "sunscrypt.release": "v8-history-safe"},
    "ExposedPorts": {"8000/tcp": {}},
    "HostConfig": {
        "Binds": [f"{shared}:/app/var:rw"],
        "PortBindings": {"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": host_port}]},
        "RestartPolicy": {"Name": "unless-stopped", "MaximumRetryCount": 0},
        "NetworkMode": old["HostConfig"].get("NetworkMode") or "bridge",
    },
}
created = request("POST", f"/containers/create?name={urllib.parse.quote(new_name, safe='')}", config)
request("POST", f"/containers/{created['Id']}/start")
PY

# Readiness and import-only checks. No webhook POST and no client message.
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${HOST_PORT}/health" >/dev/null; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:${HOST_PORT}/health"
docker exec "$NEW" python3 -m py_compile \
  /app/academy_invite_delivery.py /app/academy_invite_link.py \
  /app/academy_bothelp_upsert.py /app/academy_webhook_app.py
docker exec "$NEW" python3 -m pytest -q \
  /app/test_academy_invite_delivery.py /app/test_academy_bothelp_upsert.py \
  /app/test_academy_invite_link.py /app/test_academy_intent_alert.py
docker inspect -f 'name={{.Name}} running={{.State.Running}} image={{.Config.Image}} mounts={{range .Mounts}}{{.Source}}:{{.Destination}};{{end}}' "$NEW"
docker logs --since 2m "$NEW" 2>&1 | tail -100
echo "SPARE READY ONLY. nginx/current v7 unchanged."
REMOTE_BUILD
