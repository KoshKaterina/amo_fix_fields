"""Run on server 2 after copying a reviewed release into /opt/academy-deposit-link."""
import hashlib
import json
import os
import secrets
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path("/opt/academy-deposit-link")
NGINX = Path("/etc/nginx/sites-enabled/team-panel")
SNIPPET = Path("/etc/nginx/snippets/academy-deposit-link.conf")
BACKUP = Path("/opt/backups/academy-deposit-link-20261014")
INCLUDE = "    include /etc/nginx/snippets/academy-deposit-link.conf;\n"


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def main():
    ROOT.mkdir(mode=0o755, parents=True, exist_ok=True)
    if run("docker", "ps", "-a", "--filter", "name=^academy-deposit-link$", "--format", "{{.Names}}"):
        raise SystemExit("Dedicated payment container already exists; review before updating")
    env_file = ROOT / "payment.env"
    if env_file.exists():
        raise SystemExit("Payment configuration already exists; do not overwrite")
    names = ("OZON_PAY_ACCESS_KEY", "OZON_PAY_SECRET_KEY", "OZON_PAY_NOTIFICATION_SECRET_KEY")
    # Only the three named credentials. Capture privately, never print/put in argv.
    code = "import os,json; print(json.dumps({k:os.getenv(k,'') for k in " + repr(names) + "}))"
    values = json.loads(run("docker", "exec", "amo-fix-fields", "python", "-c", code))
    if not all(values.get(k) and "\n" not in values[k] and "\r" not in values[k] for k in names):
        raise SystemExit("Required Ozon configuration is missing or malformed")
    values["ACADEMY_PAYMENT_SESSION_SECRET"] = secrets.token_hex(32)
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write("".join(f"{k}={v}\n" for k, v in values.items()))
    values.clear()
    data = ROOT / "data"
    data.mkdir(mode=0o700)
    os.chown(data, 65534, 65534)
    image = run("docker", "inspect", "amo-fix-fields", "--format", "{{.Image}}")
    if not image.startswith("sha256:"):
        raise SystemExit("Immutable dependency image is required")
    # A shared dependency image must not expose historical credential files.
    credential_files = run(
        "docker", "run", "--rm", "--network", "none", "--user", "0",
        "--entrypoint", "python", image, "-c",
        "from pathlib import Path; print(len(list(Path('/app').rglob('.env*'))))")
    if credential_files != "0":
        raise SystemExit("Dependency image contains credential files; rebuild it without secret-bearing layers")
    result = run(
        "docker", "run", "-d", "--name", "academy-deposit-link", "--restart", "unless-stopped",
        "--user", "65534:65534", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--memory", "256m", "--cpus", "1", "--pids-limit", "128",
        "--log-opt", "max-size=10m", "--log-opt", "max-file=5", "--tmpfs", "/tmp:rw,nosuid,size=16m",
        "--env-file", str(env_file), "-p", "127.0.0.1:8040:8000",
        "-v", f"{ROOT}/app.py:/service/app.py:ro", "-v", f"{data}:/data:rw",
        "-w", "/service", "--entrypoint", "python", image,
        "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log")
    for _ in range(20):
        try:
            with urllib.request.urlopen("http://127.0.0.1:8040/health", timeout=2) as response:
                if response.status == 200:
                    break
        except OSError:
            time.sleep(0.5)
    else:
        raise SystemExit("Dedicated container did not become healthy; nginx is unchanged")
    # Read fresh and write in this same process; no stale wholesale config upload.
    original = NGINX.read_bytes()
    if INCLUDE.encode() in original or SNIPPET.exists():
        raise SystemExit("Payment nginx routing already exists; review before changing")
    marker = b"    location /api/ {"
    if original.count(marker) != 1:
        raise SystemExit("Cannot find unique nginx insertion point")
    BACKUP.mkdir(parents=True, exist_ok=True)
    backup_file = BACKUP / "team-panel.before.conf"
    if backup_file.exists():
        raise SystemExit("Backup already exists; do not overwrite")
    backup_file.write_bytes(original)
    SNIPPET.write_bytes((ROOT / "nginx.conf").read_bytes())
    updated = original.replace(marker, INCLUDE.encode() + b"\n" + marker, 1)
    if NGINX.read_bytes() != original:
        raise SystemExit("Nginx changed during deployment; stop")
    NGINX.write_bytes(updated)
    try:
        subprocess.run(["nginx", "-t"], check=True)
        subprocess.run(["systemctl", "reload", "nginx"], check=True)
    except subprocess.CalledProcessError:
        if NGINX.read_bytes() == updated:
            NGINX.write_bytes(original)
            SNIPPET.unlink()
        raise
    release = {"deployed_at": int(time.time()), "image": image,
               "container": result[:12], "code_sha256": hashlib.sha256((ROOT / "app.py").read_bytes()).hexdigest(),
               "nginx_before_sha256": hashlib.sha256(original).hexdigest(),
               "nginx_after_sha256": hashlib.sha256(updated).hexdigest()}
    (ROOT / "DEPLOYED.json").write_text(json.dumps(release, indent=2) + "\n")
    print(json.dumps(release))


if __name__ == "__main__":
    main()
