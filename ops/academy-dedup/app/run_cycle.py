#!/usr/bin/env python3
"""Server-only Academy deal deduplicator.

Builds a fresh read-only plan, then merges only unambiguous open duplicates
inside the Academy pipeline. Uses the existing amo OAuth token; no token is
copied into this directory. Every attempted group gets a pre-merge backup and
post-verification. Closed deals and non-Academy pipelines are never merged.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone, timedelta

HERE = pathlib.Path(__file__).resolve().parent
STATE = pathlib.Path("/var/lib/academy-dedup")
RUNS = STATE / "runs"
LOCK = STATE / "cycle.lock"
AMO_ENV = "/opt/integrations/amo_fix_fields/.env"
BASE = "https://new5a2e8ea7b16b4.amocrm.ru"
PIPELINE = 8642410
CLOSED = {142, 143}
MAX_GROUPS = int(os.environ.get("ACADEMY_DEDUP_MAX_GROUPS", "10"))
PAUSE_S = float(os.environ.get("ACADEMY_DEDUP_PAUSE_S", "2"))
MIN_AGE_S = int(os.environ.get("ACADEMY_DEDUP_MIN_AGE_S", "60"))
MS_ORDER_FIELD = 576689
PAY_LINK_FIELD = 577617
APP_TAG = "оставил заявку с веба"
ATT_TAG = "был на вебе 1"
APP_TASK_TEXT = "оставил заявку с веба"
MSK = timezone(timedelta(hours=3))

os.environ.setdefault("AMO_ENV", AMO_ENV)
sys.path.insert(0, str(HERE))
import amo  # noqa: E402
import build_plan as bp  # noqa: E402


def log(message: str) -> None:
    print(f"[{datetime.now(MSK):%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def atomic_json(path: pathlib.Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def curl_json(method: str, path: str, *, form=None, body=None, tries=5):
    url = path if path.startswith("http") else BASE + path
    token = amo._token()
    for attempt in range(tries):
        cmd = [
            "curl", "-sS", "--globoff", "--compressed", "--max-time", "90",
            "-w", "\n%{http_code}", "-X", method,
            "--variable", "%ACADEMY_AMO_TOKEN",
            "--expand-header", "Authorization: Bearer {{ACADEMY_AMO_TOKEN}}",
            "-H", "Accept: application/json",
            "-H", "X-Requested-With: XMLHttpRequest",
        ]
        payload = None
        if form is not None:
            payload = urllib.parse.urlencode(form, doseq=True)
            cmd += ["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-"]
        elif body is not None:
            payload = json.dumps(body, ensure_ascii=False)
            cmd += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        cmd.append(url)
        proc = subprocess.run(
            cmd, input=payload,
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "ACADEMY_AMO_TOKEN": token},
        )
        out = proc.stdout or ""
        cut = out.rfind("\n")
        raw, code_s = (out[:cut], out[cut + 1:].strip()) if cut >= 0 else ("", out.strip())
        code = int(code_s) if code_s.isdigit() else 0
        if code == 429 or code >= 500 or code == 0:
            time.sleep(1.5 * (attempt + 1))
            continue
        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": raw[:500]}
        return code, parsed
    return 0, {"error": "network"}


def find_first(value, key):
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = find_first(child, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_first(child, key)
            if found is not None:
                return found
    return None


def poll_job(job_id: str):
    query = urllib.parse.urlencode({"request[multiactions][status][0][job_id]": job_id})
    for _ in range(40):
        code, data = curl_json("GET", f"/ajax/v1/multiactions/status?{query}")
        if code != 200:
            time.sleep(1)
            continue
        rows = find_first(data, "data")
        if isinstance(rows, list):
            row = next((x for x in rows if str(x.get("job_id")) == str(job_id)), None)
            if row:
                status = int(row.get("status") or 0)
                if status == 3:
                    return True, row
                if status == 4:
                    return False, row
        time.sleep(0.8)
    return False, {"error": "job timeout", "job_id": job_id}


def merge_form(absorbed_id: int, result: dict):
    pairs = [("id[]", str(absorbed_id)), ("id[]", str(result["ID"]))]
    pairs += [
        ("result_element[NAME]", result.get("NAME") or ""),
        ("result_element[MAIN_USER_ID]", str(result.get("MAIN_USER_ID") or "")),
        ("result_element[DATE_CREATE]", result["DATE_CREATE"]),
        ("result_element[PRICE]", str(result.get("PRICE") or 0)),
    ]
    for field_id, value in (result.get("cfv") or {}).items():
        if isinstance(value, list):
            pairs.extend((f"result_element[cfv][{field_id}][]", str(v)) for v in value)
        else:
            pairs.append((f"result_element[cfv][{field_id}]", str(value)))
    pairs.extend(("result_element[CONTACTS][]", str(cid)) for cid in result.get("CONTACTS") or [])
    pairs.append(("result_element[ID]", str(result["ID"])))
    return pairs


def merge_pair(absorbed_id: int, result: dict):
    code, data = curl_json("POST", "/ajax/merge/leads/save", form=merge_form(absorbed_id, result))
    job_id = find_first(data, "job_id")
    if code != 202 or job_id is None:
        return False, {"http": code, "response": data}
    ok, status = poll_job(str(job_id))
    return ok, {"http": code, "job_id": str(job_id), "status": status}


def add_note(lead_id: int, text: str):
    return curl_json("POST", f"/api/v4/leads/{lead_id}/notes", body=[{"note_type": "common", "params": {"text": text}}])


def enforce_survivor_card(item):
    """amo native merge may ignore chosen name/owner; enforce both via v4."""
    expected = item["result_element"]
    return curl_json(
        "PATCH",
        f"/api/v4/leads/{int(item['survivor'])}",
        body={
            "name": expected.get("NAME") or "",
            "responsible_user_id": int(expected["MAIN_USER_ID"]),
        },
    )


def get_lead(lead_id: int):
    data = amo.get(f"leads/{lead_id}?with=contacts") or {}
    return data if data.get("id") else None


def get_contacts(ids):
    out = {}
    ids = sorted(set(ids))
    for start in range(0, len(ids), 100):
        query = "&".join(f"filter[id][]={value}" for value in ids[start:start + 100])
        data = amo.get(f"contacts?{query}&limit=250") or {}
        for row in ((data.get("_embedded") or {}).get("contacts") or []):
            out[int(row["id"])] = row
    return out


def get_tasks(ids):
    out = []
    ids = sorted(set(int(x) for x in ids))
    for start in range(0, len(ids), 15):
        query = "&".join(f"filter[entity_id][]={value}" for value in ids[start:start + 15])
        data = amo.get(f"tasks?filter[entity_type]=leads&{query}&limit=250") or {}
        if data.get("_error"):
            raise RuntimeError(f"amo task read failed: {data.get('_error')}")
        out.extend(((data.get("_embedded") or {}).get("tasks") or []))
    return out


def strong_keys(contact):
    keys = set()
    phone = bp.phone(contact)
    if phone:
        keys.add("ph:" + phone)
    for field_id, prefix in ((bp.F_BOTHELP, "bh"), (bp.F_TGID, "tg"), (bp.F_TGUSER, "un")):
        value = bp.cf(contact, field_id)
        if value not in (None, ""):
            keys.add(f"{prefix}:{str(value).strip().lower().lstrip('@')}")
    return keys


def identity_is_safe(leads, contacts):
    per_lead = {}
    for lead in leads:
        keys = set()
        for linked in ((lead.get("_embedded") or {}).get("contacts") or []):
            cid = int(linked["id"])
            keys.add(f"cid:{cid}")
            if cid in contacts:
                keys.update(strong_keys(contacts[cid]))
        per_lead[int(lead["id"])] = keys
    survivor_keys = per_lead[int(leads[0]["id"])]
    return all(survivor_keys & per_lead[int(other["id"])] for other in leads[1:]), per_lead


def field_value(lead, field_id):
    return bp.cf(lead, field_id)


def preflight(item):
    ids = [int(item["survivor"])] + [int(x) for x in item["absorbed"]]
    live = [get_lead(value) for value in ids]
    if not live[0]:
        return False, "survivor_missing", None
    # Already absorbed by a concurrent/previous run is idempotent; rebuild next cycle.
    if any(row is None for row in live[1:]):
        return False, "absorbed_already_missing", None
    if any(int(row.get("pipeline_id") or 0) != PIPELINE for row in live):
        return False, "outside_academy", None
    if any(int(row.get("status_id") or 0) in CLOSED for row in live):
        return False, "closed_deal", None
    if any(time.time() - int(row.get("created_at") or 0) < MIN_AGE_S for row in live):
        return False, "too_fresh", None
    ranked = sorted(
        live,
        key=lambda row: (bp.STAGE_RANK.get(int(row["status_id"]), 0), bp.lead_has_human_work(row), -int(row["created_at"])),
        reverse=True,
    )
    if int(ranked[0]["id"]) != int(item["survivor"]):
        return False, "survivor_changed", None
    if any(field_value(row, MS_ORDER_FIELD) for row in live):
        return False, "has_ms_order", None
    if any(field_value(row, PAY_LINK_FIELD) for row in live):
        return False, "has_payment_link", None
    contact_ids = [int(c["id"]) for row in live for c in ((row.get("_embedded") or {}).get("contacts") or [])]
    contacts = get_contacts(contact_ids)
    safe, evidence = identity_is_safe(live, contacts)
    if not safe:
        return False, "weak_or_transitive_identity", {"evidence": {str(k): sorted(v) for k, v in evidence.items()}}
    tasks = get_tasks(ids) if item.get("needs_application_followup") else []
    return True, "ok", {"leads": live, "contacts": list(contacts.values()), "tasks": tasks}


def ensure_webinar_markers(item):
    lead_id = int(item["survivor"])
    live = get_lead(lead_id)
    if not live:
        return False, {"reason": "survivor_missing"}
    wanted_tags = set(item.get("tags_after") or [])
    if item.get("needs_application_followup"):
        wanted_tags.add(APP_TAG)
    if item.get("needs_attendee_tag"):
        wanted_tags.add(ATT_TAG)
    actual_tags = {
        tag.get("name")
        for tag in ((live.get("_embedded") or {}).get("tags") or [])
        if tag.get("name")
    }
    tag_http = None
    if not wanted_tags.issubset(actual_tags):
        tag_http, _ = curl_json(
            "PATCH", f"/api/v4/leads/{lead_id}",
            body={"_embedded": {"tags": [{"name": name} for name in sorted(actual_tags | wanted_tags)]}},
        )
        if not 200 <= tag_http < 300:
            return False, {"reason": "tag_patch_failed", "tag_http": tag_http}
    task_http = None
    task_created = False
    if item.get("needs_application_followup"):
        tasks = get_tasks([lead_id])
        exists = any(
            not task.get("is_completed")
            and (task.get("text") or "").strip().lower() == APP_TASK_TEXT
            for task in tasks
        )
        if not exists:
            task_http, _ = curl_json(
                "POST", "/api/v4/tasks",
                body=[{
                    "entity_id": lead_id,
                    "entity_type": "leads",
                    "task_type_id": 1,
                    "responsible_user_id": int(item["result_element"]["MAIN_USER_ID"]),
                    "complete_till": int(time.time()) + 86400,
                    "text": APP_TASK_TEXT,
                }],
            )
            if not 200 <= task_http < 300:
                return False, {"reason": "task_create_failed", "task_http": task_http}
            task_created = True
    return True, {"tag_http": tag_http, "task_http": task_http, "task_created": task_created}


def execute_group(item, group_dir: pathlib.Path):
    absorbed = [int(x) for x in item["absorbed"]]
    if not absorbed or int(item["survivor"]) in absorbed or len(set(absorbed)) != len(absorbed):
        # A lead read twice from shifting pages must never be merged into itself.
        atomic_json(group_dir / "preflight.json", {"ok": False, "reason": "inconsistent_plan", "item": item})
        return {"status": "skipped", "reason": "inconsistent_plan", "survivor": item["survivor"]}
    ok, reason, backup = preflight(item)
    atomic_json(group_dir / "preflight.json", {"ok": ok, "reason": reason, "item": item, "backup": backup})
    if not ok:
        return {"status": "skipped", "reason": reason, "survivor": item["survivor"]}
    results = []
    for absorbed_id in item["absorbed"]:
        merged, detail = merge_pair(int(absorbed_id), item["result_element"])
        results.append({"absorbed": absorbed_id, "ok": merged, "detail": detail})
        atomic_json(group_dir / "progress.json", results)
        if not merged:
            return {"status": "failed", "reason": "merge_failed", "survivor": item["survivor"], "results": results}
        time.sleep(PAUSE_S)
    patch_code, patch_result = enforce_survivor_card(item)
    markers_ok, markers_result = ensure_webinar_markers(item)
    note_code, note_result = add_note(int(item["survivor"]), item.get("note") or "Автоматическая склейка дублей Академии.")
    survivor = get_lead(int(item["survivor"]))
    absorbed_alive = [value for value in item["absorbed"] if get_lead(int(value))]
    expected = item["result_element"]
    expected_fields = {int(k): v for k, v in (expected.get("cfv") or {}).items()}
    actual_fields = bp.cf_all(survivor) if survivor else {}

    def canonical(value):
        return sorted(str(x) for x in value) if isinstance(value, list) else str(value)

    field_mismatches = [
        fid for fid, value in expected_fields.items()
        if fid not in actual_fields or canonical(actual_fields[fid]) != canonical(value)
    ]
    expected_contacts = {int(x) for x in expected.get("CONTACTS") or []}
    actual_contacts = {
        int(x["id"])
        for x in (((survivor or {}).get("_embedded") or {}).get("contacts") or [])
    }
    exact_ok = bool(survivor) and not field_mismatches and expected_contacts.issubset(actual_contacts)
    exact_ok = exact_ok and int(survivor.get("responsible_user_id") or 0) == int(expected["MAIN_USER_ID"])
    exact_ok = exact_ok and (survivor.get("name") or "").strip() == (expected.get("NAME") or "").strip()
    if item.get("survivor_status_id") is not None:
        exact_ok = exact_ok and int(survivor.get("status_id") or 0) == int(item["survivor_status_id"])
    actual_tags = {
        tag.get("name")
        for tag in (((survivor or {}).get("_embedded") or {}).get("tags") or [])
        if tag.get("name")
    }
    expected_tags = set(item.get("tags_after") or [])
    exact_ok = exact_ok and expected_tags.issubset(actual_tags)
    if item.get("needs_application_followup"):
        exact_ok = exact_ok and any(
            not task.get("is_completed")
            and (task.get("text") or "").strip().lower() == APP_TASK_TEXT
            for task in get_tasks([int(item["survivor"])])
        )
    final = {
        "status": "merged" if not absorbed_alive and exact_ok and markers_ok and 200 <= patch_code < 300 and 200 <= note_code < 300 else "failed",
        "survivor": item["survivor"],
        "absorbed": item["absorbed"],
        "patch_http": patch_code,
        "patch_result": patch_result,
        "markers": markers_result,
        "note_http": note_code,
        "note_result": note_result,
        "survivor_alive": bool(survivor),
        "absorbed_alive": absorbed_alive,
        "field_mismatches": field_mismatches,
        "missing_contacts": sorted(expected_contacts - actual_contacts),
        "exact_ok": exact_ok,
        "results": results,
    }
    atomic_json(group_dir / "postverify.json", final)
    return final


def build_fresh_plan(run_dir: pathlib.Path):
    env = dict(os.environ, AMO_ENV=AMO_ENV)
    proc = subprocess.run(
        [sys.executable, str(HERE / "build_plan.py"), "--out", str(run_dir), "--any-stage"],
        env=env, capture_output=True, text=True, timeout=600,
    )
    (run_dir / "planner.stdout.log").write_text(proc.stdout + proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"planner failed: {proc.returncode}")
    plan = json.loads((run_dir / "plan.json").read_text(encoding="utf-8"))
    suspicious = json.loads((run_dir / "suspicious.json").read_text(encoding="utf-8"))
    return plan + suspicious


def self_test():
    sample = {"ID": 10, "NAME": "Lead", "MAIN_USER_ID": 20, "DATE_CREATE": "2026-10-08 12:00:00", "PRICE": 0,
              "cfv": {"1": "x", "2": ["a", "b"]}, "CONTACTS": [30, 31]}
    form = merge_form(11, sample)
    assert form.count(("id[]", "11")) == 1 and form.count(("id[]", "10")) == 1
    assert ("result_element[cfv][2][]", "a") in form and ("result_element[CONTACTS][]", "31") in form
    assert find_first({"response": {"data": [{"job_id": "abc"}]}}, "job_id") == "abc"
    with tempfile.TemporaryDirectory() as tmp:
        result = execute_group({"survivor": 10, "absorbed": [10]}, pathlib.Path(tmp))
        assert result["status"] == "skipped" and result["reason"] == "inconsistent_plan"
    log("self-test: OK")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    STATE.mkdir(parents=True, exist_ok=True)
    RUNS.mkdir(parents=True, exist_ok=True)
    with LOCK.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log("another cycle is running; exit")
            return 0
        run_dir = RUNS / datetime.now(MSK).strftime("%Y%m%dT%H%M%S%z")
        run_dir.mkdir(parents=True)
        candidates = build_fresh_plan(run_dir)
        log(f"fresh candidates: {len(candidates)}; apply={args.apply}")
        if not args.apply:
            return 0
        results = []
        for index, item in enumerate(candidates[:MAX_GROUPS], 1):
            group_dir = run_dir / f"group-{index:03d}-{item['survivor']}"
            group_dir.mkdir()
            result = execute_group(item, group_dir)
            results.append(result)
            log(f"group {item['survivor']}: {result['status']} ({result.get('reason', 'ok')})")
            if result["status"] == "failed":
                atomic_json(run_dir / "result.json", results)
                return 1
        atomic_json(run_dir / "result.json", results)
        merged = sum(1 for row in results if row["status"] == "merged")
        skipped = sum(1 for row in results if row["status"] == "skipped")
        log(f"cycle complete: merged={merged}, skipped={skipped}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
