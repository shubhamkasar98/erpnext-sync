#!/usr/bin/env python3
"""
zk_erp_sync.py - ZKTeco biometric devices -> ERPNext / HRMS v16 Employee Checkin

Pulls attendance logs from one or more ZKTeco devices and pushes them to
ERPNext as Employee Checkin records. Built to run from cron every few minutes.

Duplicate protection (two layers):
  1. Local SQLite state: every punch is stored once, keyed on
     (device_id, user_id, timestamp), and pushed only once.
  2. Server side: HRMS rejects a second checkin for the same employee and
     timestamp. The script treats that rejection as "already synced".

Other behaviour:
  - Fetching and pushing are decoupled. If ERP is down, punches wait in the
    local queue and go out on the next run. Nothing is lost.
  - Repeat presses (same user within min_gap_seconds, across devices) are
    skipped, so "Alternating IN/OUT" shift logic isn't thrown off.
  - Punches for users with no matching Employee are parked as "unmapped"
    and retried automatically every few hours (or via --retry-failed).
  - Optional: updates Shift Type "Last Sync of Checkin" only when EVERY
    device was read successfully and the queue is empty.
  - A file lock stops overlapping cron runs.

Commands:
  python zk_erp_sync.py --config config.ini                # normal run
  python zk_erp_sync.py --config config.ini --dry-run      # read-only preview
  python zk_erp_sync.py --config config.ini --test-erp     # URL, keys, HRMS, mapping
  python zk_erp_sync.py --config config.ini --test-devices # connect to each device
  python zk_erp_sync.py --config config.ini --list-users   # device users vs ERP mapping
  python zk_erp_sync.py --config config.ini --status       # local queue summary
  python zk_erp_sync.py --config config.ini --retry-failed # requeue failed/unmapped
"""
from __future__ import annotations

import argparse
import configparser
import csv
import fcntl
import json
import logging
import re
import sqlite3
import sys
import time
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

# HRMS v16: hrms/hr/doctype/employee_checkin/employee_checkin.py (POST only)
CHECKIN_METHOD = "hrms.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
ALLOWED_EMPLOYEE_FIELDS = {"name", "employee", "attendance_device_id"}
TS_FMT = "%Y-%m-%d %H:%M:%S"
DUPLICATE_MARKERS = ("already has a log with the same timestamp",)
UNMAPPED_MARKERS = ("no employee found",)
# ZKTeco punch codes -> ERPNext log_type (only used when log_type_mode = punch)
# 0 Check-In, 1 Check-Out, 2 Break-Out, 3 Break-In, 4 OT-In, 5 OT-Out
PUNCH_TO_LOG_TYPE = {0: "IN", 1: "OUT", 2: "OUT", 3: "IN", 4: "IN", 5: "OUT"}
DONE_STATES = ("synced", "duplicate")

log = logging.getLogger("zk_erp_sync")
Punch = namedtuple("Punch", "user_id ts punch status")


class ErpUnavailable(Exception):
    """Network error or 5xx. Stop pushing for this run; retry next run."""


class ErpFatal(Exception):
    """Bad API keys, missing permission, or HRMS not installed. Needs a human."""


# --------------------------------------------------------------------------- config

def load_config(path: Path) -> dict:
    cp = configparser.ConfigParser(inline_comment_prefixes=(";", "#"), interpolation=None)
    if not cp.read(path):
        raise SystemExit(f"Config file not found or unreadable: {path}")
    for sec in ("erpnext", "sync"):
        if not cp.has_section(sec):
            raise SystemExit(f"Config is missing the [{sec}] section")
    base = path.resolve().parent

    def rel(value, default):
        p = Path((value or "").strip() or default).expanduser()
        return p if p.is_absolute() else base / p

    erp, sync = cp["erpnext"], cp["sync"]
    start = sync.get("import_start_date", "").strip()
    if not start:
        raise SystemExit("[sync] import_start_date is required, e.g. 2026-09-01")

    cfg = {
        "erp_url": erp.get("url", "").strip().rstrip("/"),
        "api_key": erp.get("api_key", "").strip(),
        "api_secret": erp.get("api_secret", "").strip(),
        "employee_field": erp.get("employee_field", "attendance_device_id").strip(),
        "timezone": erp.get("timezone", "Asia/Kolkata").strip(),
        "verify_ssl": erp.getboolean("verify_ssl", True),
        "timeout": erp.getint("request_timeout", 30),
        "import_start": datetime.strptime(start, "%Y-%m-%d"),
        "log_type_mode": sync.get("log_type_mode", "none").strip().lower(),
        "min_gap_seconds": sync.getint("min_gap_seconds", 60),
        "max_attempts": sync.getint("max_attempts", 5),
        "unmapped_retry_hours": sync.getint("unmapped_retry_hours", 6),
        "max_push_per_run": sync.getint("max_push_per_run", 5000),
        "shift_types": [s.strip() for s in sync.get("shift_types", "").split(",") if s.strip()],
        "data_dir": rel(sync.get("data_dir"), "data"),
        "log_dir": rel(sync.get("log_dir"), "logs"),
        "devices": [],
    }

    if cfg["log_type_mode"] not in ("none", "punch"):
        raise SystemExit("[sync] log_type_mode must be 'none' or 'punch'")
    if cfg["employee_field"] not in ALLOWED_EMPLOYEE_FIELDS:
        raise SystemExit(f"[erpnext] employee_field must be one of {sorted(ALLOWED_EMPLOYEE_FIELDS)} (HRMS v16 rule)")
    try:
        cfg["tz"] = ZoneInfo(cfg["timezone"])
    except ZoneInfoNotFoundError:
        raise SystemExit(f"[erpnext] unknown timezone: {cfg['timezone']}")

    for section in cp.sections():
        if not section.lower().startswith("device:"):
            continue
        s = cp[section]
        if not s.getboolean("enabled", True):
            continue
        key = section.split(":", 1)[1].strip()
        dev = {
            "key": key,
            "driver": s.get("driver", "zk").strip().lower(),
            "device_id": s.get("device_id", key).strip(),
            "ip": s.get("ip", "").strip(),
            "port": s.getint("port", 4370),
            "password": s.getint("password", 0),
            "timeout": s.getint("timeout", 30),
            "force_udp": s.getboolean("force_udp", False),
            "disable_while_reading": s.getboolean("disable_while_reading", False),
            "sync_device_time": s.getboolean("sync_device_time", False),
            "mock_file": rel(s.get("mock_file"), f"mock_data/{key}.csv"),
            "mock_offline": s.getboolean("mock_offline", False),
        }
        if dev["driver"] not in ("zk", "mock"):
            raise SystemExit(f"[{section}] driver must be 'zk' or 'mock'")
        if dev["driver"] == "zk" and not dev["ip"]:
            raise SystemExit(f"[{section}] ip is required for driver = zk")
        cfg["devices"].append(dev)

    if not cfg["devices"]:
        raise SystemExit("No enabled [device:...] sections in config")
    ids = [d["device_id"] for d in cfg["devices"]]
    if len(ids) != len(set(ids)):
        raise SystemExit("device_id values must be unique across devices")
    return cfg


def now_local(tz: ZoneInfo) -> datetime:
    """Current wall-clock time in the ERP timezone, naive, no microseconds."""
    return datetime.now(tz).replace(tzinfo=None, microsecond=0)


def setup_logging(log_dir: Path, verbose: bool) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    fh = RotatingFileHandler(log_dir / "sync.log", maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    if sys.stdout.isatty():  # interactive: also print; under cron: file only
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)
    log.setLevel(logging.DEBUG if verbose else logging.INFO)


def acquire_lock(data_dir: Path):
    data_dir.mkdir(parents=True, exist_ok=True)
    fh = open(data_dir / "sync.lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    return fh


# --------------------------------------------------------------------------- devices

class ZKSource:
    """Real ZKTeco device over the standard SDK protocol (TCP/UDP 4370) via pyzk."""

    def __init__(self, dev: dict, tz: ZoneInfo):
        self.dev, self.tz = dev, tz

    @contextmanager
    def session(self):
        try:
            from zk import ZK
        except ImportError:
            raise SystemExit("pyzk is not installed. Run: venv/bin/pip install -r requirements.txt")
        d = self.dev
        zk = ZK(d["ip"], port=d["port"], timeout=d["timeout"], password=d["password"],
                force_udp=d["force_udp"], ommit_ping=True)
        conn = zk.connect()
        try:
            yield conn
        finally:
            try:
                conn.disconnect()
            except Exception:
                pass

    def fetch(self) -> list[Punch]:
        d = self.dev
        with self.session() as conn:
            try:
                dev_time = conn.get_time()
                drift = abs((dev_time - now_local(self.tz)).total_seconds())
                if drift > 120:
                    log.warning("[%s] device clock is off by %ds (device says %s)", d["device_id"], drift, dev_time)
            except Exception:
                pass
            if d["sync_device_time"]:
                conn.set_time(now_local(self.tz))
            if d["disable_while_reading"]:
                conn.disable_device()
            try:
                records = conn.get_attendance() or []
            finally:
                if d["disable_while_reading"]:
                    try:
                        conn.enable_device()
                    except Exception as e:
                        log.error("[%s] could NOT re-enable device, check it: %s", d["device_id"], e)
        return [
            Punch(str(r.user_id).strip(), r.timestamp.replace(microsecond=0),
                  int(r.punch or 0), int(r.status or 0))
            for r in records
        ]

    def info(self) -> dict:
        with self.session() as conn:
            out = {}
            for label, fn in (("firmware", conn.get_firmware_version), ("serial", conn.get_serialnumber),
                              ("name", conn.get_device_name), ("device_time", conn.get_time)):
                try:
                    out[label] = fn()
                except Exception as e:
                    out[label] = f"n/a ({e})"
            try:
                conn.read_sizes()
                out["users"], out["records"] = conn.users, conn.records
            except Exception as e:
                out["users"] = out["records"] = f"n/a ({e})"
            return out

    def users(self) -> list[dict]:
        with self.session() as conn:
            return [{"uid": u.uid, "user_id": str(u.user_id).strip(), "name": u.name} for u in conn.get_users()]


class MockSource:
    """Fake device backed by a CSV file (user_id,timestamp,punch,status). For testing without hardware."""

    def __init__(self, dev: dict, tz: ZoneInfo):
        self.dev, self.tz = dev, tz

    def fetch(self) -> list[Punch]:
        d = self.dev
        if d["mock_offline"]:
            raise ConnectionError("mock device is offline (mock_offline = true)")
        f = d["mock_file"]
        if not f.exists():
            raise FileNotFoundError(f"mock file not found: {f} (create it with mock_punch_generator.py)")
        out = []
        with f.open(newline="") as fh:
            for line_no, row in enumerate(csv.DictReader(fh), start=2):
                try:
                    out.append(Punch(row["user_id"].strip(),
                                     datetime.strptime(row["timestamp"].strip(), TS_FMT),
                                     int(row.get("punch") or 0), int(row.get("status") or 1)))
                except (KeyError, ValueError, AttributeError):
                    log.warning("[%s] bad row %d in %s, ignored", d["device_id"], line_no, f)
        return out

    def info(self) -> dict:
        rows = self.fetch()
        return {"firmware": "MOCK", "serial": f"MOCK-{self.dev['key']}", "name": f"Mock ({self.dev['mock_file'].name})",
                "device_time": now_local(self.tz), "users": len({r.user_id for r in rows}), "records": len(rows)}

    def users(self) -> list[dict]:
        ids = sorted({r.user_id for r in self.fetch()})
        return [{"uid": i + 1, "user_id": u, "name": f"Mock user {u}"} for i, u in enumerate(ids)]


def make_source(dev: dict, tz: ZoneInfo):
    return MockSource(dev, tz) if dev["driver"] == "mock" else ZKSource(dev, tz)


# --------------------------------------------------------------------------- local state

SCHEMA = """
CREATE TABLE IF NOT EXISTS punches (
    device_id       TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    ts              TEXT NOT NULL,
    punch           INTEGER,
    status          INTEGER,
    state           TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    erp_name        TEXT,
    fetched_at      TEXT NOT NULL,
    last_attempt_at TEXT,
    synced_at       TEXT,
    PRIMARY KEY (device_id, user_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_punches_state ON punches(state, ts);
CREATE INDEX IF NOT EXISTS idx_punches_user_ts ON punches(user_id, ts);
CREATE TABLE IF NOT EXISTS device_runs (
    device_id     TEXT PRIMARY KEY,
    last_ok_at    TEXT,
    last_count    INTEGER,
    last_error    TEXT,
    last_error_at TEXT
);
"""


class Store:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(db_path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)

    def commit(self):
        self.db.commit()

    def known_keys(self, device_id: str) -> set:
        cur = self.db.execute("SELECT user_id, ts FROM punches WHERE device_id=?", (device_id,))
        return {(r[0], r[1]) for r in cur}

    def has_near_punch(self, user_id: str, ts: datetime, gap: int) -> bool:
        lo = (ts - timedelta(seconds=gap)).strftime(TS_FMT)
        hi = (ts + timedelta(seconds=gap)).strftime(TS_FMT)
        cur = self.db.execute(
            "SELECT 1 FROM punches WHERE user_id=? AND state!='skipped' AND ts BETWEEN ? AND ? LIMIT 1",
            (user_id, lo, hi))
        return cur.fetchone() is not None

    def insert(self, device_id: str, p: Punch, state: str, note: str | None, now: datetime):
        self.db.execute(
            "INSERT OR IGNORE INTO punches (device_id,user_id,ts,punch,status,state,last_error,fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (device_id, p.user_id, p.ts.strftime(TS_FMT), p.punch, p.status, state, note, now.strftime(TS_FMT)))

    def record_device_run(self, device_id: str, now: datetime, count: int | None = None, error: str | None = None):
        n = now.strftime(TS_FMT)
        self.db.execute("INSERT OR IGNORE INTO device_runs (device_id) VALUES (?)", (device_id,))
        if error is None:
            self.db.execute("UPDATE device_runs SET last_ok_at=?, last_count=? WHERE device_id=?", (n, count, device_id))
        else:
            self.db.execute("UPDATE device_runs SET last_error=?, last_error_at=? WHERE device_id=?",
                            (error[:500], n, device_id))
        self.db.commit()

    def due_rows(self, cfg: dict, now: datetime) -> list[sqlite3.Row]:
        cutoff = (now - timedelta(hours=cfg["unmapped_retry_hours"])).strftime(TS_FMT)
        return self.db.execute(
            """SELECT rowid AS rid, * FROM punches
               WHERE state='pending'
                  OR (state='failed' AND attempts < ?)
                  OR (state='unmapped' AND (last_attempt_at IS NULL OR last_attempt_at < ?))
               ORDER BY ts, device_id LIMIT ?""",
            (cfg["max_attempts"], cutoff, cfg["max_push_per_run"])).fetchall()

    def queue_left(self, cfg: dict) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM punches WHERE state='pending' OR (state='failed' AND attempts < ?)",
            (cfg["max_attempts"],)).fetchone()[0]

    def mark(self, rid: int, state: str, now: datetime, error: str | None = None, erp_name: str | None = None):
        n = now.strftime(TS_FMT)
        self.db.execute(
            """UPDATE punches SET state=?, attempts=attempts+1, last_error=?, erp_name=COALESCE(?, erp_name),
               last_attempt_at=?, synced_at=CASE WHEN ? IN ('synced','duplicate') THEN ? ELSE synced_at END
               WHERE rowid=?""",
            (state, error, erp_name, n, state, n, rid))
        self.db.commit()

    def requeue_failed(self) -> int:
        cur = self.db.execute(
            "UPDATE punches SET state='pending', attempts=0 WHERE state IN ('failed','unmapped')")
        self.db.commit()
        return cur.rowcount


# --------------------------------------------------------------------------- ERP client

def _clean(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


def _error_text(resp: requests.Response) -> str:
    try:
        data = resp.json()
    except ValueError:
        return _clean(resp.text)[:300]
    msgs = []
    if data.get("_server_messages"):
        try:
            for m in json.loads(data["_server_messages"]):
                m = json.loads(m) if isinstance(m, str) else m
                msgs.append(m.get("message", "") if isinstance(m, dict) else str(m))
        except (ValueError, TypeError):
            pass
    if not msgs and data.get("exception"):
        msgs.append(str(data["exception"]).splitlines()[-1])
    for e in data.get("errors") or []:
        if isinstance(e, dict):
            msgs.append(e.get("message", ""))
    return _clean(" | ".join(m for m in msgs if m))[:300] or f"HTTP {resp.status_code}"


class ErpClient:
    def __init__(self, cfg: dict):
        if not (cfg["erp_url"] and cfg["api_key"] and cfg["api_secret"]):
            raise SystemExit("[erpnext] url, api_key and api_secret are required")
        self.url, self.timeout, self.verify = cfg["erp_url"], cfg["timeout"], cfg["verify_ssl"]
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"token {cfg['api_key']}:{cfg['api_secret']}",
            "Accept": "application/json",
        })

    def _req(self, method: str, path: str, **kw) -> requests.Response:
        try:
            r = self.s.request(method, f"{self.url}{path}", timeout=self.timeout, verify=self.verify, **kw)
        except requests.RequestException as e:
            raise ErpUnavailable(f"cannot reach ERP: {e}") from e
        if r.status_code >= 500 or r.status_code in (408, 429):
            # frappe.throw() normally gives 417; real 5xx means server trouble
            body = r.text.lower()
            if not any(m in body for m in DUPLICATE_MARKERS + UNMAPPED_MARKERS):
                raise ErpUnavailable(f"ERP returned HTTP {r.status_code}: {_error_text(r)}")
        return r

    def whoami(self) -> str:
        r = self._req("GET", "/api/method/frappe.auth.get_logged_in_user")
        if r.status_code != 200:
            raise ErpFatal(f"login failed (HTTP {r.status_code}): {_error_text(r)}")
        return r.json().get("message")

    def get_list(self, doctype: str, fields: list, filters: list | None = None, limit: int = 0) -> list:
        params = {"fields": json.dumps(fields), "limit_page_length": limit}
        if filters:
            params["filters"] = json.dumps(filters)
        r = self._req("GET", f"/api/resource/{quote(doctype)}", params=params)
        if r.status_code != 200:
            raise ErpFatal(f"cannot read {doctype} (HTTP {r.status_code}): {_error_text(r)}")
        return r.json().get("data", [])

    def get_doc(self, doctype: str, name: str) -> dict:
        r = self._req("GET", f"/api/resource/{quote(doctype)}/{quote(name, safe='')}")
        if r.status_code != 200:
            raise ErpFatal(f"cannot read {doctype} '{name}' (HTTP {r.status_code}): {_error_text(r)}")
        return r.json().get("data", {})

    def add_checkin(self, payload: dict) -> tuple[str, str | None]:
        """Returns (state, detail). state: synced | duplicate | unmapped | failed."""
        r = self._req("POST", f"/api/method/{CHECKIN_METHOD}", json=payload)
        if r.status_code == 200:
            msg = r.json().get("message") or {}
            return "synced", (msg.get("name") if isinstance(msg, dict) else None)
        body = r.text.lower()
        if any(m in body for m in DUPLICATE_MARKERS):
            return "duplicate", None
        if any(m in body for m in UNMAPPED_MARKERS):
            return "unmapped", _error_text(r)
        if r.status_code in (401, 403, 404):
            raise ErpFatal(f"HTTP {r.status_code} on checkin API: {_error_text(r)}")
        return "failed", _error_text(r)

    def set_last_sync(self, shift_type: str, value: str) -> None:
        r = self._req("PUT", f"/api/resource/Shift%20Type/{quote(shift_type, safe='')}",
                      json={"last_sync_of_checkin": value})
        if r.status_code != 200:
            raise ErpFatal(f"cannot update Shift Type '{shift_type}' (HTTP {r.status_code}): {_error_text(r)}")


# --------------------------------------------------------------------------- sync run

def fetch_stage(cfg: dict, store: Store, dry_run: bool) -> bool:
    all_ok = True
    gap = cfg["min_gap_seconds"]
    for dev in cfg["devices"]:
        did = dev["device_id"]
        now = now_local(cfg["tz"])
        try:
            t0 = time.monotonic()
            punches = make_source(dev, cfg["tz"]).fetch()
            log.debug("[%s] read %d records in %.1fs", did, len(punches), time.monotonic() - t0)
        except Exception as e:
            all_ok = False
            log.error("[%s] fetch FAILED: %s", did, e)
            if not dry_run:
                store.record_device_run(did, now, error=str(e))
            continue

        known = store.known_keys(did)
        future_limit = now + timedelta(days=1)
        fresh, seen = [], set()
        for p in sorted(punches, key=lambda x: (x.ts, x.user_id)):
            k = (p.user_id, p.ts.strftime(TS_FMT))
            if p.ts < cfg["import_start"] or k in known or k in seen or not p.user_id:
                continue
            seen.add(k)
            fresh.append(p)

        queued = skipped = 0
        for p in fresh:
            if dry_run:
                queued += 1
                continue
            if p.ts > future_limit:
                store.insert(did, p, "skipped", "timestamp in the future, check device clock", now)
                skipped += 1
            elif gap > 0 and store.has_near_punch(p.user_id, p.ts, gap):
                store.insert(did, p, "skipped", f"repeat punch within {gap}s", now)
                skipped += 1
            else:
                store.insert(did, p, "pending", None, now)
                queued += 1
        if not dry_run:
            store.commit()
            store.record_device_run(did, now, count=len(punches))
        log.info("[%s] device records=%d new=%d%s skipped=%d", did, len(punches), queued,
                 " (dry-run, not saved)" if dry_run else "", skipped)
    return all_ok


def push_stage(cfg: dict, store: Store, erp: ErpClient | None, dry_run: bool) -> dict:
    stats = {"synced": 0, "duplicate": 0, "unmapped": 0, "failed": 0, "aborted": False}
    rows = store.due_rows(cfg, now_local(cfg["tz"]))
    if dry_run:
        log.info("dry-run: %d punches already queued locally would be pushed (plus the new ones above)", len(rows))
        return stats
    for row in rows:
        payload = {
            "employee_field_value": row["user_id"],
            "timestamp": row["ts"],
            "device_id": row["device_id"],
            "employee_fieldname": cfg["employee_field"],
        }
        if cfg["log_type_mode"] == "punch":
            lt = PUNCH_TO_LOG_TYPE.get(row["punch"])
            if lt:
                payload["log_type"] = lt
        try:
            state, detail = erp.add_checkin(payload)
        except ErpUnavailable as e:
            log.error("ERP unavailable, stopping push for this run (queue kept): %s", e)
            stats["aborted"] = True
            break
        except ErpFatal as e:
            log.error("ERP rejected the request, fix config/permissions: %s", e)
            stats["aborted"] = True
            break
        now = now_local(cfg["tz"])
        if state == "synced":
            store.mark(row["rid"], "synced", now, erp_name=detail)
            log.debug("pushed %s %s @%s -> %s", row["device_id"], row["user_id"], row["ts"], detail)
        elif state == "duplicate":
            store.mark(row["rid"], "duplicate", now, error="already in ERP")
            log.debug("already in ERP: %s @%s", row["user_id"], row["ts"])
        elif state == "unmapped":
            if row["state"] != "unmapped":
                log.warning("no Employee with %s=%s (punch %s on %s). Will retry every %dh.",
                            cfg["employee_field"], row["user_id"], row["ts"], row["device_id"],
                            cfg["unmapped_retry_hours"])
            store.mark(row["rid"], "unmapped", now, error=detail)
        else:
            store.mark(row["rid"], "failed", now, error=detail)
            log.warning("push failed (attempt %d/%d) %s @%s: %s", row["attempts"] + 1, cfg["max_attempts"],
                        row["user_id"], row["ts"], detail)
        stats[state] += 1
    return stats


def shift_stage(cfg: dict, store: Store, erp: ErpClient, run_started: datetime, devices_ok: bool, stats: dict):
    if not cfg["shift_types"]:
        return
    left = store.queue_left(cfg)
    if not devices_ok or stats["aborted"] or left:
        log.warning("Last Sync of Checkin NOT updated (devices_ok=%s, push_aborted=%s, queue_left=%d)",
                    devices_ok, stats["aborted"], left)
        return
    # small buffer so a punch made during this run is never behind the marker
    value = (run_started - timedelta(minutes=2)).strftime(TS_FMT)
    for st in cfg["shift_types"]:
        try:
            erp.set_last_sync(st, value)
            log.debug("Shift Type '%s' last_sync_of_checkin = %s", st, value)
        except (ErpFatal, ErpUnavailable) as e:
            log.error("Shift Type update failed: %s", e)


def run_sync(cfg: dict, dry_run: bool) -> int:
    t0 = time.monotonic()
    run_started = now_local(cfg["tz"])
    store = Store(cfg["data_dir"] / "sync_state.db")
    erp = None if dry_run else ErpClient(cfg)
    devices_ok = fetch_stage(cfg, store, dry_run)
    stats = push_stage(cfg, store, erp, dry_run)
    if not dry_run:
        shift_stage(cfg, store, erp, run_started, devices_ok, stats)
    log.info("run done in %.1fs | devices_ok=%s | pushed=%d already_in_erp=%d unmapped=%d failed=%d | queue_left=%d%s",
             time.monotonic() - t0, devices_ok, stats["synced"], stats["duplicate"], stats["unmapped"],
             stats["failed"], store.queue_left(cfg), " | PUSH ABORTED" if stats["aborted"] else "")
    return 0 if devices_ok and not stats["aborted"] else 1


# --------------------------------------------------------------------------- commands

def cmd_test_erp(cfg: dict) -> int:
    erp = ErpClient(cfg)
    try:
        print(f"[OK]   Logged in as: {erp.whoami()}  ({cfg['erp_url']})")
    except (ErpFatal, ErpUnavailable) as e:
        print(f"[FAIL] {e}")
        return 1
    rc = 0
    try:
        erp.get_list("Employee Checkin", ["name"], limit=1)
        print("[OK]   Employee Checkin is readable (HRMS installed, permission OK)")
    except (ErpFatal, ErpUnavailable) as e:
        print(f"[FAIL] Employee Checkin: {e}\n       Is HRMS installed on the site? Does the API user have the HR User role?")
        rc = 1
    try:
        emps = erp.get_list("Employee", ["name", "employee_name", cfg["employee_field"]],
                            [[cfg["employee_field"], "is", "set"], ["status", "=", "Active"]])
        print(f"[OK]   Active employees with {cfg['employee_field']} set: {len(emps)}")
        for e in emps[:15]:
            print(f"         {e.get(cfg['employee_field']):>8}  {e['name']}  {e.get('employee_name', '')}")
        if len(emps) > 15:
            print(f"         ... and {len(emps) - 15} more")
        if not emps:
            print("       WARNING: nobody is mapped. Fill 'Attendance Device ID' on Employee.")
    except (ErpFatal, ErpUnavailable) as e:
        print(f"[FAIL] Employee list: {e}")
        rc = 1
    for st in cfg["shift_types"]:
        try:
            d = erp.get_doc("Shift Type", st)
            print(f"[OK]   Shift Type '{st}': auto_attendance={d.get('enable_auto_attendance')} "
                  f"auto_update_last_sync={d.get('auto_update_last_sync')} last_sync={d.get('last_sync_of_checkin')}")
            if d.get("auto_update_last_sync"):
                print("       NOTE: untick 'Automatically update Last Sync of Checkin' when this script manages it.")
        except (ErpFatal, ErpUnavailable) as e:
            print(f"[FAIL] Shift Type '{st}': {e}")
            rc = 1
    return rc


def cmd_test_devices(cfg: dict) -> int:
    rc = 0
    for dev in cfg["devices"]:
        where = dev["ip"] + f":{dev['port']}" if dev["driver"] == "zk" else str(dev["mock_file"])
        try:
            info = make_source(dev, cfg["tz"]).info()
            print(f"[OK]   {dev['device_id']} ({dev['driver']}, {where})")
            for k, v in info.items():
                print(f"         {k:<12} {v}")
        except Exception as e:
            print(f"[FAIL] {dev['device_id']} ({dev['driver']}, {where}): {e}")
            rc = 1
    return rc


def cmd_list_users(cfg: dict) -> int:
    mapped = None
    try:
        erp = ErpClient(cfg)
        emps = erp.get_list("Employee", ["name", "employee_name", cfg["employee_field"]],
                            [[cfg["employee_field"], "is", "set"]])
        mapped = {str(e[cfg["employee_field"]]).strip(): e for e in emps}
    except (SystemExit, ErpFatal, ErpUnavailable) as e:
        print(f"(ERP not checked: {e})")
    rc = 0
    for dev in cfg["devices"]:
        print(f"\n== {dev['device_id']}")
        try:
            users = make_source(dev, cfg["tz"]).users()
        except Exception as e:
            print(f"[FAIL] {e}")
            rc = 1
            continue
        print(f"{'user_id':>10}  {'device name':<24} ERP employee")
        for u in sorted(users, key=lambda x: x["user_id"]):
            if mapped is None:
                erp_col = "?"
            elif u["user_id"] in mapped:
                e = mapped[u["user_id"]]
                erp_col = f"{e['name']} ({e.get('employee_name', '')})"
            else:
                erp_col = "** NOT MAPPED **"
            print(f"{u['user_id']:>10}  {str(u['name'])[:24]:<24} {erp_col}")
    return rc


def cmd_status(cfg: dict) -> int:
    store = Store(cfg["data_dir"] / "sync_state.db")
    print("Devices:")
    for r in store.db.execute("SELECT * FROM device_runs ORDER BY device_id"):
        print(f"  {r['device_id']:<16} last_ok={r['last_ok_at']} records={r['last_count']} "
              f"last_error_at={r['last_error_at']} {('- ' + r['last_error']) if r['last_error'] else ''}")
    print("\nPunches by state:")
    for r in store.db.execute("SELECT device_id, state, COUNT(*) c, MAX(ts) m FROM punches "
                              "GROUP BY device_id, state ORDER BY device_id, state"):
        print(f"  {r['device_id']:<16} {r['state']:<10} {r['c']:>7}  latest punch {r['m']}")
    rows = store.db.execute("SELECT device_id, user_id, ts, state, attempts, last_error FROM punches "
                            "WHERE state IN ('failed','unmapped') ORDER BY ts DESC LIMIT 20").fetchall()
    if rows:
        print("\nLatest problems (max 20):")
        for r in rows:
            print(f"  {r['ts']} {r['device_id']} user={r['user_id']} {r['state']} x{r['attempts']}: {r['last_error']}")
    print(f"\nQueue left: {store.queue_left(cfg)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="ZKTeco -> ERPNext/HRMS v16 Employee Checkin sync")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parent / "config.ini"))
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="read devices, show what would happen, change nothing")
    g.add_argument("--test-erp", action="store_true", help="check ERP URL, API keys, HRMS, employee mapping")
    g.add_argument("--test-devices", action="store_true", help="connect to each device and print its info")
    g.add_argument("--list-users", action="store_true", help="list device users and their ERP mapping")
    g.add_argument("--status", action="store_true", help="show local sync state")
    g.add_argument("--retry-failed", action="store_true", help="requeue failed and unmapped punches now")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    setup_logging(cfg["log_dir"], args.verbose)

    if args.test_erp:
        return cmd_test_erp(cfg)
    if args.test_devices:
        return cmd_test_devices(cfg)
    if args.list_users:
        return cmd_list_users(cfg)
    if args.status:
        return cmd_status(cfg)

    lock = acquire_lock(cfg["data_dir"])
    if lock is None:
        log.warning("previous run still in progress, skipping this one")
        return 0
    try:
        if args.retry_failed:
            n = Store(cfg["data_dir"] / "sync_state.db").requeue_failed()
            log.info("requeued %d failed/unmapped punches; they go out on the next run", n)
            return 0
        return run_sync(cfg, dry_run=args.dry_run)
    except SystemExit:
        raise
    except Exception:
        log.exception("sync run crashed")
        return 1
    finally:
        lock.close()


if __name__ == "__main__":
    sys.exit(main())
