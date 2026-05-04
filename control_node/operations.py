import uuid
import time
import threading
from numbers import Integral
from datetime import datetime, timezone
from pymongo import WriteConcern
from pymongo.errors import PyMongoError

import db
import config

_leader_term = 1
_log_index = 0
_log_index_initialized = False
_write_concern = "majority"   # toggled via dashboard; "majority" (sync) or 1 (async)
_reconciler_started = False
_reconciler_lock = threading.Lock()
_health_lock = threading.Lock()
_pending_statuses = ("pending_follower", "timeout")
_secondary_health = {
    "reachable": False,
    "last_checked_at": None,
    "last_ok_at": None,
    "last_failure_at": None,
    "last_error": None,
    "state_changed_at": None,
    "consecutive_successes": 0,
    "consecutive_failures": 0,
    "pending_count": 0,
    "last_reconcile_at": None,
    "last_reconcile_count": 0,
}


def set_write_concern(w):
    """Accepts 'majority', 1, or '1'. Numeric values are stored as int so that
    PyMongo sends them as a numeric w-value rather than a custom tag name."""
    global _write_concern
    if isinstance(w, str) and w.isdigit():
        w = int(w)
    if w != "majority" and not isinstance(w, int):
        raise ValueError(f"invalid write concern: {w!r}")
    _write_concern = w


def get_write_concern():
    return _write_concern


def _next_op():
    global _log_index, _log_index_initialized
    if not _log_index_initialized:
        last = db.get_primary()["operation_logs"].find_one(sort=[("log_index", -1)])
        _log_index = last.get("log_index", 0) if last else 0
        _log_index_initialized = True
    _log_index += 1
    return str(uuid.uuid4()), _log_index


def _now():
    return datetime.now(timezone.utc)


def _wc():
    return WriteConcern(w=_write_concern, wtimeout=config.WRITE_TIMEOUT_MS)


def _is_async_write():
    return _write_concern == 1


def _secondary_has_version(target_id, version_after):
    secondary = db.get_secondary()
    doc = secondary["items"].find_one(
        {"_id": target_id},
        {"version": 1},
    )
    if not doc:
        return False
    version = doc.get("version")
    if not isinstance(version, Integral) or not isinstance(version_after, Integral):
        return False
    return int(version) >= int(version_after)


def _poll_secondary(target_id, version_after, operation_id):
    deadline = time.time() + config.POLL_TIMEOUT_MS / 1000
    while time.time() < deadline:
        try:
            if _secondary_has_version(target_id, version_after):
                return datetime.now(timezone.utc)
        except PyMongoError:
            return None
        time.sleep(config.POLL_INTERVAL_MS / 1000)
    return None


def _write_log(entry: dict):
    return db.get_primary()["operation_logs"].insert_one(entry).inserted_id


def _mark_log_visible(log_id, leader_write_time):
    follower_visible_time = _now()
    delay_ms = (follower_visible_time - leader_write_time).total_seconds() * 1000
    db.get_primary()["operation_logs"].update_one(
        {"_id": log_id},
        {"$set": {
            "follower_visible_time": follower_visible_time,
            "replication_delay_ms": delay_ms,
            "status": "visible_on_follower",
        }},
    )
    return delay_ms


def _mark_log_timeout(log_id):
    db.get_primary()["operation_logs"].update_one(
        {"_id": log_id},
        {"$set": {"status": "timeout"}},
    )


def _complete_log_visibility(log_id, target_id, version_after, leader_write_time):
    follower_visible_time = _poll_secondary(target_id, version_after, None)
    if not follower_visible_time:
        _mark_log_timeout(log_id)
        return None

    delay_ms = (follower_visible_time - leader_write_time).total_seconds() * 1000
    db.get_primary()["operation_logs"].update_one(
        {"_id": log_id},
        {"$set": {
            "follower_visible_time": follower_visible_time,
            "replication_delay_ms": delay_ms,
            "status": "visible_on_follower",
        }},
    )
    return delay_ms


def _log_entry(
    operation_id,
    log_index,
    operation_type,
    target_id,
    leader_write_time,
    version_before,
    version_after,
    client_id,
):
    return {
        "operation_id": operation_id,
        "leader_term": _leader_term,
        "log_index": log_index,
        "operation_type": operation_type,
        "target_collection": "items",
        "target_id": target_id,
        "leader_write_time": leader_write_time,
        "follower_visible_time": None,
        "replication_delay_ms": None,
        "version_before": version_before,
        "version_after": version_after,
        "client_id": client_id,
        "write_concern": _write_concern,
        "status": "pending_follower",
    }


def reconcile_pending_once(limit=None):
    """Complete pending/timeout log entries once the secondary catches up.

    This is the dashboard's async recovery path for w=1 writes. The write
    request records a durable operation log on the primary and returns
    immediately; this function later observes the secondary and fills in the
    follower visibility fields.
    """
    limit = limit or config.RECONCILE_BATCH_SIZE
    primary = db.get_primary()
    logs = primary["operation_logs"].find({
        "status": {"$in": list(_pending_statuses)},
        "follower_visible_time": None,
    }).sort("log_index", 1).limit(limit)

    updated = 0
    try:
        db.get_secondary().command("ping")
    except PyMongoError:
        return updated

    for log in logs:
        target_id = log.get("target_id")
        version_after = log.get("version_after")
        leader_write_time = log.get("leader_write_time")
        if not target_id or not isinstance(version_after, Integral) or not leader_write_time:
            continue
        try:
            if _secondary_has_version(target_id, version_after):
                _mark_log_visible(log["_id"], leader_write_time)
                updated += 1
        except PyMongoError:
            break
    return updated


def pending_log_count():
    return db.get_primary()["operation_logs"].count_documents({
        "status": {"$in": list(_pending_statuses)},
        "follower_visible_time": None,
    })


def drain_pending_logs(max_batches=None):
    """Process pending logs in bounded batches after the secondary recovers."""
    max_batches = max_batches or config.RECONCILE_DRAIN_BATCHES
    total_updated = 0

    for _ in range(max_batches):
        pending_before = pending_log_count()
        if pending_before == 0:
            break

        updated = reconcile_pending_once(limit=config.RECONCILE_BATCH_SIZE)
        total_updated += updated

        if updated == 0:
            break

    return total_updated


def _set_secondary_health(reachable, checked_at, error=None, reconciled=0):
    with _health_lock:
        previous = _secondary_health["reachable"]
        if previous != reachable:
            _secondary_health["state_changed_at"] = checked_at

        _secondary_health["reachable"] = reachable
        _secondary_health["last_checked_at"] = checked_at
        _secondary_health["last_error"] = None if reachable else str(error)

        if reachable:
            _secondary_health["last_ok_at"] = checked_at
            _secondary_health["consecutive_successes"] += 1
            _secondary_health["consecutive_failures"] = 0
        else:
            _secondary_health["last_failure_at"] = checked_at
            _secondary_health["consecutive_failures"] += 1
            _secondary_health["consecutive_successes"] = 0

        _secondary_health["last_reconcile_at"] = checked_at
        _secondary_health["last_reconcile_count"] = reconciled
        try:
            _secondary_health["pending_count"] = pending_log_count()
        except PyMongoError:
            _secondary_health["pending_count"] = None


def mark_secondary_reachable(reconciled=0):
    _set_secondary_health(True, _now(), reconciled=reconciled)


def get_secondary_health():
    with _health_lock:
        return dict(_secondary_health)


def _healthcheck_once():
    checked_at = _now()
    reconciled = 0

    try:
        db.get_secondary().command("ping")
        _set_secondary_health(True, checked_at, reconciled=reconciled)
        reconciled = drain_pending_logs()
        _set_secondary_health(True, _now(), reconciled=reconciled)
    except PyMongoError as exc:
        _set_secondary_health(False, checked_at, error=exc)


def refresh_secondary_health_once():
    _healthcheck_once()
    return get_secondary_health()


def _healthcheck_loop():
    while True:
        try:
            _healthcheck_once()
        except Exception:
            pass
        time.sleep(config.HEALTHCHECK_INTERVAL_MS / 1000)


def _reconciler_loop():
    while True:
        try:
            reconcile_pending_once()
        except Exception:
            pass
        time.sleep(config.RECONCILE_INTERVAL_MS / 1000)


def start_reconciler():
    global _reconciler_started
    with _reconciler_lock:
        if _reconciler_started:
            return
        reconcile_thread = threading.Thread(
            target=_reconciler_loop,
            name="replication-log-reconciler",
            daemon=True,
        )
        health_thread = threading.Thread(
            target=_healthcheck_loop,
            name="secondary-healthcheck",
            daemon=True,
        )
        reconcile_thread.start()
        health_thread.start()
        _reconciler_started = True


def insert_item(key: str, value: dict, client_id: str = "control_node"):
    primary = db.get_primary()
    operation_id, log_index = _next_op()
    leader_write_time = _now()

    doc = {
        "key": key,
        "value": value,
        "version": 1,
        "leader_term": _leader_term,
        "last_log_index": log_index,
        "last_operation_id": operation_id,
        "last_updated": leader_write_time,
        "deleted": False,
        "created_by": client_id,
    }

    result = primary["items"].with_options(write_concern=_wc()).insert_one(doc)
    target_id = result.inserted_id
    log_id = _write_log(_log_entry(
        operation_id, log_index, "insert", target_id, leader_write_time,
        None, 1, client_id,
    ))

    if _is_async_write():
        return target_id, None

    delay_ms = _complete_log_visibility(log_id, target_id, 1, leader_write_time)

    return target_id, delay_ms


def update_item(target_id, new_value: dict, client_id: str = "control_node"):
    primary = db.get_primary()
    operation_id, log_index = _next_op()

    current = primary["items"].find_one({"_id": target_id})
    if not current:
        raise ValueError(f"Item not found: {target_id}")

    version_before = current["version"]
    version_after = version_before + 1
    leader_write_time = _now()

    primary["items"].with_options(write_concern=_wc()).update_one(
        {"_id": target_id},
        {"$set": {
            "value": new_value,
            "version": version_after,
            "last_log_index": log_index,
            "last_operation_id": operation_id,
            "last_updated": leader_write_time,
            "leader_term": _leader_term,
        }}
    )
    log_id = _write_log(_log_entry(
        operation_id, log_index, "update", target_id, leader_write_time,
        version_before, version_after, client_id,
    ))

    if _is_async_write():
        return None

    delay_ms = _complete_log_visibility(log_id, target_id, version_after, leader_write_time)

    return delay_ms


def delete_item(target_id, client_id: str = "control_node"):
    primary = db.get_primary()
    operation_id, log_index = _next_op()

    current = primary["items"].find_one({"_id": target_id})
    if not current:
        raise ValueError(f"Item not found: {target_id}")

    version_before = current["version"]
    version_after = version_before + 1
    leader_write_time = _now()

    primary["items"].with_options(write_concern=_wc()).update_one(
        {"_id": target_id},
        {"$set": {
            "deleted": True,
            "version": version_after,
            "last_log_index": log_index,
            "last_operation_id": operation_id,
            "last_updated": leader_write_time,
            "leader_term": _leader_term,
        }}
    )
    log_id = _write_log(_log_entry(
        operation_id, log_index, "delete", target_id, leader_write_time,
        version_before, version_after, client_id,
    ))

    if _is_async_write():
        return None

    delay_ms = _complete_log_visibility(log_id, target_id, version_after, leader_write_time)

    return delay_ms
