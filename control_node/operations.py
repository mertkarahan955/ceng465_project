import uuid
import time
import threading
from numbers import Integral
from datetime import datetime, timezone
from bson import ObjectId
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


def _as_aware_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _wc():
    if _is_async_write():
        return WriteConcern(w=_write_concern)
    return WriteConcern(w=_write_concern, wtimeout=config.WRITE_TIMEOUT_MS)


def _is_async_write():
    return _write_concern == 1


def _require_secondary_for_majority():
    if _is_async_write():
        return
    try:
        db.get_secondary().command("ping")
    except PyMongoError as exc:
        _set_secondary_health(False, _now(), error=exc)
        raise RuntimeError("w=majority requires the secondary to be reachable; write rejected before primary mutation")


def _coerce_target_id(target_id):
    if isinstance(target_id, ObjectId):
        return target_id
    if isinstance(target_id, str):
        try:
            return ObjectId(target_id)
        except Exception:
            return target_id
    if isinstance(target_id, dict) and "$oid" in target_id:
        try:
            return ObjectId(target_id["$oid"])
        except Exception:
            return target_id
    return target_id


def _secondary_has_state(target_id, version_after, log_index=None, operation_id=None):
    secondary = db.get_secondary()
    doc = secondary["items"].find_one(
        {"_id": _coerce_target_id(target_id)},
        {"version": 1, "last_log_index": 1},
    )
    if not doc and operation_id:
        doc = secondary["items"].find_one(
            {"last_operation_id": operation_id},
            {"version": 1, "last_log_index": 1},
        )
    if not doc:
        return False

    version = doc.get("version")
    if isinstance(version, Integral) and isinstance(version_after, Integral):
        if int(version) >= int(version_after):
            return True

    secondary_log_index = doc.get("last_log_index")
    if isinstance(secondary_log_index, Integral) and isinstance(log_index, Integral):
        return int(secondary_log_index) >= int(log_index)

    return False


def _poll_secondary(target_id, version_after, operation_id):
    deadline = time.time() + config.POLL_TIMEOUT_MS / 1000
    while time.time() < deadline:
        try:
            if _secondary_has_state(target_id, version_after):
                return datetime.now(timezone.utc)
        except PyMongoError:
            return None
        time.sleep(config.POLL_INTERVAL_MS / 1000)
    return None


def _write_log(entry: dict):
    return db.get_primary()["operation_logs"].insert_one(entry).inserted_id


def _mark_log_visible(log_id, leader_write_time):
    follower_visible_time = _now()
    leader_write_time = _as_aware_utc(leader_write_time)
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


def mark_pending_logs_visible_for_item(target_id, secondary_version, secondary_log_index=None):
    """Close pending logs for an item once the item table proves it is synced."""
    if not isinstance(secondary_version, Integral):
        return 0

    primary = db.get_primary()
    filters = [
        {"target_id": target_id},
        {"target_id": str(target_id)},
        {"target_id": _coerce_target_id(target_id)},
    ]

    updated = 0
    seen = set()
    for target_filter in filters:
        query = {
            **target_filter,
            "status": {"$in": list(_pending_statuses)},
            "follower_visible_time": None,
        }
        for log in primary["operation_logs"].find(query):
            log_id = log["_id"]
            if log_id in seen:
                continue
            seen.add(log_id)

            version_after = log.get("version_after")
            log_index = log.get("log_index")
            version_matches = isinstance(version_after, Integral) and int(secondary_version) >= int(version_after)
            log_matches = (
                isinstance(secondary_log_index, Integral)
                and isinstance(log_index, Integral)
                and int(secondary_log_index) >= int(log_index)
            )

            if version_matches or log_matches:
                _mark_log_visible(log_id, log["leader_write_time"])
                updated += 1

    return updated


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
        log_index = log.get("log_index")
        operation_id = log.get("operation_id")
        leader_write_time = log.get("leader_write_time")
        if not target_id or not isinstance(version_after, Integral) or not leader_write_time:
            continue
        try:
            if _secondary_has_state(target_id, version_after, log_index, operation_id):
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

        if updated == 0 and pending_log_count() >= pending_before:
            break

    return total_updated


def sweep_synced_items_from_secondary():
    """Use the item table as ground truth to close stale pending logs."""
    updated = 0
    secondary = db.get_secondary()
    cursor = secondary["items"].find(
        {},
        {"_id": 1, "version": 1, "last_log_index": 1},
    )
    for doc in cursor:
        updated += mark_pending_logs_visible_for_item(
            doc["_id"],
            doc.get("version"),
            doc.get("last_log_index"),
        )
    return updated


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
        reconciled += sweep_synced_items_from_secondary()
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
    _require_secondary_for_majority()
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
    _require_secondary_for_majority()
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
    _require_secondary_for_majority()
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
