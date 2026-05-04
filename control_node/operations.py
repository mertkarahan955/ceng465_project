import uuid
import time
from datetime import datetime, timezone
from pymongo import WriteConcern

import db
import config

_leader_term = 1
_log_index = 0
_write_concern = "majority"   # toggled via dashboard


def set_write_concern(w: str):
    global _write_concern
    _write_concern = w


def get_write_concern() -> str:
    return _write_concern


def _next_op():
    global _log_index
    _log_index += 1
    return str(uuid.uuid4()), _log_index


def _now():
    return datetime.now(timezone.utc)


def _wc():
    return WriteConcern(w=_write_concern)


def _poll_secondary(target_id, version_after, operation_id):
    secondary = db.get_secondary()
    deadline = time.time() + config.POLL_TIMEOUT_MS / 1000
    while time.time() < deadline:
        doc = secondary["items"].find_one({"_id": target_id})
        if doc and doc.get("version") == version_after:
            return datetime.now(timezone.utc)
        time.sleep(config.POLL_INTERVAL_MS / 1000)
    return None


def _write_log(entry: dict):
    db.get_primary()["operation_logs"].insert_one(entry)


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

    follower_visible_time = _poll_secondary(target_id, 1, operation_id)
    delay_ms = None
    if follower_visible_time:
        delay_ms = (follower_visible_time - leader_write_time).total_seconds() * 1000

    _write_log({
        "operation_id": operation_id,
        "leader_term": _leader_term,
        "log_index": log_index,
        "operation_type": "insert",
        "target_collection": "items",
        "target_id": target_id,
        "leader_write_time": leader_write_time,
        "follower_visible_time": follower_visible_time,
        "replication_delay_ms": delay_ms,
        "version_before": None,
        "version_after": 1,
        "client_id": client_id,
        "write_concern": _write_concern,
        "status": "visible_on_follower" if follower_visible_time else "timeout",
    })

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

    follower_visible_time = _poll_secondary(target_id, version_after, operation_id)
    delay_ms = None
    if follower_visible_time:
        delay_ms = (follower_visible_time - leader_write_time).total_seconds() * 1000

    _write_log({
        "operation_id": operation_id,
        "leader_term": _leader_term,
        "log_index": log_index,
        "operation_type": "update",
        "target_collection": "items",
        "target_id": target_id,
        "leader_write_time": leader_write_time,
        "follower_visible_time": follower_visible_time,
        "replication_delay_ms": delay_ms,
        "version_before": version_before,
        "version_after": version_after,
        "client_id": client_id,
        "write_concern": _write_concern,
        "status": "visible_on_follower" if follower_visible_time else "timeout",
    })

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

    follower_visible_time = _poll_secondary(target_id, version_after, operation_id)
    delay_ms = None
    if follower_visible_time:
        delay_ms = (follower_visible_time - leader_write_time).total_seconds() * 1000

    _write_log({
        "operation_id": operation_id,
        "leader_term": _leader_term,
        "log_index": log_index,
        "operation_type": "delete",
        "target_collection": "items",
        "target_id": target_id,
        "leader_write_time": leader_write_time,
        "follower_visible_time": follower_visible_time,
        "replication_delay_ms": delay_ms,
        "version_before": version_before,
        "version_after": version_after,
        "client_id": client_id,
        "write_concern": _write_concern,
        "status": "visible_on_follower" if follower_visible_time else "timeout",
    })

    return delay_ms
