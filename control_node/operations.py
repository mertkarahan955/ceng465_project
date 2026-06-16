"""Replication operations layer for CENG465 Single-Leader demo.

Core write functions (insert_item / update_item / delete_item) are collection-
agnostic: pass ``collection="vehicles"`` etc. to target any of the 6 fleet
collections. Default is ``"items"`` for backward compatibility.

Fleet domain layer sits on top: thin wrappers that build the correct key/value
shape and route to the right collection.  Access pattern query functions
demonstrate the three consistency models by reading from PRIMARY vs SECONDARY.
"""

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
_log_index_lock = threading.Lock()
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


# ── Write concern ──────────────────────────────────────────────────────────────

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


# ── Internal helpers ───────────────────────────────────────────────────────────

def _next_op():
    global _log_index, _log_index_initialized
    with _log_index_lock:
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
        raise RuntimeError(
            "w=majority requires the secondary to be reachable; "
            "write rejected before primary mutation"
        )


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


# ── Replication visibility checks ─────────────────────────────────────────────

def _secondary_has_state(
    target_id, version_after, log_index=None, operation_id=None, collection="items"
):
    """Return True if secondary already has version_after (or higher) for this doc."""
    secondary = db.get_secondary()
    doc = secondary[collection].find_one(
        {"_id": _coerce_target_id(target_id)},
        {"version": 1, "last_log_index": 1},
    )
    if not doc and operation_id:
        doc = secondary[collection].find_one(
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


def _poll_secondary(target_id, version_after, operation_id, collection="items"):
    deadline = time.time() + config.POLL_TIMEOUT_MS / 1000
    while time.time() < deadline:
        try:
            if _secondary_has_state(target_id, version_after, collection=collection):
                return datetime.now(timezone.utc)
        except PyMongoError:
            return None
        time.sleep(config.POLL_INTERVAL_MS / 1000)
    return None


# ── Operation log helpers ──────────────────────────────────────────────────────

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
    """Close pending logs for a document once the item table proves it is synced."""
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
            version_matches = (
                isinstance(version_after, Integral)
                and int(secondary_version) >= int(version_after)
            )
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


def _complete_log_visibility(log_id, target_id, version_after, leader_write_time, collection="items"):
    follower_visible_time = _poll_secondary(target_id, version_after, None, collection=collection)
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
    collection="items",
):
    return {
        "operation_id": operation_id,
        "leader_term": _leader_term,
        "log_index": log_index,
        "operation_type": operation_type,
        "target_collection": collection,
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


# ── Background reconciliation ──────────────────────────────────────────────────

def reconcile_pending_once(limit=None):
    """Complete pending/timeout log entries once the secondary catches up.

    Reads ``target_collection`` from each log so it queries the right collection
    (works for all 6 fleet collections + legacy "items").
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
        collection = log.get("target_collection", "items")
        if not target_id or not isinstance(version_after, Integral) or not leader_write_time:
            continue
        try:
            if _secondary_has_state(
                target_id, version_after, log_index, operation_id, collection=collection
            ):
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
    """Use the item tables as ground truth to close stale pending logs.

    Sweeps all 6 fleet collections + legacy "items" collection.
    """
    updated = 0
    secondary = db.get_secondary()
    all_collections = list(config.FLEET_COLLECTIONS) + ["items"]
    for coll in all_collections:
        try:
            cursor = secondary[coll].find(
                {},
                {"_id": 1, "version": 1, "last_log_index": 1},
            )
            for doc in cursor:
                updated += mark_pending_logs_visible_for_item(
                    doc["_id"],
                    doc.get("version"),
                    doc.get("last_log_index"),
                )
        except PyMongoError:
            continue
    return updated


# ── Secondary health ───────────────────────────────────────────────────────────

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


# ── Generic collection CRUD (collection-agnostic core) ────────────────────────

def insert_item(key: str, value: dict, client_id: str = "control_node", collection: str = "items"):
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

    result = primary[collection].with_options(write_concern=_wc()).insert_one(doc)
    target_id = result.inserted_id
    log_id = _write_log(_log_entry(
        operation_id, log_index, "insert", target_id, leader_write_time,
        None, 1, client_id, collection=collection,
    ))

    if _is_async_write():
        return target_id, None

    delay_ms = _complete_log_visibility(log_id, target_id, 1, leader_write_time, collection=collection)
    return target_id, delay_ms


def update_item(target_id, new_value: dict, client_id: str = "control_node", collection: str = "items"):
    primary = db.get_primary()
    _require_secondary_for_majority()
    operation_id, log_index = _next_op()

    leader_write_time = _now()

    # Atomic read-modify-write: $inc on version + $set in a single round-trip.
    # find_one + update_one would let concurrent threads read the same version
    # and produce the same version_after (lost update). find_one_and_update with
    # $inc eliminates that window — MongoDB serialises the increment internally.
    old_doc = primary[collection].with_options(write_concern=_wc()).find_one_and_update(
        {"_id": target_id},
        {"$inc": {"version": 1},
         "$set": {
             "value": new_value,
             "last_log_index": log_index,
             "last_operation_id": operation_id,
             "last_updated": leader_write_time,
             "leader_term": _leader_term,
         }},
        return_document=False,
    )
    if old_doc is None:
        raise ValueError(f"Item not found in {collection}: {target_id}")

    version_before = old_doc["version"]
    version_after  = version_before + 1
    log_id = _write_log(_log_entry(
        operation_id, log_index, "update", target_id, leader_write_time,
        version_before, version_after, client_id, collection=collection,
    ))

    if _is_async_write():
        return None

    delay_ms = _complete_log_visibility(log_id, target_id, version_after, leader_write_time, collection=collection)
    return delay_ms


def delete_item(target_id, client_id: str = "control_node", collection: str = "items"):
    primary = db.get_primary()
    _require_secondary_for_majority()
    operation_id, log_index = _next_op()

    current = primary[collection].find_one({"_id": target_id})
    if not current:
        raise ValueError(f"Item not found in {collection}: {target_id}")

    version_before = current["version"]
    version_after = version_before + 1
    leader_write_time = _now()

    primary[collection].with_options(write_concern=_wc()).update_one(
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
        version_before, version_after, client_id, collection=collection,
    ))

    if _is_async_write():
        return None

    delay_ms = _complete_log_visibility(log_id, target_id, version_after, leader_write_time, collection=collection)
    return delay_ms


# ── Fleet domain insert helpers ────────────────────────────────────────────────
# Each function builds the structured value dict and routes to the right collection.

def insert_vehicle(vehicle_id, plate, vehicle_type, max_payload_kg, manufacture_year,
                   client_id="control_node"):
    return insert_item(
        key=f"VHC-{vehicle_id}",
        value={
            "vehicle_id": vehicle_id,
            "plate": plate,
            "vehicle_type": vehicle_type,        # truck / van / motorcycle
            "max_payload_kg": max_payload_kg,
            "manufacture_year": manufacture_year,
            "is_active": True,
        },
        client_id=client_id,
        collection="vehicles",
    )


def insert_driver(driver_id, name, license_class, phone, assigned_vehicle_id=None,
                  client_id="control_node"):
    return insert_item(
        key=f"DRV-{driver_id}",
        value={
            "driver_id": driver_id,
            "name": name,
            "license_class": license_class,      # B / C / D / E
            "phone": phone,
            "assigned_vehicle_id": assigned_vehicle_id,
        },
        client_id=client_id,
        collection="drivers",
    )


def insert_depot(depot_id, name, city, lat, lng, capacity_vehicles,
                 client_id="control_node"):
    return insert_item(
        key=f"DEP-{depot_id}",
        value={
            "depot_id": depot_id,
            "name": name,
            "city": city,
            "lat": lat,
            "lng": lng,
            "capacity_vehicles": capacity_vehicles,
        },
        client_id=client_id,
        collection="depots",
    )


def insert_shipment(shipment_id, origin_depot, destination_depot, customer,
                    weight_kg, package_count, status="pending",
                    assigned_vehicle_id=None, client_id="control_node"):
    return insert_item(
        key=f"SHP-{shipment_id}",
        value={
            "shipment_id": shipment_id,
            "origin_depot": origin_depot,
            "destination_depot": destination_depot,
            "customer": customer,
            "weight_kg": weight_kg,
            "package_count": package_count,
            "status": status,                    # pending / in_transit / delivered / cancelled
            "assigned_vehicle_id": assigned_vehicle_id,
        },
        client_id=client_id,
        collection="shipments",
    )


def insert_position(vehicle_id, lat, lng, city, district, speed_kmh,
                    client_id="control_node"):
    return insert_item(
        key=f"POS-{vehicle_id}-{int(time.time() * 1000)}",
        value={
            "vehicle_id": vehicle_id,
            "lat": lat,
            "lng": lng,
            "city": city,
            "district": district,
            "speed_kmh": speed_kmh,
        },
        client_id=client_id,
        collection="positions",
    )


def insert_incident(vehicle_id, incident_type, severity, description,
                    lat=None, lng=None, client_id="control_node"):
    return insert_item(
        key=f"INC-{vehicle_id}-{int(time.time() * 1000)}",
        value={
            "vehicle_id": vehicle_id,
            "incident_type": incident_type,      # breakdown / accident / delay / fuel_low
            "severity": severity,                # low / medium / high / critical
            "description": description,
            "lat": lat,
            "lng": lng,
            "resolved": False,
        },
        client_id=client_id,
        collection="incidents",
    )


# ── Fleet access pattern queries ───────────────────────────────────────────────
# These demonstrate the three consistency models used in the experiments.

def get_fleet_overview(limit=20):
    """All vehicles + their latest position.

    Reads from SECONDARY — demonstrates Eventual Consistency.
    Stale vehicle positions (secondary lag) are the expected observation.
    """
    secondary = db.get_secondary()
    vehicles = list(
        secondary["vehicles"].find({"deleted": False})
        .sort("last_updated", -1)
        .limit(limit)
    )
    result = []
    for v in vehicles:
        vid = v.get("value", {}).get("vehicle_id")
        pos = None
        if vid:
            pos = secondary["positions"].find_one(
                {"value.vehicle_id": vid, "deleted": False},
                sort=[("last_updated", -1)],
            )
        result.append({"vehicle": v, "latest_position": pos})
    return result


def get_vehicle_history(vehicle_id, limit=10):
    """Position history for one vehicle (sorted newest-first).

    Reads from SECONDARY — demonstrates Monotonic Reads.
    Version list from secondary must be non-decreasing.
    """
    secondary = db.get_secondary()
    return list(
        secondary["positions"].find(
            {"value.vehicle_id": vehicle_id, "deleted": False},
            sort=[("last_updated", -1)],
        ).limit(limit)
    )


def get_current_position(vehicle_id):
    """Latest recorded position for a vehicle.

    Reads from PRIMARY — demonstrates Read-After-Write.
    Dispatcher writes a position update, then reads immediately to confirm.
    """
    primary = db.get_primary()
    return primary["positions"].find_one(
        {"value.vehicle_id": vehicle_id, "deleted": False},
        sort=[("last_updated", -1)],
    )


def get_active_shipments(limit=20):
    """All shipments that are not yet delivered.

    Reads from SECONDARY — eventual consistency is acceptable for fleet overview.
    """
    secondary = db.get_secondary()
    return list(
        secondary["shipments"].find(
            {"value.status": {"$ne": "delivered"}, "deleted": False},
            sort=[("last_updated", -1)],
        ).limit(limit)
    )


def get_open_incidents(limit=20):
    """All unresolved incidents ordered by severity.

    Reads from PRIMARY — demonstrates Read-After-Write for safety-critical data.
    A dispatcher who files an incident must see it immediately; reading stale
    secondary data could hide a critical incident.
    """
    primary = db.get_primary()
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    incidents = list(
        primary["incidents"].find(
            {"value.resolved": False, "deleted": False},
            sort=[("last_updated", -1)],
        ).limit(limit)
    )
    # Sort by severity in Python (avoids a custom sort index for demo)
    incidents.sort(key=lambda d: severity_order.get(d.get("value", {}).get("severity", "low"), 9))
    return incidents


def _serialize_doc(doc):
    """Convert a MongoDB document to a JSON-serialisable dict."""
    if doc is None:
        return None
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out
