"""Experiment 5 — Concurrent Writes via Atomic $inc.

Spec:
  Objective: Show that concurrent increments to the SAME numeric field are
             preserved when PRIMARY applies them with MongoDB's atomic $inc.
  Steps:
    - Insert one shared vehicle document synchronously.
    - User A and User B each send concurrent w=1 "load cargo" requests.
    - Every request increments vehicles.value.capacity by +100.
    - Read SECONDARY right after the burst, then after full convergence.
  Expected: PRIMARY serialises every increment, version rises monotonically,
            and final capacity equals initial_capacity + total_increments*100.
            SECONDARY may lag briefly, but must converge to the same total.
"""

import threading
import time

import config
import db
import operations

from .common import (
    measure_net_one_way,
    ms,
    replicate_ack_events,
    req_resp_events,
    safe_lat,
    serialize_log,
    write_concern,
)

CW_ACTORS = {
    "user_a":    {"label": "User A",    "color": "#79c0ff", "ip": "(thread A)"},
    "user_b":    {"label": "User B",    "color": "#f0883e", "ip": "(thread B)"},
    "primary":   {"label": "PRIMARY",   "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY", "color": "#a371f7", "ip": "192.168.88.70"},
}
CW_ACTOR_ORDER = ["user_a", "user_b", "primary", "secondary"]

WRITES_PER_USER = 2
CAPACITY_INCREMENT = 100
INITIAL_CAPACITY = 5000
SHARED_VEHICLE_ID = "CW-SHARED"


def _user_thread(user_id: str, t0: float, item_id, out: list, lock: threading.Lock):
    """Fire WRITES_PER_USER atomic capacity increments on the shared document."""
    for seq in range(1, WRITES_PER_USER + 1):
        t_send = ms() - t0
        try:
            result = operations.increment_value_field(
                item_id,
                "capacity",
                CAPACITY_INCREMENT,
                client_id=user_id,
                collection="vehicles",
            )
        except Exception:
            continue
        t_recv = ms() - t0
        with lock:
            out.append({
                "user": user_id,
                "seq": seq,
                "t_send": t_send,
                "t_recv": t_recv,
                "delta": CAPACITY_INCREMENT,
                "capacity_before": result["value_before"],
                "capacity_after": result["value_after"],
                "version_before": result["version_before"],
                "version_after": result["version_after"],
                "log_index": result["log_index"],
            })


def _poller_thread(t0: float, item_id, targets: set, out: dict, lock: threading.Lock):
    """Record when SECONDARY first reaches each target version."""
    pending = set(targets)
    deadline = time.time() + 1.5
    while pending and time.time() < deadline:
        doc = db.get_secondary()["vehicles"].find_one({"_id": item_id})
        if doc:
            version = doc.get("version", 0)
            for target in sorted(list(pending)):
                if version >= target:
                    with lock:
                        out[target] = ms() - t0
                    pending.discard(target)
        time.sleep(0.003)


def _wait_for_secondary_version(item_id, expected_version: int, timeout_s: float = 3.0):
    deadline = time.time() + timeout_s
    last_doc = None
    while time.time() < deadline:
        last_doc = db.get_secondary()["vehicles"].find_one({"_id": item_id})
        if last_doc and last_doc.get("version", 0) >= expected_version:
            return last_doc
        time.sleep(0.02)
    return last_doc


def run_concurrent_writes():
    """Two users load cargo concurrently; $inc preserves every +100 capacity step."""
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    item_id, _ = operations.insert_vehicle(
        SHARED_VEHICLE_ID, "34 CWSHRD", "truck", INITIAL_CAPACITY, 2020,
    )

    results_a: list = []
    results_b: list = []
    poll_result: dict = {}
    lock = threading.Lock()

    total_writes = 2 * WRITES_PER_USER
    expected_final_version = 1 + total_writes
    target_versions = set(range(2, expected_final_version + 1))

    with write_concern(1):
        t0 = ms()

        thread_a = threading.Thread(
            target=_user_thread, args=("user_a", t0, item_id, results_a, lock), daemon=True,
        )
        thread_b = threading.Thread(
            target=_user_thread, args=("user_b", t0, item_id, results_b, lock), daemon=True,
        )
        poller = threading.Thread(
            target=_poller_thread, args=(t0, item_id, target_versions, poll_result, lock), daemon=True,
        )
        thread_a.start()
        thread_b.start()
        poller.start()
        thread_a.join()
        thread_b.join()
        poller.join()

    all_writes = sorted(results_a + results_b, key=lambda w: (w["log_index"], w["t_send"]))
    total_writes = len(all_writes)
    expected_final_version = 1 + total_writes
    expected_total_increment = total_writes * CAPACITY_INCREMENT
    expected_final_capacity = INITIAL_CAPACITY + expected_total_increment

    repl_delays = []
    for w in all_writes:
        oplog_start_ms = safe_lat(w["t_send"] + net_p)
        applied_ms = poll_result.get(w["version_after"])
        if applied_ms is not None:
            secondary_applied_ms = max(applied_ms, oplog_start_ms + safe_lat(net_s))
            total_repl_ms = secondary_applied_ms - w["t_recv"]
        else:
            secondary_applied_ms = oplog_start_ms + safe_lat(net_s)
            total_repl_ms = secondary_applied_ms - w["t_recv"]
        repl_delays.append(total_repl_ms)
        w["oplog_start_ms"] = oplog_start_ms
        w["secondary_applied_ms"] = secondary_applied_ms
        w["total_repl_ms"] = total_repl_ms
        w["reached_secondary"] = applied_ms is not None

    real_delays = [d for d in repl_delays if d > 0]
    repl_ms = sum(real_delays) / len(real_delays) if real_delays else 0.0

    snap_t_start = ms() - t0
    snap_doc = db.get_secondary()["vehicles"].find_one({"_id": item_id})
    primary_snap = db.get_primary()["vehicles"].find_one({"_id": item_id})
    snap_t_end = ms() - t0

    snap_version = snap_doc.get("version") if snap_doc else 0
    primary_snap_version = primary_snap.get("version") if primary_snap else 0
    snap_capacity = snap_doc.get("value", {}).get("capacity") if snap_doc else None
    primary_snap_capacity = primary_snap.get("value", {}).get("capacity") if primary_snap else None
    snap_stale = snap_version < primary_snap_version
    visible_immediately = max(0, snap_version - 1)

    converged_secondary = _wait_for_secondary_version(item_id, expected_final_version)

    t_final_start = ms() - t0
    primary_final = db.get_primary()["vehicles"].find_one({"_id": item_id})
    secondary_final = converged_secondary or db.get_secondary()["vehicles"].find_one({"_id": item_id})
    t_final_end = ms() - t0

    primary_final_capacity = primary_final.get("value", {}).get("capacity") if primary_final else None
    secondary_final_capacity = secondary_final.get("value", {}).get("capacity") if secondary_final else None
    final_versions_match = (
        primary_final is not None
        and secondary_final is not None
        and secondary_final.get("version") == primary_final.get("version")
    )
    increments_preserved = (
        primary_final_capacity == expected_final_capacity
        and secondary_final_capacity == expected_final_capacity
    )
    order_preserved = final_versions_match and increments_preserved

    events = []

    def _write_events(writes, user_actor, letter):
        out = []
        for w in writes:
            v_before = w["version_before"]
            v_after = w["version_after"]
            req_data = {
                "vehicle_id": SHARED_VEHICLE_ID,
                "capacity": w["capacity_before"],
                "delta_capacity": w["delta"],
            }
            resp_data = {
                "vehicle_id": SHARED_VEHICLE_ID,
                "capacity": w["capacity_after"],
                "delta_capacity": w["delta"],
            }
            out += req_resp_events(
                w["t_send"], w["t_recv"], net_p,
                user_actor, "primary",
                f"load cargo +{w['delta']}kg (w=1) — {letter}{w['seq']}", "write",
                f"ok — capacity {w['capacity_before']} -> {w['capacity_after']}", "ok",
                req_meta={
                    "collection": "vehicles",
                    "db_action": "increment_capacity",
                    "document_id": str(item_id),
                    "version": f"v{v_before} -> v{v_after}",
                    "data_before": {"capacity": w["capacity_before"]},
                    "data": req_data,
                },
                resp_meta={
                    "collection": "vehicles",
                    "db_action": "increment_capacity",
                    "document_id": str(item_id),
                    "version": f"v{v_after}",
                    "data_before": {"capacity": w["capacity_before"]},
                    "data": resp_data,
                },
            )
            out += replicate_ack_events(
                w["oplog_start_ms"], w["secondary_applied_ms"], net_s,
                f"oplog (async) — v{v_after}, +{w['delta']}kg",
                f"✓ v{v_after} applied — capacity {w['capacity_after']}",
                replicate_meta={
                    "collection": "vehicles",
                    "db_action": "replicate_increment",
                    "document_id": str(item_id),
                    "version": f"v{v_before} -> v{v_after}",
                    "data_before": {"capacity": w["capacity_before"]},
                    "data": {"capacity": w["capacity_after"], "delta_capacity": w["delta"]},
                },
                ack_meta={
                    "collection": "vehicles",
                    "db_action": "applied_on_secondary",
                    "document_id": str(item_id),
                    "version": f"v{v_after}",
                    "data": {"capacity": w["capacity_after"]},
                },
            )
        return out

    events += _write_events(results_a, "user_a", "A")
    events += _write_events(results_b, "user_b", "B")

    events += req_resp_events(
        snap_t_start, snap_t_end, net_s,
        "user_a", "secondary",
        "read right after burst", "read",
        (
            f"capacity {snap_capacity} — lags PRIMARY ({primary_snap_capacity}) ⚡ async lag"
            if snap_stale else
            f"capacity {snap_capacity} — matches PRIMARY ✓"
        ),
        "stale_response" if snap_stale else "fresh_response",
        req_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id)},
        resp_meta={
            "collection": "vehicles",
            "db_action": "find",
            "document_id": str(item_id),
            "version": snap_version,
            "data": snap_doc.get("value") if snap_doc else None,
        },
    )

    events += req_resp_events(
        t_final_start, t_final_end, net_s,
        "user_b", "secondary",
        "final read — convergence check", "read",
        (
            f"✓ converged — capacity {secondary_final_capacity}"
            if order_preserved else
            f"⚡ divergence — capacity {secondary_final_capacity}"
        ),
        "fresh_response" if order_preserved else "stale_response",
        req_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id)},
        resp_meta={
            "collection": "vehicles",
            "db_action": "find",
            "document_id": str(item_id),
            "version": secondary_final.get("version") if secondary_final else None,
            "data": secondary_final.get("value") if secondary_final else None,
        },
    )

    events.sort(key=lambda e: e["t_ms"])

    log = db.get_primary()["operation_logs"].find_one(
        {"target_id": item_id},
        sort=[("log_index", -1)],
    )

    avg_a = (sum(w["t_recv"] - w["t_send"] for w in results_a) / len(results_a)) if results_a else 0
    avg_b = (sum(w["t_recv"] - w["t_send"] for w in results_b) / len(results_b)) if results_b else 0

    return {
        "experiment": "concurrent_writes",
        "title": "Concurrent Writes — Atomic $inc Capacity Growth",
        "description": (
            f"User A and User B each fire {WRITES_PER_USER} concurrent w=1 load requests to the same "
            f"vehicle. Every request does value.capacity += {CAPACITY_INCREMENT} with MongoDB $inc, "
            "so PRIMARY preserves every increment and SECONDARY eventually converges to the same total."
        ),
        "actors": CW_ACTORS,
        "actor_order": CW_ACTOR_ORDER,
        "events": events,
        "log": serialize_log(log),
        "summary": {
            "writes_per_user": WRITES_PER_USER,
            "total_writes": total_writes,
            "capacity_increment": CAPACITY_INCREMENT,
            "initial_capacity": INITIAL_CAPACITY,
            "total_increment_added": expected_total_increment,
            "expected_final_capacity": expected_final_capacity,
            "primary_final_capacity": primary_final_capacity,
            "secondary_final_capacity": secondary_final_capacity,
            "expected_final_version": expected_final_version,
            "actual_final_version": primary_final.get("version") if primary_final else None,
            "visible_immediately": visible_immediately,
            "visible_pct": round(visible_immediately / total_writes * 100) if total_writes else 0,
            "secondary_lagged": snap_stale,
            "avg_write_ms_user_a": round(avg_a, 2),
            "avg_write_ms_user_b": round(avg_b, 2),
            "replication_delay_ms": round(repl_ms, 2),
            "order_preserved": order_preserved,
            "order_violated": not order_preserved,
            "increments_preserved": increments_preserved,
            "final_value": primary_final.get("value") if primary_final else None,
            "consistency_model": "Concurrent Atomic Increments",
            "consistency_achieved": order_preserved,
        },
    }
