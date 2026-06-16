"""Experiment 5 — Concurrent Writes (Extended Scenario).

Spec:
  Objective: Test how concurrent writes to the SAME record on the leader are
             propagated to followers.
  Steps:
    - Insert a shared vehicle document (synchronously, so both nodes have it).
    - Two users then fire concurrent w=1 UPDATES to that SAME document.
    - Read from the follower to check whether it converges to the same final
      value/version as the leader.
  Expected: PRIMARY serialises every concurrent update via a single, global
            log_index. SECONDARY applies the oplog in that exact order, so it
            must converge to the identical final value — never get "stuck" on
            an intermediate value from one of the two users.
  Observe:  Document how different consistency models (w=majority vs w=1) impact
            the visibility and convergence of concurrent writes on the follower.

Implementation:
  Two threads (User A and User B) fire updates to the SAME PRIMARY document
  simultaneously. PRIMARY serialises all writes internally — each update gets
  a unique log_index. SECONDARY applies the oplog in that log_index order, so
  the last update (by log_index) always wins on both nodes.
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

# 4-actor layout: two clients + primary + secondary
CW_ACTORS = {
    "user_a":    {"label": "User A",    "color": "#79c0ff", "ip": "(thread A)"},
    "user_b":    {"label": "User B",    "color": "#f0883e", "ip": "(thread B)"},
    "primary":   {"label": "PRIMARY",   "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY", "color": "#a371f7", "ip": "192.168.88.70"},
}
CW_ACTOR_ORDER = ["user_a", "user_b", "primary", "secondary"]

WRITES_PER_USER = 2   # each user fires this many w=1 updates concurrently
SHARED_VEHICLE_ID = "CW-SHARED"


def _vehicle_value(seq: int, who: str) -> dict:
    return {
        "vehicle_id": SHARED_VEHICLE_ID,
        "type": "truck",
        "capacity": 5000 + seq * 100,
        "year": 2020 + seq,
        "last_writer": who,
    }


def _user_thread(user_id: str, t0: float, item_id, out: list, lock: threading.Lock):
    """Worker: fire WRITES_PER_USER w=1 updates to the shared document."""
    for i in range(WRITES_PER_USER):
        value = _vehicle_value(i + 1, user_id)
        t_ws = ms() - t0
        try:
            operations.update_item(item_id, value, collection="vehicles")
        except Exception:
            continue
        t_we = ms() - t0
        with lock:
            out.append({
                "user":   user_id,
                "seq":    i + 1,
                "t_send": t_ws,
                "t_recv": t_we,
                "value":  value,
            })


def _poller_thread(t0: float, item_id, targets: set, out: dict, lock: threading.Lock):
    """Poll SECONDARY; for each target version, record the t0-relative time it first appears.

    Bounded to a short window: under LWW, a "lost update" can mean some
    target versions never appear (two writes compute the same version_after),
    so this must not block the experiment for long.
    """
    pending = set(targets)
    deadline = time.time() + 0.3
    while pending and time.time() < deadline:
        doc = db.get_secondary()["vehicles"].find_one({"_id": item_id})
        if doc:
            v = doc.get("version", 0)
            for target in sorted(pending):
                if v >= target:
                    with lock:
                        out[target] = ms() - t0
                    pending.discard(target)
        time.sleep(0.002)


def run_concurrent_writes():
    """Two concurrent users update the SAME document → PRIMARY serialises →
    SECONDARY converges to the identical final value.

    Setup — shared document:
      Insert one vehicle document with the default (majority) write concern,
      so both PRIMARY and SECONDARY have it before the concurrent burst.

    Phase A — concurrent w=1 update burst (2 threads):
      User A and User B each fire WRITES_PER_USER updates to that SAME
      document simultaneously. w=1 means PRIMARY does not wait for SECONDARY
      ACK — updates return fast. PRIMARY assigns a unique, monotonic
      log_index to each update internally. Each update's oplog entry starts
      propagating to SECONDARY the moment it lands on PRIMARY (its own
      replicate+ACK pair), not after the whole burst finishes.

    Phase B — read right after the burst:
      Right after both threads finish, read the document from SECONDARY.
      Its version tells us how many of the concurrent updates have already
      propagated (async lag).

    Phase C — full-sync + convergence check:
      Poll SECONDARY until it reaches the final version.
      Verify: SECONDARY's final value == PRIMARY's final value (last write,
      by log_index, wins on both nodes).
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    # ── Setup: insert the shared document both users will update ─────────────
    item_id, _ = operations.insert_vehicle(
        SHARED_VEHICLE_ID, "34 CWSHRD", "truck", 5000, 2020,
    )

    # ── Phase A: concurrent w=1 updates from two threads ──────────────────────
    results_a: list = []
    results_b: list = []
    poll_result: dict = {}
    lock = threading.Lock()

    final_version = 1 + 2 * WRITES_PER_USER   # v1 from the initial insert + 2*WRITES_PER_USER updates
    target_versions = set(range(2, final_version + 1))

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

    all_writes   = sorted(results_a + results_b, key=lambda w: w["t_send"])
    total_writes = len(all_writes)

    # ── Resolve per-write oplog replication timing ────────────────────────────
    # Each write's oplog entry starts propagating the moment the write request
    # lands on PRIMARY, not when the w=1 ack returns to the client.
    fallback_repl_ms = 0.0
    repl_delays = []
    for i, w in enumerate(all_writes):
        v = i + 2   # v1 is the initial insert; writes produce v2..vFINAL in arrival order
        oplog_start_ms = safe_lat(w["t_send"] + net_p)
        applied_ms     = poll_result.get(v)
        if applied_ms is not None:
            secondary_applied_ms = max(applied_ms, oplog_start_ms + safe_lat(net_s))
            total_repl_ms        = secondary_applied_ms - w["t_recv"]
        else:
            total_repl_ms        = fallback_repl_ms
            secondary_applied_ms = oplog_start_ms + total_repl_ms
        repl_delays.append(total_repl_ms)
        w["v"]                     = v
        w["oplog_start_ms"]        = oplog_start_ms
        w["secondary_applied_ms"]  = secondary_applied_ms
        w["total_repl_ms"]         = total_repl_ms
        w["reached_secondary"]     = applied_ms is not None

    real_delays = [d for d in repl_delays if d > 0]
    repl_ms = sum(real_delays) / len(real_delays) if real_delays else fallback_repl_ms

    # ── Phase B: read SECONDARY right after the burst ─────────────────────────
    # Read PRIMARY and SECONDARY at the same moment so "stale" means
    # "secondary lags PRIMARY right now", not "secondary lags theoretical final".
    snap_t_start         = ms() - t0
    snap_doc             = db.get_secondary()["vehicles"].find_one({"_id": item_id})
    primary_snap         = db.get_primary()["vehicles"].find_one({"_id": item_id})
    snap_t_end           = ms() - t0

    snap_version         = snap_doc.get("version") if snap_doc else 0
    primary_snap_version = primary_snap.get("version") if primary_snap else 0
    snap_stale           = snap_version < primary_snap_version
    visible_immediately  = max(0, snap_version - 1)

    # Final read on both nodes: did they converge on the same value?
    t_final_start   = ms() - t0
    primary_final   = db.get_primary()["vehicles"].find_one({"_id": item_id})
    secondary_final = db.get_secondary()["vehicles"].find_one({"_id": item_id})
    t_final_end     = ms() - t0

    order_preserved = (
        primary_final is not None
        and secondary_final is not None
        and secondary_final.get("version") == primary_final.get("version")
        and secondary_final.get("value") == primary_final.get("value")
    )

    # ── Build timeline events ─────────────────────────────────────────────────
    events = []

    # Phase A: each user's updates on their own lane, each followed by its own
    # oplog replicate + ACK pair to SECONDARY.
    def _write_events(writes, user_actor, letter):
        out = []
        for w in writes:
            v = w["v"]
            out += req_resp_events(
                w["t_send"], w["t_recv"], net_p,
                user_actor, "primary",
                f"update vehicle (w=1) — {letter}{w['seq']}", "write",
                f"ok — {letter}{w['seq']}", "ok",
                req_meta={"collection": "vehicles", "db_action": "update",
                          "document_id": str(item_id), "version": f"v{v - 1} -> v{v}", "data": w["value"]},
                resp_meta={"collection": "vehicles", "db_action": "update",
                           "document_id": str(item_id), "version": f"v{v}", "data": w["value"]},
            )
            if w["reached_secondary"]:
                out += replicate_ack_events(
                    w["oplog_start_ms"], w["secondary_applied_ms"], net_s,
                    f"oplog (async) — v{v}",
                    f"✓ v{v} applied — {w['total_repl_ms']:.0f}ms after write",
                    replicate_meta={"collection": "vehicles", "db_action": "replicate_update",
                                    "document_id": str(item_id), "version": f"v{v - 1} -> v{v}", "data": w["value"]},
                    ack_meta={"collection": "vehicles", "db_action": "applied_on_secondary",
                              "document_id": str(item_id), "version": f"v{v}", "data": w["value"]},
                )
            else:
                # This version was overwritten on PRIMARY before reaching SECONDARY (lost update).
                out.append({
                    "t_ms":        w["oplog_start_ms"],
                    "latency_ms":  safe_lat(net_s),
                    "from":        "primary",
                    "to":          "secondary",
                    "label":       f"oplog (async) — v{v} ⚡ lost update",
                    "type":        "lost_update",
                    "collection":  "vehicles",
                    "db_action":   "replicate_update",
                    "document_id": str(item_id),
                    "version":     f"v{v - 1} -> v{v}",
                    "data":        w["value"],
                })
        return out

    events += _write_events(results_a, "user_a", "A")
    events += _write_events(results_b, "user_b", "B")

    # Phase B: read from secondary right after the burst (use user_a lane as the "reader")
    events += req_resp_events(
        snap_t_start, snap_t_end, net_s,
        "user_a", "secondary",
        "read right after burst", "read",
        (
            f"v{snap_version} — lags PRIMARY (v{primary_snap_version}) ⚡ async lag"
            if snap_stale else
            f"v{snap_version} — matches PRIMARY ✓"
        ),
        "stale_response" if snap_stale else "fresh_response",
        req_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id)},
        resp_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id),
                   "version": snap_version, "data": snap_doc.get("value") if snap_doc else None},
    )

    # Phase C: final convergence read (use user_b lane)
    events += req_resp_events(
        t_final_start, t_final_end, net_s,
        "user_b", "secondary",
        "final read — convergence check", "read",
        "✓ converged with PRIMARY" if order_preserved else "⚡ DIVERGED from PRIMARY",
        "fresh_response" if order_preserved else "stale_response",
        req_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id)},
        resp_meta={"collection": "vehicles", "db_action": "find", "document_id": str(item_id),
                   "version": secondary_final.get("version") if secondary_final else None,
                   "data": secondary_final.get("value") if secondary_final else None},
    )

    events.sort(key=lambda e: e["t_ms"])

    log = db.get_primary()["operation_logs"].find_one(
        {"target_id": item_id},
        sort=[("log_index", -1)],
    )

    # Average w=1 write duration per user
    avg_a = (sum(w["t_recv"] - w["t_send"] for w in results_a) / len(results_a)) if results_a else 0
    avg_b = (sum(w["t_recv"] - w["t_send"] for w in results_b) / len(results_b)) if results_b else 0

    return {
        "experiment":  "concurrent_writes",
        "title":       "Concurrent Writes — Last-Write-Wins Convergence",
        "description": (
            f"User A and User B each fire {WRITES_PER_USER} w=1 updates to the SAME "
            "vehicle document simultaneously. PRIMARY serialises all updates "
            "(log_index). SECONDARY must converge to the identical final value."
        ),
        "actors":      CW_ACTORS,
        "actor_order": CW_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "summary": {
            "writes_per_user":      WRITES_PER_USER,
            "total_writes":         total_writes,
            "expected_final_version": final_version,
            "actual_final_version":   primary_snap_version,
            "lost_updates":           final_version - primary_snap_version,
            "visible_immediately":    visible_immediately,
            "visible_pct":            round(visible_immediately / primary_snap_version * 100) if primary_snap_version else 0,
            "secondary_lagged":       snap_stale,
            "avg_write_ms_user_a":  round(avg_a, 2),
            "avg_write_ms_user_b":  round(avg_b, 2),
            "replication_delay_ms": round(repl_ms, 2),
            "order_preserved":      order_preserved,
            "order_violated":       not order_preserved,
            "final_value":          primary_final.get("value") if primary_final else None,
            "consistency_model":    "Concurrent Writes",
            "consistency_achieved": order_preserved,
        },
    }
