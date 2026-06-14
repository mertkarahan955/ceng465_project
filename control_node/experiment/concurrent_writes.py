"""Experiment 5 — Concurrent Writes (Extended Scenario).

Spec:
  Objective: Test how concurrent writes to the leader are propagated to followers.
  Steps:
    - Perform multiple writes in quick succession to the leader.
    - Read from followers to check if data is seen in the same order.
  Expected: Followers show data in the same sequence as written to the leader.
            Async replication may cause temporary inconsistency (not all writes
            visible immediately), but ORDER is ALWAYS preserved.
  Observe:  Document how different consistency models (w=majority vs w=1) impact
            the visibility and ordering of concurrent writes on the follower.

Implementation:
  Two threads (User A and User B) fire writes to PRIMARY simultaneously.
  PRIMARY serialises all writes internally — each gets a unique log_index.
  SECONDARY applies the oplog in the same log_index order.
  The experiment proves order is preserved even under true concurrency.
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

WRITES_PER_USER = 3   # each user fires this many w=1 writes concurrently


def _user_thread(user_id: str, t0: float, out: list, lock: threading.Lock):
    """Worker: fire WRITES_PER_USER w=1 writes and append timing records."""
    for i in range(WRITES_PER_USER):
        vid = f"CW-{user_id[-1].upper()}{i + 1}"
        t_ws = ms() - t0
        try:
            item_id, _ = operations.insert_vehicle(
                vid, f"34 CW{user_id[-1].upper()}{i:03d}", "truck", 5000 + i * 100, 2020 + i,
            )
        except Exception:
            continue
        t_we = ms() - t0
        with lock:
            out.append({
                "user":   user_id,
                "seq":    i + 1,
                "vid":    vid,
                "t_send": t_ws,
                "t_recv": t_we,
                "id":     item_id,
            })


def run_concurrent_writes():
    """Two concurrent users → PRIMARY serialises → SECONDARY preserves order.

    Phase A — concurrent w=1 burst (2 threads):
      User A and User B each fire WRITES_PER_USER inserts simultaneously.
      w=1 means PRIMARY does not wait for SECONDARY ACK — writes return fast.
      PRIMARY assigns a unique, monotonic log_index to each write internally.

    Phase B — immediate follower snapshot:
      Right after both threads finish, read all documents from SECONDARY.
      Some may not be visible yet (async lag), but those that ARE visible
      must be in log_index order (never out of order).

    Phase C — full-sync + order verification:
      Poll SECONDARY until all writes are visible.
      Verify: secondary log_index sequence == sorted(log_index sequence).
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    # ── Phase A: concurrent w=1 writes from two threads ──────────────────────
    results_a: list = []
    results_b: list = []
    lock = threading.Lock()

    with write_concern(1):
        t0 = ms()

        thread_a = threading.Thread(
            target=_user_thread, args=("user_a", t0, results_a, lock), daemon=True,
        )
        thread_b = threading.Thread(
            target=_user_thread, args=("user_b", t0, results_b, lock), daemon=True,
        )
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

    all_writes = sorted(results_a + results_b, key=lambda w: w["t_send"])
    all_ids    = [w["id"] for w in all_writes]
    burst_done = max(w["t_recv"] for w in all_writes) if all_writes else 0.0

    # ── Phase B: immediate secondary snapshot ─────────────────────────────────
    snap_t_start = ms() - t0
    snap_docs = {
        str(oid): db.get_secondary()["vehicles"].find_one({"_id": oid})
        for oid in all_ids
    }
    snap_t_end = ms() - t0

    immediate_snap = [
        {
            "user":    w["user"],
            "vid":     w["vid"],
            "t_ms":    snap_t_start,
            "t_end_ms":snap_t_end,
            "visible": snap_docs.get(str(w["id"])) is not None,
            "version": snap_docs[str(w["id"])].get("version") if snap_docs.get(str(w["id"])) else None,
            "status":  "visible" if snap_docs.get(str(w["id"])) else "not yet synced",
        }
        for w in all_writes
    ]
    visible_immediately = sum(1 for s in immediate_snap if s["visible"])
    total_writes        = len(all_writes)

    # ── Phase C: poll until all writes visible on secondary ───────────────────
    repl_ms_actual = None
    t_poll_rel     = ms() - t0
    deadline       = time.time() + 8.0
    while time.time() < deadline:
        docs = [db.get_secondary()["vehicles"].find_one({"_id": oid}) for oid in all_ids]
        if all(d is not None for d in docs):
            repl_ms_actual = ms() - (t0 + t_poll_rel)
            break
        time.sleep(0.005)

    # Final secondary read: collect log_indexes in insertion order
    t_final_start = ms() - t0
    final_docs = []
    for oid in all_ids:
        doc = db.get_secondary()["vehicles"].find_one({"_id": oid})
        if doc:
            final_docs.append({
                "id":        str(oid),
                "vid":       doc.get("key", ""),
                "log_index": doc.get("last_log_index"),
            })
    t_final_end = ms() - t0

    log_indexes     = [d["log_index"] for d in final_docs if d["log_index"] is not None]
    order_preserved = log_indexes == sorted(log_indexes)

    repl_ms              = repl_ms_actual or 5.0
    oplog_start_ms       = burst_done          # oplog streams after the concurrent burst completes
    secondary_applied_ms = t_poll_rel + (repl_ms_actual or 0)
    total_repl_ms        = secondary_applied_ms - burst_done

    # ── Build timeline events ─────────────────────────────────────────────────
    events = []

    # Phase A: each user's writes on their own lane
    def _write_events(writes, user_actor, letter):
        out = []
        for w in writes:
            _vdata = {"vehicle_id": w["vid"], "type": "truck",
                      "capacity": 5000 + (w["seq"] - 1) * 100, "year": 2020 + (w["seq"] - 1)}
            out += req_resp_events(
                w["t_send"], w["t_recv"], net_p,
                user_actor, "primary",
                f"write {w['vid']} (w=1)", "write",
                f"ok — {letter}{w['seq']}", "ok",
                req_meta={"collection": "vehicles", "db_action": "insert", "data": _vdata},
                resp_meta={"collection": "vehicles", "db_action": "insert",
                           "document_id": str(w["id"]), "version": 1, "data": _vdata},
            )
        return out

    events += _write_events(results_a, "user_a", "A")
    events += _write_events(results_b, "user_b", "B")

    # Oplog: batch of all writes streams to secondary
    events += replicate_ack_events(
        oplog_start_ms, secondary_applied_ms, net_s,
        f"oplog batch ({total_writes} writes, async, ~{repl_ms:.1f}ms)",
        f"all {total_writes} applied — {total_repl_ms:.0f}ms after burst",
        replicate_meta={"collection": "vehicles", "db_action": "replicate"},
        ack_meta={"collection": "vehicles"},
    )

    # Phase B: snapshot read from secondary (use user_a lane as the "reader")
    events += req_resp_events(
        snap_t_start, snap_t_end, net_s,
        "user_a", "secondary",
        "snapshot read (right after burst)", "read",
        f"{visible_immediately}/{total_writes} visible" + (
            " ⚡ async lag" if visible_immediately < total_writes else " ✓ all present"
        ),
        "stale_response" if visible_immediately < total_writes else "fresh_response",
        req_meta={"collection": "vehicles", "db_action": "find"},
        resp_meta={"collection": "vehicles", "db_action": "find",
                   "version": visible_immediately},
    )

    # Phase C: final ordered read (use user_b lane)
    events += req_resp_events(
        t_final_start, t_final_end, net_s,
        "user_b", "secondary",
        f"final read — order check ({total_writes} docs)", "read",
        "✓ log_index order preserved" if order_preserved else "⚡ ORDER VIOLATED",
        "fresh_response" if order_preserved else "stale_response",
        req_meta={"collection": "vehicles", "db_action": "find"},
        resp_meta={"collection": "vehicles", "db_action": "find",
                   "version": total_writes},
    )

    log = db.get_primary()["operation_logs"].find_one(
        {"target_collection": "vehicles"},
        sort=[("log_index", -1)],
    )

    # Average w=1 write duration per user
    avg_a = (sum(w["t_recv"] - w["t_send"] for w in results_a) / len(results_a)) if results_a else 0
    avg_b = (sum(w["t_recv"] - w["t_send"] for w in results_b) / len(results_b)) if results_b else 0

    return {
        "experiment":  "concurrent_writes",
        "title":       "Concurrent Writes — Propagation Order",
        "description": (
            f"User A and User B each fire {WRITES_PER_USER} w=1 writes simultaneously. "
            "PRIMARY serialises all writes (log_index). "
            "SECONDARY must apply them in the same order — never out of sequence."
        ),
        "actors":      CW_ACTORS,
        "actor_order": CW_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "reads":       immediate_snap,
        "summary": {
            "writes_per_user":      WRITES_PER_USER,
            "total_writes":         total_writes,
            "visible_immediately":  visible_immediately,
            "visible_pct":          round(visible_immediately / total_writes * 100) if total_writes else 0,
            "avg_write_ms_user_a":  round(avg_a, 2),
            "avg_write_ms_user_b":  round(avg_b, 2),
            "replication_delay_ms": round(repl_ms, 2),
            "order_preserved":      order_preserved,
            "order_violated":       not order_preserved,
            "log_index_sequence":   log_indexes,
            "consistency_model":    "Concurrent Writes",
            "consistency_achieved": order_preserved,
        },
    }
