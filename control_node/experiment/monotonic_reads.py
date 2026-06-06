"""Experiment 4 — Monotonic Reads (DDIA Figure 5-4).

Spec:
  Setup:    Perform a sequence of updates on a single record on the leader
            (increment version from 1 to 5).
  Test:     Sequentially read the record from a follower node.
  Expected: Each read reflects the same or a later version — no backwards.
  Observe:  Log any backward reads (e.g. v5 → v3) and analyse the cause.
"""

import time

import config
import db
import operations

from .common import (
    DEFAULT_ACTOR_ORDER,
    DEFAULT_ACTORS,
    get_log,
    measure_net_one_way,
    ms,
    req_resp_events,
    safe_lat,
    serialize_log,
    write_concern,
)

# Number of updates after the initial insert: v1 (insert) → v5 (final update)
_STATUSES = [
    ("pending",     1),   # insert   → v1
    ("in_transit",  2),   # update 1 → v2
    ("in_transit",  3),   # update 2 → v3
    ("in_transit",  4),   # update 3 → v4
    ("delivered",   5),   # update 4 → v5
]
FINAL_VERSION = _STATUSES[-1][1]


def run_monotonic_reads():
    """Monotonic reads — v1→v5 writes on PRIMARY, then sequential SECONDARY reads.

    w=1 fires all five writes without waiting for the secondary to acknowledge.
    Sequential reads from the same secondary must show a non-decreasing version
    sequence: we might see the secondary mid-propagation (v2, v3, v4…) but we
    must never see a version go backwards (e.g. v5 → v3).

    backward_reads in the summary records any detected violations for analysis.
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    shipment_value = {
        "shipment_id": "MON-EXP",
        "origin_depot": "DEP-IST",
        "destination_depot": "DEP-ANK",
        "customer": "MonotonicTest",
        "weight_kg": 300,
        "package_count": 3,
        "status": "in_transit",
        "assigned_vehicle_id": None,
    }

    with write_concern(1):
        t0 = ms()

        # ── Phase 1: v1 insert ────────────────────────────────────────────────
        shp_id, _ = operations.insert_shipment(
            "MON-EXP", "DEP-IST", "DEP-ANK", "MonotonicTest",
            300, 3, status=_STATUSES[0][0],
        )
        t_w1 = ms() - t0
        write_events = [{"v": 1, "t_send": 0.0, "t_recv": t_w1}]

        # ── Phase 2: v2→v5 updates (w=1, fire-and-forget, as fast as possible) ─
        for status, v in _STATUSES[1:]:
            t_ws = ms() - t0
            operations.update_item(
                shp_id,
                {
                    "shipment_id":        "MON-EXP",
                    "origin_depot":       "DEP-IST",
                    "destination_depot":  "DEP-ANK",
                    "customer":           "MonotonicTest",
                    "weight_kg":          300,
                    "package_count":      3,
                    "status":             status,
                },
                collection="shipments",
            )
            t_we = ms() - t0
            write_events.append({"v": v, "t_send": t_ws, "t_recv": t_we})

        # ── Phase 3: measure first appearance on secondary (oplog latency) ────
        repl_ms_actual = None
        t_poll_rel = ms() - t0
        deadline = time.time() + 5.0
        while time.time() < deadline:
            doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
            if doc:
                repl_ms_actual = ms() - (t0 + t_poll_rel)
                break
            time.sleep(0.002)

        # ── Phase 4: 5 sequential reads from SECONDARY ───────────────────────
        reads = []
        for _ in range(5):
            t_r1 = ms() - t0
            doc  = db.get_secondary()["shipments"].find_one({"_id": shp_id})
            t_r2 = ms() - t0
            reads.append({
                "t_ms":     t_r1,
                "t_end_ms": t_r2,
                "version":  doc.get("version")                          if doc else 0,
                "status":   doc.get("value", {}).get("status", "—")    if doc else "not synced",
                "data":     doc.get("value") if doc else None,
            })
            time.sleep(0.025)   # 25 ms gap — gives oplog a chance to catch up mid-sequence

        log = get_log(shp_id)

    # ── Monotonicity check ────────────────────────────────────────────────────
    versions  = [r["version"] for r in reads]
    monotonic = all(versions[i] <= versions[i + 1] for i in range(len(versions) - 1))

    # Log every backward read for analysis
    backward_reads = [
        {
            "read_index":   i,
            "from_version": versions[i],
            "to_version":   versions[i + 1],
            "cause":        "impossible with single ordered oplog (should never occur)",
        }
        for i in range(len(versions) - 1)
        if versions[i] > versions[i + 1]
    ]

    # ── Resolve replication delay ─────────────────────────────────────────────
    repl_ms = repl_ms_actual or (log.get("replication_delay_ms") if log else None) or 5.0

    # Anchor timestamps for oplog events
    # primary commit happens at net_p after t_send=0; oplog departs from there.
    oplog_start_ms       = safe_lat(net_p)
    secondary_applied_ms = t_poll_rel + (repl_ms_actual or 0)
    total_repl_ms        = secondary_applied_ms - write_events[0]["t_recv"]

    # ── Build timeline events ─────────────────────────────────────────────────
    events = []

    # All 5 writes to PRIMARY (each shows its own processing band)
    for we in write_events:
        v     = we["v"]
        ltype = "write"
        req   = "write (w=1, insert)" if v == 1 else f"update → v{v} (w=1)"
        ok    = f"ok v{v}" + (f" ({we['t_recv']:.1f}ms)" if v == 1 else "")
        events += req_resp_events(
            we["t_send"], we["t_recv"], net_p,
            "client", "primary",
            "write shipment (w=1)", "write",
            f"ok v1 ({t_w1:.1f}ms)", "ok",
            req_meta={
                "collection": "shipments",
                "db_action": "insert",
                "document_id": str(shp_id),
                "version": "v1",
                "data": shipment_value,
            },
            resp_meta={
                "collection": "shipments",
                "db_action": "write_ack",
                "document_id": str(shp_id),
                "version": "v1",
                "data": shipment_value,
            },
        ),
        # Oplog: departs from primary after commit; lands at secondary quickly
        {"t_ms": safe_lat(net_p),
         "latency_ms": safe_lat(net_s),
         "from": "primary", "to": "secondary",
         "label": "oplog (async)", "type": "replicate",
         "collection": "shipments", "db_action": "replicate_insert",
         "document_id": str(shp_id), "version": "v1", "data": shipment_value},
        # Ack: secondary finishes applying v1 — pairs with oplog for span line
        {"t_ms": secondary_applied_ms,
         "latency_ms": safe_lat(net_s),
         "from": "secondary", "to": "primary",
         "label": f"✓ write applied (v1) — {total_repl_ms:.0f}ms after write",
         "type": "ack",
         "collection": "shipments", "db_action": "applied_on_secondary",
         "document_id": str(shp_id), "version": "v1", "data": shipment_value},
    ]

    prev_v = None
    for r in reads:
        v         = r["version"]
        s         = r["status"]
        went_back = prev_v is not None and v < prev_v
        prev_v    = v
        resp_label = f"v{v} ({s})" + (" ⚡ BACKWARDS!" if went_back else " ✓")
        resp_type  = "stale_response" if went_back else "fresh_response"
        events += req_resp_events(
            r["t_ms"], r["t_end_ms"], net_s,
            "client", "secondary",
            "read (SECONDARY)", "read",
            resp_label, resp_type,
            req_meta={
                "collection": "shipments",
                "db_action": "read",
                "document_id": str(shp_id),
            },
            resp_meta={
                "collection": "shipments",
                "db_action": "read_result",
                "document_id": str(shp_id),
                "version": f"v{v}" if v else "not visible on secondary",
                "data": r["data"],
            },
        )

    return {
        "experiment":  "monotonic_reads",
        "title":       "Monotonic Reads",
        "description": (
            f"w=1 ile v1→v{FINAL_VERSION} yazılır (5 write, fire-and-forget), "
            "ardından aynı SECONDARY'den 5 ardışık okuma. "
            "Version numarası asla geri gidemez. (DDIA Figure 5-4)"
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "reads":       reads,
        "summary": {
            "write_concern":        "1 (async)",
            "versions_written":     [we["v"] for we in write_events],
            "versions_seen":        versions,
            "monotonic":            monotonic,
            "monotonic_violated":   not monotonic,
            "backward_reads":       backward_reads,
            "backward_count":       len(backward_reads),
            "replication_delay_ms": round(repl_ms, 2)        if repl_ms  else None,
            "consistency_model":    "Monotonic Reads",
            "consistency_achieved": monotonic,
            "final_version":        max(versions)             if versions else None,
        },
    }
