"""Experiment 4 — Monotonic Reads (DDIA Figure 5-4)."""

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


def run_monotonic_reads():
    """Monotonic reads guarantee — single user, always hitting the same replica.

    Every arrow pair uses req_resp_events so all messages show the
    N_/ (N=network) shape with actual measured network latency.
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    with write_concern(1):
        t0 = ms()

        shp_id, _ = operations.insert_shipment(
            "MON-EXP", "DEP-IST", "DEP-ANK", "MonotonicTest",
            300, 3, status="in_transit"
        )
        t_w1 = ms() - t0  # write returned at this offset from t0

        # Poll secondary until v1 is visible so reads start from a known state
        repl_ms_actual = None
        t_poll   = ms()
        deadline = time.time() + 5.0
        while time.time() < deadline:
            doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
            if doc:
                repl_ms_actual = ms() - t_poll
                break
            time.sleep(0.002)

        reads = []
        for _ in range(3):
            t_r1 = ms() - t0
            doc  = db.get_secondary()["shipments"].find_one({"_id": shp_id})
            t_r2 = ms() - t0
            reads.append({
                "t_ms":     t_r1,
                "t_end_ms": t_r2,
                "version":  doc.get("version") if doc else 0,
                "status":   doc.get("value", {}).get("status", "—") if doc else "not synced",
            })
            time.sleep(0.04)

        log = get_log(shp_id)

    versions  = [r["version"] for r in reads]
    monotonic = all(versions[i] <= versions[i + 1] for i in range(len(versions) - 1))

    events = [
        # Write shipment — N_/ (N=network) using measured net_p
        *req_resp_events(
            0, t_w1, net_p,
            "client", "primary",
            "write shipment (w=1)", "write",
            f"ok v1 ({t_w1:.1f}ms)", "ok",
        ),
        # Oplog: departs from primary after commit (net_p into timeline)
        {"t_ms": safe_lat(net_p),
         "latency_ms": safe_lat(repl_ms_actual or 20),
         "from": "primary", "to": "secondary",
         "label": f"oplog (async, ~{(repl_ms_actual or 0):.1f}ms)", "type": "replicate"},
    ]

    prev_v = None
    for r in reads:
        v = r["version"]
        s = r["status"]
        went_back = prev_v is not None and v < prev_v
        prev_v    = v
        resp_label = f"v{v} ({s})" + (" ⚡ BACKWARDS!" if went_back else " ✓")
        resp_type  = "stale_response" if went_back else "fresh_response"

        # Each read — N_/ (N=network) using measured net_s
        events.extend(req_resp_events(
            r["t_ms"], r["t_end_ms"], net_s,
            "client", "secondary",
            "read (SECONDARY)", "read",
            resp_label, resp_type,
        ))

    repl_ms = repl_ms_actual or (log.get("replication_delay_ms") if log else None)

    return {
        "experiment":  "monotonic_reads",
        "title":       "Monotonic Reads",
        "description": (
            "w=1 ile tek write, ardından aynı SECONDARY'den 3 ardışık okuma. "
            "Version numarası asla geri gidemez. (DDIA Figure 5-4)"
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "summary": {
            "write_concern":        "1 (async)",
            "versions_seen":        versions,
            "monotonic":            monotonic,
            "monotonic_violated":   not monotonic,
            "replication_delay_ms": round(repl_ms, 2) if repl_ms else None,
            "consistency_model":    "Monotonic Reads",
            "consistency_achieved": monotonic,
            "final_version":        max(versions) if versions else None,
        },
    }
