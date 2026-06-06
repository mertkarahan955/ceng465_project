"""Experiment 3 — Read-After-Write (DDIA Figure 5-3)."""

import time

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


def run_read_after_write():
    """RAW consistency — single user, two read paths."""
    net_p = measure_net_one_way(db.get_primary)
    net_s = measure_net_one_way(db.get_secondary)

    with write_concern(1):
        t0 = ms()
        item_id, _ = operations.insert_incident(
            "RAW-EXP", "breakdown", "critical",
            "RAW demo — dispatcher files incident"
        )
        t1 = ms()
        write_ms = t1 - t0
        half_w = safe_lat(write_ms / 2)

        t_raw1 = ms() - t0
        pri_doc = db.get_primary()["incidents"].find_one({"_id": item_id})
        t_raw2 = ms() - t0

        t_stale1 = ms() - t0
        sec_doc = db.get_secondary()["incidents"].find_one({"_id": item_id})
        t_stale2 = ms() - t0
        stale = sec_doc is None

        repl_ms_actual = None
        if stale:
            t_poll = ms()
            deadline = time.time() + 5.0
            while time.time() < deadline:
                doc = db.get_secondary()["incidents"].find_one({"_id": item_id})
                if doc:
                    repl_ms_actual = ms() - t_poll
                    break
                time.sleep(0.002)
        else:
            repl_ms_actual = safe_lat(t_stale1 - half_w)

        log = get_log(item_id)

    repl_ms = repl_ms_actual or (log.get("replication_delay_ms") if log else None) or 5.0

    events = [
        {"t_ms": 0,          "latency_ms": half_w,      "from": "client",    "to": "primary",
         "label": "write incident (w=1)",              "type": "write"},
        {"t_ms": half_w,     "latency_ms": half_w,      "from": "primary",   "to": "client",
         "label": f"ok ({write_ms:.1f}ms) — immediately", "type": "ok"},
        {"t_ms": half_w,     "latency_ms": safe_lat(repl_ms), "from": "primary", "to": "secondary",
         "label": f"oplog (async, ~{repl_ms:.1f}ms)", "type": "replicate"},
        *req_resp_events(
            t_raw1, t_raw2, net_p,
            "client", "primary",
            "read (RAW path — PRIMARY)", "read",
            f"✓ fresh ({t_raw2 - t_raw1:.1f}ms) — always", "fresh_response",
        ),
        *req_resp_events(
            t_stale1, t_stale2, net_s,
            "client", "secondary",
            "read (wrong path)", "read",
            "⚡ stale! (write not propagated)" if stale else "✓ fresh (fast sync)",
            "stale_response" if stale else "fresh_response",
        ),
    ]

    return {
        "experiment":  "read_after_write",
        "title":       "Read-After-Write Consistency",
        "description": (
            "Yazıdan sonra SECONDARY'den okursak kendi yazımızı göremeyebiliriz. "
            "RAW çözümü: write yapan session için okumaları PRIMARY'ye yönlendir. "
            "(DDIA Figure 5-3)"
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "summary": {
            "write_concern":        "1 (async)",
            "write_returned_ms":    round(write_ms, 2),
            "stale_read_observed":  stale,
            "raw_read_ms":          round(t_raw2 - t_raw1, 2),
            "raw_fresh":            pri_doc is not None,
            "replication_delay_ms": round(repl_ms, 2) if repl_ms else None,
            "consistency_model":    "Read-After-Write",
            "consistency_achieved": pri_doc is not None,
        },
    }
