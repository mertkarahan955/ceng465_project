"""Experiment 3 — Read-After-Write (DDIA Figure 5-3).

Spec:
  Setup:    A client writes a record on the leader node.
  Test:     Immediately read the record back from the leader to confirm it
            reflects the latest write.
  Expected: The client should immediately see their write on the leader;
            other clients may experience a delay on followers.
  Observe:  Record the time followers take to reflect the new data.
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
    replicate_ack_events,
    req_resp_events,
    safe_lat,
    serialize_log,
    write_concern,
)


def run_read_after_write():
    """RAW consistency — single user, two read paths.

    Every arrow pair uses req_resp_events so all messages show the
    N_/ (N=network) shape:  request departs → arrives → (processing) → departs → arrives.
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    incident_value = {
        "vehicle_id": "RAW-EXP",
        "incident_type": "breakdown",
        "severity": "critical",
        "description": "RAW demo — dispatcher files incident",
        "lat": None,
        "lng": None,
        "resolved": False,
    }

    with write_concern(1):
        t0 = ms()
        item_id, _ = operations.insert_incident(
            "RAW-EXP", "breakdown", "critical",
            "RAW demo — dispatcher files incident"
        )
        t1 = ms()
        write_ms = t1 - t0

        # RAW read: hit PRIMARY (correct routing — always fresh)
        t_raw1  = ms() - t0
        pri_doc = db.get_primary()["incidents"].find_one({"_id": item_id})
        t_raw2  = ms() - t0

        # Stale read: hit SECONDARY (shows the anomaly)
        t_stale1 = ms() - t0
        sec_doc  = db.get_secondary()["incidents"].find_one({"_id": item_id})
        t_stale2 = ms() - t0
        stale    = sec_doc is None

        # Poll secondary to measure when write becomes visible there
        t_poll_rel     = ms() - t0   # t0-relative timestamp, right before polling
        repl_ms_actual = None
        if stale:
            deadline = time.time() + 5.0
            while time.time() < deadline:
                doc = db.get_secondary()["incidents"].find_one({"_id": item_id})
                if doc:
                    repl_ms_actual = ms() - (t0 + t_poll_rel)
                    break
                time.sleep(0.002)

        log = get_log(item_id)

    repl_ms = repl_ms_actual or (log.get("replication_delay_ms") if log else None) or 5.0

    # Oplog entry is written as part of the write itself — it starts
    # propagating the moment the write request lands on PRIMARY.
    oplog_start_ms = safe_lat(net_p)

    # When did secondary finish applying the write?
    if stale and repl_ms_actual:
        secondary_applied_ms = t_poll_rel + repl_ms_actual
    elif not stale:
        secondary_applied_ms = t_stale2   # already visible by the time we read it
    else:
        secondary_applied_ms = write_ms + repl_ms

    secondary_applied_ms = max(secondary_applied_ms, oplog_start_ms + safe_lat(net_s))
    total_repl_ms = secondary_applied_ms - write_ms

    events = [
        # Write (w=1) — N_/ (N=network) shape using measured net_p
        *req_resp_events(
            0, write_ms, net_p,
            "client", "primary",
            "write incident (w=1)", "write",
            f"ok ({write_ms:.1f}ms) — immediately", "ok",
            req_meta={
                "collection": "incidents",
                "db_action": "insert",
                "document_id": str(item_id),
                "version": "v1",
                "data": incident_value,
            },
            resp_meta={
                "collection": "incidents",
                "db_action": "write_ack",
                "document_id": str(item_id),
                "version": "v1",
                "data": incident_value,
            },
        ),
        # Async oplog: primary sends after commit; secondary applies at secondary_applied_ms
        *replicate_ack_events(
            oplog_start_ms, secondary_applied_ms, net_s,
            "oplog (async)",
            f"✓ write applied — {total_repl_ms:.0f}ms after write",
            replicate_meta={"collection": "incidents", "db_action": "replicate_insert",
                            "document_id": str(item_id), "version": "v1", "data": incident_value},
            ack_meta={"collection": "incidents", "db_action": "applied_on_secondary",
                      "document_id": str(item_id), "version": "v1", "data": incident_value},
        ),

        # RAW read from PRIMARY — N_/ (N=network) shape using measured net_p
        *req_resp_events(
            t_raw1, t_raw2, net_p,
            "client", "primary",
            "read (RAW path — PRIMARY)", "read",
            f"✓ fresh ({t_raw2 - t_raw1:.1f}ms) — always", "fresh_response",
            req_meta={
                "collection": "incidents",
                "db_action": "read",
                "document_id": str(item_id),
            },
            resp_meta={
                "collection": "incidents",
                "db_action": "read_result",
                "document_id": str(item_id),
                "version": f"v{pri_doc.get('version')}" if pri_doc else "not found",
                "data": pri_doc.get("value") if pri_doc else None,
            },
        ),

        # Stale read from SECONDARY — N_/ (N=network) shape using measured net_s
        *req_resp_events(
            t_stale1, t_stale2, net_s,
            "client", "secondary",
            "read (wrong path)", "read",
            "⚡ stale! (write not propagated)" if stale else "✓ fresh (fast sync)",
            "stale_response" if stale else "fresh_response",
            req_meta={
                "collection": "incidents",
                "db_action": "read",
                "document_id": str(item_id),
            },
            resp_meta={
                "collection": "incidents",
                "db_action": "read_result",
                "document_id": str(item_id),
                "version": f"v{sec_doc.get('version')}" if sec_doc else "not visible on secondary",
                "data": sec_doc.get("value") if sec_doc else None,
            },
        ),
    ]

    events.sort(key=lambda e: e["t_ms"])

    return {
        "experiment":  "read_after_write",
        "title":       "Read-After-Write Consistency",
        "description": (
            "Reading from SECONDARY right after a write may not show our own write. "
            "RAW fix: route reads from the writing session to PRIMARY. "
            "(DDIA Figure 5-3)"
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "summary": {
            "write_concern":        "1 (async)",
            "write_returned_ms":    round(write_ms, 2),
            # Observation: did the leader immediately confirm the write?
            "raw_read_ms":          round(t_raw2 - t_raw1, 2),
            "raw_fresh":            pri_doc is not None,
            # Observation: did the follower see a stale copy right after the write?
            "stale_read_observed":  stale,
            # Observation: how long did the follower take to reflect the new data?
            "follower_sync_ms":     round(repl_ms, 2) if repl_ms else None,
            "replication_delay_ms": round(repl_ms, 2) if repl_ms else None,
            "consistency_model":    "Read-After-Write",
            "consistency_achieved": pri_doc is not None,
        },
    }
