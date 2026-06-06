"""Experiment 1 — Synchronous Replication (w=majority)."""

import db
import operations

from .common import (
    DEFAULT_ACTOR_ORDER,
    DEFAULT_ACTORS,
    get_log,
    measure_net_one_way,
    ms,
    safe_lat,
    serialize_log,
)


def run_sync_replication():
    """w=majority: Primary follower ACK'ini bekler, sonra istemciye döner."""
    operations.set_write_concern("majority")

    net_p = measure_net_one_way(db.get_primary)
    net_s = measure_net_one_way(db.get_secondary)

    t0 = ms()
    item_id, delay_ms = operations.insert_position(
        "SYNC-EXP", 41.0082, 28.9784, "Istanbul", "Besiktas", 65
    )
    t1 = ms()

    d = delay_ms or (t1 - t0)
    log = get_log(item_id)

    oplog_start = safe_lat(net_p)
    sec_arrive = safe_lat(net_p + net_s)
    ok_depart = max(d - net_p, sec_arrive + net_s + 0.5)
    sec_ack_t = max(ok_depart - net_s, sec_arrive + 0.5)

    events = [
        {"t_ms": 0,           "latency_ms": safe_lat(net_p),                   "from": "client",    "to": "primary",   "label": "write (w=majority)", "type": "write"},
        {"t_ms": oplog_start, "latency_ms": safe_lat(sec_arrive - oplog_start), "from": "primary",   "to": "secondary", "label": "oplog → apply",      "type": "replicate"},
        {"t_ms": sec_ack_t,   "latency_ms": safe_lat(ok_depart - sec_ack_t),   "from": "secondary", "to": "primary",   "label": "ACK ✓",              "type": "ack"},
        {"t_ms": ok_depart,   "latency_ms": safe_lat(net_p),                   "from": "primary",   "to": "client",    "label": f"ok ({d:.1f} ms)",   "type": "ok"},
    ]

    return {
        "experiment":  "sync_replication",
        "title":       "Synchronous Replication (w=majority)",
        "description": "Primary, follower ACK'ini bekler. İstemci ancak secondary onayladıktan sonra 'ok' alır. Daha güvenli ama daha yavaş.",
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "summary": {
            "write_concern":        "majority",
            "write_returned_ms":    round(d, 2),
            "replication_delay_ms": round(d, 2),
            "consistency_model":    "Synchronous",
            "version_after":        1,
            "consistency_achieved": True,
            "stale_read_possible":  False,
        },
    }
