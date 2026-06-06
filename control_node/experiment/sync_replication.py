"""Experiment 1 — Synchronous Replication (w=majority)."""

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
)


def run_sync_replication():
    """w=majority: Primary follower ACK'ini bekler, sonra istemciye döner.

    Timeline (N_/ (N=network) shape for each arrow pair):
      Client → Primary   : write request  (net_p)
      Primary → Secondary: oplog → apply  (net_s)
      Secondary → Primary: ACK            (net_s)
      Primary → Client   : ok             (net_p)  ← only after ACK
    """
    operations.set_write_concern("majority")

    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    position_value = {
        "vehicle_id": "SYNC-EXP",
        "lat": 41.0082,
        "lng": 28.9784,
        "city": "Istanbul",
        "district": "Besiktas",
        "speed_kmh": 65,
    }

    t0 = ms()
    item_id, delay_ms = operations.insert_position(
        "SYNC-EXP", 41.0082, 28.9784, "Istanbul", "Besiktas", 65
    )
    t1 = ms()

    d   = delay_ms or (t1 - t0)
    log = get_log(item_id)

    # Sub-timings derived from measured one-way latencies.
    #
    # t=0        client sends write
    # t=net_p    primary receives; immediately starts oplog
    # t=net_p+net_s  secondary receives oplog, applies
    # t=d-net_s  secondary sends ACK (just before ok_depart)
    # t=d-net_p  primary sends ok  (after ACK received)
    # t=d        client receives ok
    oplog_start = safe_lat(net_p)
    sec_arrive  = safe_lat(net_p + net_s)
    ok_depart   = max(d - net_p, sec_arrive + net_s + 0.5)
    sec_ack_t   = max(ok_depart - net_s, sec_arrive + 0.5)

    events = [
        # Write request: N_/ (N=network)  (client → primary)
        *req_resp_events(
            0, d, net_p,
            "client", "primary",
            "write (w=majority)", "write",
            f"ok ({d:.1f} ms)", "ok",
            req_meta={
                "collection": "positions",
                "db_action": "insert",
                "document_id": str(item_id),
                "version": "v1",
                "data": position_value,
            },
            resp_meta={
                "collection": "positions",
                "db_action": "write_ack",
                "document_id": str(item_id),
                "version": "v1",
                "data": position_value,
            },
        ),
        # Oplog: primary → secondary  (arrives quickly, secondary processes)
        {"t_ms": oplog_start, "latency_ms": safe_lat(sec_arrive - oplog_start),
         "from": "primary", "to": "secondary",
         "label": "oplog → apply", "type": "replicate",
         "collection": "positions", "db_action": "replicate_insert",
         "document_id": str(item_id), "version": "v1", "data": position_value},
        # ACK: secondary → primary  (pairs with oplog → span on secondary lane)
        {"t_ms": sec_ack_t, "latency_ms": safe_lat(ok_depart - sec_ack_t),
         "from": "secondary", "to": "primary",
         "label": "ACK ✓", "type": "ack",
         "collection": "positions", "db_action": "applied_on_secondary",
         "document_id": str(item_id), "version": "v1", "data": position_value},
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
