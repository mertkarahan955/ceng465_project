"""Experiment 4 — Monotonic Reads (DDIA Figure 5-4).

Spec:
  Setup:    A shared shipment document exists at v1 on both PRIMARY and
            SECONDARY.
  Test:     Two concurrent clients —
              - Reader: fires a sequence of reads against SECONDARY.
              - Writer: fires a sequence of w=1 updates (v1 -> v2 -> ... ->
                vFINAL) on PRIMARY, starting shortly after the Reader's
                first read.
  Expected: The version sequence the Reader observes must never decrease.
            It may see v1 one or more times (oplog hasn't caught up yet),
            then v2, v3, ... as replication catches up — but it must never
            see a higher version and then fall back to a lower one.
  Observe:  Log any backward reads (e.g. v3 -> v2) and analyse the cause.
"""

import threading
import time

import config
import db
import operations

from .common import (
    get_log,
    measure_net_one_way,
    ms,
    replicate_ack_events,
    req_resp_events,
    safe_lat,
    serialize_log,
    write_concern,
)

# v1 (initial insert) -> v2 -> v3 (writer's two sequential updates)
_STATUSES = [
    ("pending",     1),   # insert   -> v1
    ("in_transit",  2),   # update 1 -> v2
    ("delivered",   3),   # update 2 -> v3
]
FINAL_VERSION = _STATUSES[-1][1]

# Interleaved timeline (t0..t6):
#   t0: read#1   t1: write#1   t2: read#2   t3: read#3
#   t4: write#2  t5: read#4    t6: read#5
READ_COUNT     = 5    # sequential reads the Reader client performs
READ_GAP_MS    = 20   # gap between Reader's reads
WRITE_DELAY_MS = 8    # Writer fires its first update shortly after Reader's first read (t1, between t0 and t2)
WRITE_GAP_MS   = 40   # gap to the Writer's second update (t4, between t3 and t5)

# 4-actor layout: Reader client, Writer client, primary, secondary
MONO_ACTORS = {
    "user_a":    {"label": "Reader Client", "color": "#79c0ff", "ip": "(thread A)"},
    "user_b":    {"label": "Writer Client", "color": "#f0883e", "ip": "(thread B)"},
    "primary":   {"label": "PRIMARY",       "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY",     "color": "#a371f7", "ip": "192.168.88.70"},
}
MONO_ACTOR_ORDER = ["user_a", "user_b", "primary", "secondary"]


def _shipment_value(status: str) -> dict:
    return {
        "shipment_id": "MON-EXP",
        "origin_depot": "DEP-IST",
        "destination_depot": "DEP-ANK",
        "customer": "MonotonicTest",
        "weight_kg": 300,
        "package_count": 3,
        "status": status,
        "assigned_vehicle_id": None,
    }


def _reader_thread(t0: float, shp_id, out: list, lock: threading.Lock):
    """Reader: READ_COUNT sequential reads from SECONDARY, with a gap between each."""
    for _ in range(READ_COUNT):
        t_r1 = ms() - t0
        doc  = db.get_secondary()["shipments"].find_one({"_id": shp_id})
        t_r2 = ms() - t0
        with lock:
            out.append({
                "t_ms":     t_r1,
                "t_end_ms": t_r2,
                "version":  doc.get("version") if doc else 0,
                "status":   doc.get("value", {}).get("status", "—") if doc else "not synced",
                "data":     doc.get("value") if doc else None,
            })
        time.sleep(READ_GAP_MS / 1000)


def _writer_thread(t0: float, shp_id, out: list, lock: threading.Lock):
    """Writer: v2 -> ... -> vFINAL, one w=1 update at a time, with a gap between each."""
    time.sleep(WRITE_DELAY_MS / 1000)
    prev_status = _STATUSES[0][0]
    for status, v in _STATUSES[1:]:
        value = _shipment_value(status)
        t_ws  = ms() - t0
        operations.update_item(shp_id, value, collection="shipments")
        t_we  = ms() - t0
        with lock:
            out.append({
                "v": v,
                "t_send": t_ws,
                "t_recv": t_we,
                "value": value,
                "value_before": _shipment_value(prev_status),
            })
        prev_status = status
        time.sleep(WRITE_GAP_MS / 1000)


def _poller_thread(t0: float, shp_id, targets: set, out: dict, lock: threading.Lock):
    """Poll SECONDARY; for each target version, record the t0-relative time it first appears."""
    pending = set(targets)
    deadline = time.time() + 8.0
    while pending and time.time() < deadline:
        doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
        if doc:
            v = doc.get("version", 0)
            for target in sorted(pending):
                if v >= target:
                    with lock:
                        out[target] = ms() - t0
                    pending.discard(target)
        time.sleep(0.002)


def run_monotonic_reads():
    """Monotonic reads — concurrent Reader + Writer on the same document.

    Setup: insert v1 with the default (majority) write concern, so both
    PRIMARY and SECONDARY have v1 before the race begins.

    Race (w=1):
      - Reader fires READ_COUNT sequential reads against SECONDARY.
      - Writer fires a sequence of w=1 updates (v1 -> v2 -> ... -> vFINAL),
        starting shortly after Reader's first read.

    The Reader's version sequence must never decrease. backward_reads in the
    summary records any detected violations for analysis.
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    # ── Setup: v1 insert, majority write concern — both nodes have v1 ─────────
    shp_id, _ = operations.insert_shipment(
        "MON-EXP", "DEP-IST", "DEP-ANK", "MonotonicTest",
        300, 3, status=_STATUSES[0][0],
    )

    # ── Race: Reader (SECONDARY reads) vs Writer (PRIMARY w=1 updates) ────────
    # A poller runs alongside them so the oplog ACK is timestamped at the
    # moment vFINAL actually becomes visible on SECONDARY — not at whatever
    # time the reader/writer threads happen to finish.
    reads: list        = []
    write_events: list = []
    poll_result: dict  = {}
    lock = threading.Lock()

    with write_concern(1):
        t0 = ms()

        reader = threading.Thread(
            target=_reader_thread, args=(t0, shp_id, reads, lock), daemon=True,
        )
        writer = threading.Thread(
            target=_writer_thread, args=(t0, shp_id, write_events, lock), daemon=True,
        )
        target_versions = {v for _, v in _STATUSES[1:]}
        poller = threading.Thread(
            target=_poller_thread, args=(t0, shp_id, target_versions, poll_result, lock), daemon=True,
        )
        reader.start()
        writer.start()
        poller.start()
        reader.join()
        writer.join()
        poller.join()

        log = get_log(shp_id)

    write_events.sort(key=lambda w: w["t_send"])

    # ── Monotonicity check ────────────────────────────────────────────────────
    versions  = [r["version"] for r in reads]
    monotonic = all(versions[i] <= versions[i + 1] for i in range(len(versions) - 1))

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

    # ── Resolve replication delay per write ───────────────────────────────────
    fallback_repl_ms = (log.get("replication_delay_ms") if log else None) or 0.0
    repl_delays = []
    for we in write_events:
        v = we["v"]
        # Oplog entry is written as part of the write itself — it starts
        # propagating the moment the write request lands on PRIMARY, not
        # when the w=1 ack returns to the client.
        oplog_start_ms = safe_lat(we["t_send"] + net_p)
        applied_ms     = poll_result.get(v)
        if applied_ms is not None:
            secondary_applied_ms = max(applied_ms, oplog_start_ms + safe_lat(net_s))
            total_repl_ms        = secondary_applied_ms - we["t_recv"]
        else:
            total_repl_ms        = fallback_repl_ms
            secondary_applied_ms = oplog_start_ms + total_repl_ms
        repl_delays.append(total_repl_ms)
        we["oplog_start_ms"]       = oplog_start_ms
        we["secondary_applied_ms"] = secondary_applied_ms
        we["total_repl_ms"]        = total_repl_ms

    repl_ms = sum(repl_delays) / len(repl_delays) if repl_delays else fallback_repl_ms

    # ── Build timeline events ─────────────────────────────────────────────────
    events = []

    # Writer's v2 -> ... -> vFINAL updates on PRIMARY, each followed by its own
    # oplog replicate + ACK pair to SECONDARY.
    for we in write_events:
        v = we["v"]
        events += req_resp_events(
            we["t_send"], we["t_recv"], net_p,
            "user_b", "primary",
            f"update shipment -> v{v} (w=1)", "write",
            f"ok v{v}", "ok",
            req_meta={
                "collection": "shipments",
                "db_action": "update",
                "document_id": str(shp_id),
                "version": f"v{v - 1} -> v{v}",
                "data": we["value"],
                "data_before": we["value_before"],
            },
            resp_meta={
                "collection": "shipments",
                "db_action": "write_ack",
                "document_id": str(shp_id),
                "version": f"v{v}",
                "data": we["value"],
            },
        )
        events += replicate_ack_events(
            we["oplog_start_ms"], we["secondary_applied_ms"], net_s,
            f"oplog (async) — v{v}",
            f"✓ write applied (v{v}) — {we['total_repl_ms']:.0f}ms after write",
            replicate_meta={"collection": "shipments", "db_action": "replicate_update",
                            "document_id": str(shp_id), "version": f"v{v - 1} -> v{v}",
                            "data": we["value"]},
            ack_meta={"collection": "shipments", "db_action": "applied_on_secondary",
                      "document_id": str(shp_id), "version": f"v{v}",
                      "data": we["value"]},
        )

    # Reader's sequential reads from SECONDARY
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
            "user_a", "secondary",
            "read (Reader, SECONDARY)", "read",
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

    events.sort(key=lambda e: e["t_ms"])

    return {
        "experiment":  "monotonic_reads",
        "title":       "Monotonic Reads",
        "description": (
            "Reader, SECONDARY'den ardışık okurken Writer, PRIMARY'de art arda "
            f"w=1 update'lerle v1 -> v{FINAL_VERSION} yazar. Reader'ın gördüğü "
            "version dizisi asla geriye gidemez: daha yüksek bir version "
            "görüldükten sonra bir daha daha düşük bir version görülemez. "
            "(DDIA Figure 5-4)"
        ),
        "actors":      MONO_ACTORS,
        "actor_order": MONO_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "reads":       reads,
        "summary": {
            "write_concern":        "1 (async)",
            "versions_written":     [v for _, v in _STATUSES],
            "versions_seen":        versions,
            "monotonic":            monotonic,
            "monotonic_violated":   not monotonic,
            "backward_reads":       backward_reads,
            "backward_count":       len(backward_reads),
            "replication_delay_ms": round(repl_ms, 2) if repl_ms else None,
            "consistency_model":    "Monotonic Reads",
            "consistency_achieved": monotonic,
            "final_version":        max(versions) if versions else None,
        },
    }
