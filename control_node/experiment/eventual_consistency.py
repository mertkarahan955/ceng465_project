"""Experiment 2 — Eventual Consistency (w=1).

Expected Result:
  The follower (secondary) should eventually show the same data as the leader
  (primary), although it may lag behind.

Observation Guide:
  Document the time for all nodes to reach consistency and analyse factors
  contributing to delays.  The key observation is the INCONSISTENCY WINDOW:
  PRIMARY shows v2 immediately after the write while SECONDARY still returns
  v1.  Interleaved primary/secondary reads make this divergence—and eventual
  convergence—visible on the timeline.

Flow:
  1. Write v2 to PRIMARY with w=1 (returns immediately, no secondary wait).
  2. Interleave reads:
       secondary read  →  ⚡ stale  (v1, inconsistent)
       primary   read  →  ✓ fresh  (v2, always consistent)
       … repeat until secondary catches up …
       secondary read  →  ✓ fresh  (v2, consistent at last)
  3. Record the consistency window: time from write until secondary = v2.
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
)


# How many seconds secondary intentionally delays oplog application.
# Must be > 0 to guarantee an observable inconsistency window on a LAN.
_SECONDARY_DELAY_SECS = 1


def _set_secondary_delay(delay_secs: int) -> int:
    """Set secondaryDelaySecs; returns previous value."""
    admin = db.get_primary().client["admin"]
    cfg   = admin.command("replSetGetConfig")["config"]

    sec_idx = next(
        (i for i, m in enumerate(cfg["members"]) if m.get("priority", 1) == 0),
        1,
    )

    prev = cfg["members"][sec_idx].get("secondaryDelaySecs", 0)
    if prev == delay_secs:
        return prev

    cfg["members"][sec_idx]["secondaryDelaySecs"] = delay_secs
    cfg["members"][sec_idx]["priority"] = 0
    cfg["version"] = cfg.get("version", 1) + 1
    admin.command("replSetReconfig", cfg)
    time.sleep(0.5)
    return prev


def run_eventual_consistency():
    """Eventual consistency with interleaved PRIMARY / SECONDARY reads.

    The inconsistency window is made visible by alternating reads between the
    two nodes: primary always returns v2 (consistent), secondary returns v1
    (stale) until the oplog delay expires and it catches up.
    """
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    # ── Phase 1: Ensure v1 exists on BOTH nodes before we start ───────────────
    operations.set_write_concern(1)
    item_id, _ = operations.insert_position(
        "EC-BASE", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    # Wait for secondary to have v1 before setting the artificial delay.
    for _ in range(200):
        sec = db.get_secondary()["positions"].find_one({"_id": item_id})
        if sec and sec.get("version") == 1:
            break
        time.sleep(0.015)

    # ── Phase 2: Introduce artificial replication delay ────────────────────────
    prev_delay = _set_secondary_delay(_SECONDARY_DELAY_SECS)

    # ── Phase 3: w=1 update → v2 (ok returns without waiting for secondary) ───
    t0 = ms()
    operations.set_write_concern(1)
    write_start_ms = ms() - t0
    operations.update_item(item_id, {
        "vehicle_id": "EC-BASE",
        "lat": 41.0082, "lng": 28.9784,
        "city": "Istanbul", "district": "Besiktas",
        "speed_kmh": 90,
    }, collection="positions")
    write_returned_ms = ms() - t0
    operations.set_write_concern("majority")

    # ── Phase 4: Interleaved reads — secondary then primary at each interval ──
    # Each iteration documents whether the nodes are consistent with each other.
    # We guarantee at least MIN_STALE_ROUNDS stale observations before stopping,
    # so the timeline always shows: sec(stale) → pri(fresh) → sec(stale) → pri(fresh)
    # → sec(consistent) with a clear inconsistency window.
    TARGET           = 2
    MIN_STALE_ROUNDS = 2          # must see this many stale secondary reads first
    reads            = []
    stale_rounds     = 0
    try:
        for delay_ms in [200, 400, 700, 1000, 1300, 1600, 2000]:
            target_abs = t0 + write_returned_ms + delay_ms
            sleep_s    = (target_abs - ms()) / 1000
            if sleep_s > 0:
                time.sleep(sleep_s)

            # Read from SECONDARY first (may be stale)
            t_sec1    = ms() - t0
            sec_doc   = db.get_secondary()["positions"].find_one({"_id": item_id})
            t_sec2    = ms() - t0
            sec_ver   = sec_doc.get("version") if sec_doc else None
            sec_stale = sec_ver != TARGET

            # Read from PRIMARY immediately after (always fresh — reference point)
            t_pri1  = ms() - t0
            pri_doc = db.get_primary()["positions"].find_one({"_id": item_id})
            t_pri2  = ms() - t0
            pri_ver = pri_doc.get("version") if pri_doc else None

            if sec_stale:
                stale_rounds += 1

            reads.append({
                "t_ms":             t_sec1,
                "t_end_ms":         t_sec2,
                "t_pri_ms":         t_pri1,
                "t_pri_end_ms":     t_pri2,
                "sec_version":      sec_ver,
                "pri_version":      pri_ver,
                "stale":            sec_stale,
                "nodes_consistent": not sec_stale,
                "status":           f"sec=v{sec_ver} pri=v{pri_ver}",
            })

            # Only stop when secondary is consistent AND we have seen enough stale rounds
            if not sec_stale and stale_rounds >= MIN_STALE_ROUNDS:
                break
    finally:
        _set_secondary_delay(prev_delay)

    # ── Derive consistency metrics ─────────────────────────────────────────────
    first_consistent  = next((r for r in reads if r["nodes_consistent"]), None)
    synced_ms         = first_consistent["t_ms"] if first_consistent else None
    stale_count       = sum(1 for r in reads if r["stale"])
    consistency_window = round(synced_ms - write_start_ms, 1) if synced_ms else None

    log     = get_log(item_id)
    repl_ms = log.get("replication_delay_ms") if log else None

    update_duration      = write_returned_ms - write_start_ms
    primary_commit_ms    = write_start_ms + net_p
    secondary_applied_ms = synced_ms if synced_ms else (
        primary_commit_ms + repl_ms if repl_ms else write_returned_ms + 1100
    )
    total_repl_ms = secondary_applied_ms - write_start_ms

    # ── Build timeline events ──────────────────────────────────────────────────
    events = [
        # 1. w=1 write — ok returns without waiting for secondary
        *req_resp_events(
            write_start_ms, write_returned_ms, net_p,
            "client", "primary",
            "write v2 (w=1)", "write",
            f"ok ({update_duration:.1f}ms) — no secondary wait", "ok",
        ),

        # 2. Oplog network hop (fast); secondary applies later (span line shows delay)
        {"t_ms": primary_commit_ms,
         "latency_ms": safe_lat(net_s),
         "from": "primary", "to": "secondary",
         "label": "oplog async", "type": "replicate"},

        # 3. Ack when secondary finally has v2 — closes the inconsistency window
        {"t_ms": secondary_applied_ms,
         "latency_ms": safe_lat(net_s),
         "from": "secondary", "to": "primary",
         "label": f"✓ consistent (v2) — {total_repl_ms:.0f}ms after write",
         "type": "ack"},
    ]

    # 4. Interleaved reads: secondary (stale/fresh) then primary (always fresh)
    for r in reads:
        # Secondary read
        sec_label = (
            f"⚡ stale — sec=v{r['sec_version']} pri=v{r['pri_version']} (inconsistent)"
            if r["stale"] else
            f"✓ fresh — sec=v{r['sec_version']} = pri=v{r['pri_version']} (consistent!)"
        )
        events.extend(req_resp_events(
            r["t_ms"], r["t_end_ms"], net_s,
            "client", "secondary",
            "read secondary", "read",
            sec_label,
            "stale_response" if r["stale"] else "fresh_response",
        ))

        # Primary read (reference — always v2)
        events.extend(req_resp_events(
            r["t_pri_ms"], r["t_pri_end_ms"], net_p,
            "client", "primary",
            "read primary (reference)", "read",
            f"✓ v{r['pri_version']} (always fresh)", "fresh_response",
        ))

    return {
        "experiment":  "eventual_consistency",
        "title":       "Eventual Consistency (w=1)",
        "description": (
            "PRIMARY w=1 ile günceller — OK secondary beklenmeden döner. "
            "SECONDARY stale (eski) veriyi gösterir, sonra yakalar. "
            "İnterleaved okumalar PRIMARY/SECONDARY tutarsızlık penceresini gösterir."
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         serialize_log(log),
        "reads":       reads,
        "summary": {
            # Write behaviour
            "write_concern":            "1 (async)",
            "write_returned_ms":        round(update_duration, 2),
            # Consistency observation
            "consistency_model":        "Eventual",
            "stale_reads_observed":     stale_count,
            "consistency_achieved":     synced_ms is not None,
            "consistency_window_ms":    consistency_window,
            # Replication delay analysis
            "replication_delay_ms":     round(repl_ms, 2) if repl_ms else None,
            "secondary_delay_secs":     _SECONDARY_DELAY_SECS,
            "delay_factor":             f"secondaryDelaySecs={_SECONDARY_DELAY_SECS} (artificial LAN delay)",
            # Node states at each read
            "read_observations": [
                {
                    "at_ms":         round(r["t_ms"], 1),
                    "secondary":     f"v{r['sec_version']}",
                    "primary":       f"v{r['pri_version']}",
                    "consistent":    r["nodes_consistent"],
                }
                for r in reads
            ],
        },
    }
