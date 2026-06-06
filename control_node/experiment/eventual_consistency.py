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

    # ── Phase 4: Observation sequence ─────────────────────────────────────────
    # The intended observation is about PRIMARY vs SECONDARY state, not just a
    # single secondary stale read:
    #   secondary -> primary  (inconsistent)
    #   secondary -> primary  (still inconsistent)
    #   secondary             (finally consistent)
    TARGET = 2
    reads = []
    latest_primary_version = TARGET
    last_secondary_version = None

    def sleep_until_after_write(delay_ms: int) -> None:
        target_abs = t0 + write_returned_ms + delay_ms
        sleep_s = (target_abs - ms()) / 1000
        if sleep_s > 0:
            time.sleep(sleep_s)

    def observe_secondary(label: str) -> dict:
        nonlocal last_secondary_version
        t_read = ms() - t0
        doc = db.get_secondary()["positions"].find_one({"_id": item_id})
        t_end = ms() - t0
        version = doc.get("version") if doc else None
        stale = version != latest_primary_version
        last_secondary_version = version
        obs = {
            "node":             "secondary",
            "label":            label,
            "t_ms":             t_read,
            "t_end_ms":         t_end,
            "version":          version,
            "reference_version": latest_primary_version,
            "stale":            stale,
            "nodes_consistent": not stale,
            "status":           "follower lagging behind leader" if stale else "all nodes consistent",
        }
        reads.append(obs)
        return obs

    def observe_primary(label: str) -> dict:
        nonlocal latest_primary_version
        t_read = ms() - t0
        doc = db.get_primary()["positions"].find_one({"_id": item_id})
        t_end = ms() - t0
        version = doc.get("version") if doc else None
        latest_primary_version = version
        inconsistent_with_secondary = last_secondary_version != version
        obs = {
            "node":             "primary",
            "label":            label,
            "t_ms":             t_read,
            "t_end_ms":         t_end,
            "version":          version,
            "reference_version": last_secondary_version,
            "stale":            False,
            "nodes_consistent": not inconsistent_with_secondary,
            "status": (
                f"leader has v{version}; follower still v{last_secondary_version}"
                if inconsistent_with_secondary else
                "leader and follower match"
            ),
        }
        reads.append(obs)
        return obs

    try:
        sleep_until_after_write(200)
        observe_secondary("read secondary #1")
        observe_primary("read primary #1 (leader reference)")

        sleep_until_after_write(550)
        observe_secondary("read secondary #2")
        observe_primary("read primary #2 (leader reference)")

        # Final observation: wait until the follower is expected to have applied
        # the delayed oplog, then record the first secondary read that sees v2.
        sleep_until_after_write((_SECONDARY_DELAY_SECS * 1000) + 150)
        deadline = time.time() + 3.0
        final_obs = None
        while time.time() < deadline:
            final_obs = observe_secondary("read secondary #3 (eventual convergence)")
            if final_obs["nodes_consistent"]:
                break
            # Internal polling noise is not useful for the teaching timeline.
            reads.pop()
            time.sleep(0.1)
        if final_obs and final_obs not in reads:
            reads.append(final_obs)
    finally:
        _set_secondary_delay(prev_delay)

    # ── Derive consistency metrics ─────────────────────────────────────────────
    first_consistent = next(
        (r for r in reads if r["node"] == "secondary" and r["nodes_consistent"]),
        None,
    )
    synced_ms = first_consistent["t_ms"] if first_consistent else None
    stale_count = sum(1 for r in reads if r["node"] == "secondary" and r["stale"])
    consistency_window = round(synced_ms - write_start_ms, 1) if synced_ms else None

    log     = get_log(item_id)
    repl_ms = (log.get("replication_delay_ms") if log else None) or consistency_window

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

    # 4. Observation reads in the exact sequence shown to the user.
    for r in reads:
        if r["node"] == "secondary":
            resp_label = (
                f"⚡ secondary v{r['version']} vs primary v{r['reference_version']} (inconsistent)"
                if r["stale"] else
                f"✓ secondary v{r['version']} = primary v{r['reference_version']} (finally consistent)"
            )
            events.extend(req_resp_events(
                r["t_ms"], r["t_end_ms"], net_s,
                "client", "secondary",
                r["label"], "read",
                resp_label,
                "stale_response" if r["stale"] else "fresh_response",
            ))
            continue

        resp_label = (
            f"✓ primary v{r['version']} — leader ahead of secondary v{r['reference_version']}"
            if not r["nodes_consistent"] else
            f"✓ primary v{r['version']} — leader/follower match"
        )
        events.extend(req_resp_events(
            r["t_ms"], r["t_end_ms"], net_p,
            "client", "primary",
            r["label"], "read",
            resp_label, "fresh_response",
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
            "stale_read_observed":      stale_count > 0,
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
                    "at_ms":      round(r["t_ms"], 1),
                    "node":       r["node"],
                    "version":    f"v{r['version']}",
                    "reference":  f"v{r['reference_version']}" if r["reference_version"] is not None else None,
                    "consistent": r["nodes_consistent"],
                }
                for r in reads
            ],
        },
    }
