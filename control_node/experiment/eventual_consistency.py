"""Experiment 2 — Eventual Consistency (w=1)."""

import time

import config
import db
import operations

from .common import (
    get_log,
    measure_net_one_way,
    ms,
    req_resp_events,
    safe_lat,
    serialize_log,
)


def set_secondary_delay(delay_secs: int):
    """Set secondaryDelaySecs on the secondary replica set member."""
    admin = db.get_primary().client["admin"]
    cfg = admin.command("replSetGetConfig")["config"]

    sec_idx = None
    for i, m in enumerate(cfg["members"]):
        if m.get("priority", 1) == 0:
            sec_idx = i
            break
    if sec_idx is None:
        sec_idx = 1

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
    """Eventual consistency demonstrasyonu."""
    net_p = measure_net_one_way(db.get_primary,   config.PRIMARY_NODE_SERVER_URL)
    net_s = measure_net_one_way(db.get_secondary, config.SECONDARY_NODE_SERVER_URL)

    operations.set_write_concern(1)
    item_id, _ = operations.insert_position(
        "EC-BASE", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    for _ in range(200):
        sec = db.get_secondary()["positions"].find_one({"_id": item_id})
        if sec and sec.get("version") == 1:
            break
        time.sleep(0.015)

    delay_secs = 1
    prev_delay = set_secondary_delay(delay_secs)

    t0 = ms()

    t_b = ms() - t0
    baseline_doc = db.get_secondary()["positions"].find_one({"_id": item_id})
    t_b_end = ms() - t0
    baseline_version = baseline_doc.get("version") if baseline_doc else None

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

    target = 2
    reads = []
    try:
        for delay_ms in [200, 500, 1000, 1500, 2000]:
            target_abs = t0 + write_returned_ms + delay_ms
            sleep_s = (target_abs - ms()) / 1000
            if sleep_s > 0:
                time.sleep(sleep_s)

            t_r = ms() - t0
            doc = db.get_secondary()["positions"].find_one({"_id": item_id})
            t_r_end = ms() - t0

            version = doc.get("version") if doc else None
            is_stale = (version != target)
            reads.append({
                "t_ms":              t_r,
                "t_end_ms":          t_r_end,
                "found":             doc is not None,
                "version":           version,
                "stale_after_write": is_stale,
                "status":            f"v{version}" if version else "—",
            })
            if not is_stale:
                break
    finally:
        set_secondary_delay(prev_delay)

    stale_observed = any(r["stale_after_write"] for r in reads)
    first_fresh = next((r for r in reads if not r["stale_after_write"]), None)
    synced_ms = first_fresh["t_ms"] if first_fresh else None

    log = get_log(item_id)
    repl_ms = log.get("replication_delay_ms") if log else synced_ms

    update_duration = write_returned_ms - write_start_ms
    primary_commit_ms = write_start_ms + net_p
    secondary_applied_ms = synced_ms if synced_ms else (
        (primary_commit_ms + repl_ms) if repl_ms else (write_returned_ms + 50)
    )
    total_repl_ms = secondary_applied_ms - write_start_ms

    events = [
        *req_resp_events(
            t_b, t_b_end, net_s,
            "client", "secondary",
            "read (baseline)", "read",
            f"v{baseline_version} (synced)", "fresh_response",
        ),
        *req_resp_events(
            write_start_ms, write_returned_ms, net_p,
            "client", "primary",
            "update (w=1, v1→v2)", "write",
            f"ok ({update_duration:.1f}ms) — no secondary wait", "ok",
        ),
        {"t_ms": primary_commit_ms,
         "latency_ms": safe_lat(net_s),
         "from": "primary", "to": "secondary",
         "label": "oplog async",
         "type": "replicate"},
        {"t_ms": secondary_applied_ms,
         "latency_ms": safe_lat(net_s),
         "from": "secondary", "to": "primary",
         "label": f"✓ write applied (v2) — {total_repl_ms:.0f}ms after write",
         "type": "ack"},
    ]

    for r in reads:
        if r["stale_after_write"]:
            resp_label = f"⚡ stale (v{r['version']} — old)"
            resp_type = "stale_response"
        else:
            resp_label = f"✓ fresh (v{r['version']})"
            resp_type = "fresh_response"

        events.extend(req_resp_events(
            r["t_ms"], r["t_end_ms"], net_s,
            "client", "secondary",
            "read", "read",
            resp_label, resp_type,
        ))

    return {
        "experiment":  "eventual_consistency",
        "title":       "Eventual Consistency (w=1)",
        "description": "Primary w=1 ile günceller — OK secondary beklenmeden döner. Secondary stale (eski) veriyi gösterir, sonra yakalar.",
        "events":      events,
        "log":         serialize_log(log),
        "reads":       reads,
        "summary": {
            "write_concern":         "1 (async)",
            "write_returned_ms":     round(update_duration, 2),
            "replication_delay_ms":  round(repl_ms, 2) if repl_ms else None,
            "consistency_model":     "Eventual",
            "stale_read_observed":   stale_observed,
            "consistency_window_ms": round(synced_ms, 2) if synced_ms else None,
            "consistency_achieved":  synced_ms is not None,
        },
    }
