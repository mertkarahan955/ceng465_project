"""
CENG465 Consistency Experiment Runner

Her deneyi çalıştırır ve DDIA-style timeline için gerekli event'leri üretir.
Her event:
  t_ms       — gönderim anı (deney başından ms cinsinden)
  latency_ms — iletim gecikmesi (ok eğimini belirler)
  from / to  — aktör isimleri: "client" | "primary" | "secondary"
  label      — ok üzerindeki metin
  type       — renk kodlaması için
"""

import json
import os
import time
from datetime import datetime, timezone

from bson import ObjectId

import db
import operations


# ── Paths ─────────────────────────────────────────────────────────────────────

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "experiment_results")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ms():
    return time.time() * 1000


def _get_log(item_id):
    return db.get_primary()["operation_logs"].find_one({"target_id": item_id})


def _serialize_log(log):
    if not log:
        return None
    return {
        "log_index":           log.get("log_index"),
        "operation_id":        str(log.get("operation_id", ""))[:8],
        "version_before":      log.get("version_before"),
        "version_after":       log.get("version_after"),
        "write_concern":       str(log.get("write_concern", "")),
        "status":              log.get("status"),
        "replication_delay_ms": log.get("replication_delay_ms"),
        "leader_write_time":   log.get("leader_write_time").isoformat() if log.get("leader_write_time") else None,
        "follower_visible_time": log.get("follower_visible_time").isoformat() if log.get("follower_visible_time") else None,
        "target_collection":   log.get("target_collection"),
    }


def _save_result(name: str, result: dict) -> str:
    """Save experiment result as JSON. Returns the filename."""
    folder = os.path.join(RESULTS_DIR, name)
    os.makedirs(folder, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:20]
    filename = f"{ts}.json"
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"saved_at": datetime.now().isoformat(), **result}, f, ensure_ascii=False, indent=2)
    return filename


def list_results(name: str) -> list:
    """List saved results for a given experiment, newest first."""
    folder = os.path.join(RESULTS_DIR, name)
    if not os.path.isdir(folder):
        return []
    files = sorted(
        [f for f in os.listdir(folder) if f.endswith(".json")],
        reverse=True,
    )
    out = []
    for fname in files[:20]:
        path = os.path.join(folder, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            out.append({
                "filename": fname,
                "saved_at": data.get("saved_at", ""),
                "summary":  data.get("summary", {}),
            })
        except Exception:
            pass
    return out


def load_result(name: str, filename: str) -> dict:
    """Load a specific saved result."""
    path = os.path.join(RESULTS_DIR, name, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Dispatcher ────────────────────────────────────────────────────────────────

def run_experiment(name):
    registry = {
        "sync_replication":    _run_sync_replication,
        "eventual_consistency": _run_eventual_consistency,
        "read_after_write":    _run_read_after_write,
        "monotonic_reads":     _run_monotonic_reads,
    }
    fn = registry.get(name)
    if not fn:
        raise ValueError(f"Unknown experiment: {name!r}")
    result = fn()
    _save_result(name, result)
    return result


# ── Experiment 1: Synchronous Replication (w=majority) ────────────────────────

def _run_sync_replication():
    """w=majority: Primary follower'ın ACK'ini bekledikten sonra istemciye döner."""
    operations.set_write_concern("majority")

    t0 = _ms()
    item_id, delay_ms = operations.insert_position(
        "SYNC-EXP", 41.0082, 28.9784, "Istanbul", "Besiktas", 65
    )
    t1 = _ms()

    d = delay_ms if delay_ms is not None else (t1 - t0)
    log = _get_log(item_id)

    # Sub-timings proportional to measured round-trip d.
    # Use symmetric half-RTT for each request-response pair so arrows form clean V-shapes.
    half_cli = max(d * 0.06, 1.5)   # client ↔ primary one-way estimate
    oplog_start = max(half_cli * 2, d * 0.10)
    sec_apply   = d * 0.70
    sec_ack     = d * 0.78
    # ok departs from primary at d - half_cli, arrives at client at d
    ok_depart   = d - half_cli

    events = [
        # Client sends write; arrives at primary at half_cli*2 (round-trip split)
        {"t_ms": 0,           "latency_ms": half_cli * 2,            "from": "client",    "to": "primary",   "label": "write (w=majority)", "type": "write"},
        # Primary replicates to secondary
        {"t_ms": oplog_start, "latency_ms": sec_apply - oplog_start, "from": "primary",   "to": "secondary", "label": "oplog → apply",      "type": "replicate"},
        # Secondary ACKs; arrives at primary at ok_depart
        {"t_ms": sec_ack,     "latency_ms": ok_depart - sec_ack,     "from": "secondary", "to": "primary",   "label": "ACK ✓",             "type": "ack"},
        # ok departs exactly when ACK arrives → V-shape with ACK
        {"t_ms": ok_depart,   "latency_ms": half_cli,                "from": "primary",   "to": "client",    "label": f"ok ({d:.1f} ms)",  "type": "ok"},
    ]

    return {
        "experiment":  "sync_replication",
        "title":       "Synchronous Replication (w=majority)",
        "description": "Primary, follower ACK'ini bekler. İstemci ancak secondary onayladıktan sonra 'ok' alır. Daha güvenli ama daha yavaş.",
        "events":      events,
        "log":         _serialize_log(log),
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


# ── Experiment 2: Eventual Consistency (w=1) ──────────────────────────────────

def _run_eventual_consistency():
    """w=1: Primary hemen döner, secondary async olarak güncellenir."""
    operations.set_write_concern(1)

    t0 = _ms()
    item_id, _ = operations.insert_position(
        "EC-EXP", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    t1 = _ms()
    write_returned_ms = t1 - t0  # ~2-5ms fire-and-forget

    # Immediate stale read from secondary
    t_r1 = _ms() - t0
    sec_doc_now = db.get_secondary()["positions"].find_one({"_id": item_id})
    t_r1_end = _ms() - t0
    stale = sec_doc_now is None

    # Wait for eventual consistency
    synced_ms = None
    deadline = time.time() + 5.0
    while time.time() < deadline:
        doc = db.get_secondary()["positions"].find_one({"_id": item_id})
        if doc:
            synced_ms = _ms() - t0
            break
        time.sleep(0.01)

    log = _get_log(item_id)
    repl_ms = log.get("replication_delay_ms") if log else synced_ms
    operations.set_write_concern("majority")

    sync_t = synced_ms or (write_returned_ms + 100)
    oplog_arrive = sync_t * 0.75  # when oplog reaches secondary

    events = [
        # Client writes (w=1)
        {"t_ms": 0,                  "latency_ms": 3,                       "from": "client",    "to": "primary",   "label": "write (w=1)",                              "type": "write"},
        # Primary returns ok immediately (no waiting for secondary)
        {"t_ms": write_returned_ms,  "latency_ms": 3,                       "from": "primary",   "to": "client",    "label": f"ok ({write_returned_ms:.1f}ms) — immediately", "type": "ok"},
        # Primary sends oplog async
        {"t_ms": 3,                  "latency_ms": oplog_arrive - 3,        "from": "primary",   "to": "secondary", "label": "oplog (async)",                            "type": "replicate"},
    ]

    # Stale read immediately after write
    if stale:
        rtt_r1 = t_r1_end - t_r1
        half_r1 = max(rtt_r1 / 2, 1.0)
        events += [
            {"t_ms": t_r1,           "latency_ms": half_r1, "from": "client",    "to": "secondary", "label": "read (fleet overview)",  "type": "read"},
            {"t_ms": t_r1 + half_r1, "latency_ms": half_r1, "from": "secondary", "to": "client",    "label": "⚡ stale! (not synced)", "type": "stale_response"},
        ]

    # Secondary catches up, fresh read
    if synced_ms:
        events += [
            {"t_ms": oplog_arrive,   "latency_ms": 3,   "from": "secondary", "to": "primary",   "label": "oplog applied",                             "type": "ack"},
            {"t_ms": sync_t + 5,     "latency_ms": 3,   "from": "client",    "to": "secondary", "label": "read again",                                "type": "read"},
            {"t_ms": sync_t + 9,     "latency_ms": 3,   "from": "secondary", "to": "client",    "label": f"✓ fresh ({(repl_ms or sync_t):.1f}ms total)", "type": "fresh_response"},
        ]

    return {
        "experiment":  "eventual_consistency",
        "title":       "Eventual Consistency (w=1)",
        "description": "Primary hemen döner. Secondary geride kalabilir ama eninde sonunda yakalar. Stale read gözlemlenebilir.",
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":         "1 (async)",
            "write_returned_ms":     round(write_returned_ms, 2),
            "replication_delay_ms":  round(repl_ms, 2) if repl_ms else None,
            "consistency_model":     "Eventual",
            "stale_read_observed":   stale,
            "consistency_window_ms": round(synced_ms, 2) if synced_ms else None,
            "consistency_achieved":  synced_ms is not None,
        },
    }


# ── Experiment 3: Read-After-Write (w=1, async) ───────────────────────────────

def _run_read_after_write():
    """w=1 ile yazar, primary'den okur (RAW) — her zaman taze.
    Secondary'den okursa stale gelebilir çünkü w=1 async replication.

    Senaryo: Dispatcher incident girer → primary RAW path ile doğrular.
    w=1 olduğu için ok çok hızlı döner ama secondary geride kalabilir.
    """
    operations.set_write_concern(1)  # async — fast write

    t0 = _ms()
    item_id, _ = operations.insert_incident(
        "RAW-EXP", "breakdown", "critical",
        "Dispatcher files incident — Read-After-Write experiment (w=1)"
    )
    t1 = _ms()
    write_ms = t1 - t0  # ~3-5ms with w=1

    # Read from PRIMARY (read-after-write path) — always fresh
    t_rp1 = _ms() - t0
    pri_doc = db.get_primary()["incidents"].find_one({"_id": item_id})
    t_rp2 = _ms() - t0

    # Read from SECONDARY — may be stale with w=1
    time.sleep(0.010)
    t_rs1 = _ms() - t0
    sec_doc = db.get_secondary()["incidents"].find_one({"_id": item_id})
    t_rs2 = _ms() - t0

    log = _get_log(item_id)
    d = write_ms

    # Use half-RTT for each measured request-response pair so that
    # the response arrow always starts where the request arrow ends (V-shape, no X-crossing).
    half_d    = max(d / 2, 1.0)
    rtt_p     = t_rp2 - t_rp1
    half_rtt_p = max(rtt_p / 2, 1.0)
    rtt_s     = t_rs2 - t_rs1
    half_rtt_s = max(rtt_s / 2, 1.0)

    events = [
        # Write (w=1): half_d each way → V-shape, ok departs exactly when write arrives
        {"t_ms": 0,                 "latency_ms": half_d,      "from": "client",    "to": "primary",   "label": "write incident (w=1)",               "type": "write"},
        {"t_ms": half_d,            "latency_ms": half_d,      "from": "primary",   "to": "client",    "label": f"ok ({d:.1f}ms) — immediately",      "type": "ok"},
        # Async oplog starts when write arrives at primary
        {"t_ms": half_d,            "latency_ms": 30,          "from": "primary",   "to": "secondary", "label": "oplog (async)",                      "type": "replicate"},
        # Primary read: half-RTT each way → V-shape
        {"t_ms": t_rp1,             "latency_ms": half_rtt_p,  "from": "client",    "to": "primary",   "label": "read — RAW path (primary)",          "type": "read"},
        {"t_ms": t_rp1 + half_rtt_p,"latency_ms": half_rtt_p,  "from": "primary",   "to": "client",    "label": f"✓ fresh ({rtt_p:.1f}ms) — always",  "type": "fresh_response"},
        # Secondary read: half-RTT each way → V-shape, no crossing regardless of RTT
        {"t_ms": t_rs1,             "latency_ms": half_rtt_s,  "from": "client",    "to": "secondary", "label": "read — secondary (comparison)",      "type": "read"},
        {"t_ms": t_rs1 + half_rtt_s,"latency_ms": half_rtt_s,  "from": "secondary", "to": "client",
         "label": f"{'✓ fresh' if sec_doc else '⚡ stale'} ({rtt_s:.1f}ms)",
         "type": "fresh_response" if sec_doc else "stale_response"},
    ]

    operations.set_write_concern("majority")

    return {
        "experiment":  "read_after_write",
        "title":       "Read-After-Write Consistency (w=1, async)",
        "description": "w=1 ile yazılır — primary'den her zaman taze okunur. Secondary w=1 nedeniyle geride kalabilir.",
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":        "1 (async)",
            "write_returned_ms":    round(d, 2),
            "primary_read_ms":      round(t_rp2 - t_rp1, 2),
            "primary_fresh":        pri_doc is not None,
            "secondary_fresh":      sec_doc is not None,
            "consistency_model":    "Read-After-Write",
            "consistency_achieved": pri_doc is not None,
        },
    }


# ── Experiment 4: Monotonic Reads ─────────────────────────────────────────────

def _run_monotonic_reads():
    """Secondary'den okunan version numarası asla geri gidemez.

    w=1 kullanarak lag oluşturur, sonra secondary'den ardışık okumalar yapar.
    """
    operations.set_write_concern(1)

    t0 = _ms()

    shp_id, _ = operations.insert_shipment(
        "MON-EXP", "DEP-IST", "DEP-ANK", "MonotonicTest",
        300, 3, status="pending"
    )
    write_times = [_ms() - t0]

    statuses = ["in_transit", "in_transit", "delivered"]
    for status in statuses:
        operations.update_item(shp_id, {
            "shipment_id": "MON-EXP", "origin_depot": "DEP-IST",
            "destination_depot": "DEP-ANK", "customer": "MonotonicTest",
            "weight_kg": 300, "package_count": 3,
            "status": status, "assigned_vehicle_id": None,
        }, collection="shipments")
        write_times.append(_ms() - t0)

    # Read from secondary at intervals to show version progression
    reads = []
    time.sleep(0.03)
    for _ in range(5):
        t_r = _ms() - t0
        doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
        t_r_end = _ms() - t0
        reads.append({
            "t_ms":    t_r,
            "t_end_ms": t_r_end,
            "version": doc.get("version") if doc else None,
            "status":  doc.get("value", {}).get("status") if doc else "—",
        })
        time.sleep(0.08)

    versions = [r["version"] for r in reads if r["version"] is not None]
    monotonic = all(versions[i] <= versions[i+1] for i in range(len(versions)-1))

    operations.set_write_concern("majority")

    # Build events — ok starts AFTER write arrives at primary (no crossing)
    events = []
    labels = ["insert (pending)", "Update: in_transit", "Update: in_transit", "Update: delivered"]
    for i, (t_ms, lbl) in enumerate(zip(write_times, labels)):
        # Write: client → primary (3ms latency, going down)
        events.append({"t_ms": t_ms,     "latency_ms": 3,  "from": "client",  "to": "primary",   "label": lbl,            "type": "write"})
        # ok: primary → client, starts AFTER write arrives (t_ms+3), clean V shape
        events.append({"t_ms": t_ms + 3, "latency_ms": 3,  "from": "primary", "to": "client",    "label": f"ok v{i+1}",   "type": "ok"})
        # Async oplog: starts after write arrives, long latency
        events.append({"t_ms": t_ms + 4, "latency_ms": 28, "from": "primary", "to": "secondary", "label": f"oplog v{i+1}", "type": "replicate"})

    for r in reads:
        v = r["version"]
        s = r["status"]
        half_rtt = max((r["t_end_ms"] - r["t_ms"]) / 2, 1.0)
        events.append({"t_ms": r["t_ms"],            "latency_ms": half_rtt, "from": "client",    "to": "secondary", "label": "read",               "type": "read"})
        events.append({"t_ms": r["t_ms"] + half_rtt, "latency_ms": half_rtt, "from": "secondary", "to": "client",
                        "label": f"v{v} ({s})" if v else "—",
                        "type": "fresh_response" if v else "stale_response"})

    return {
        "experiment":  "monotonic_reads",
        "title":       "Monotonic Reads",
        "description": "Secondary'den ardışık okumalar asla önceki versiyondan düşük dönemez. w=1 ile lag görünür hale gelir.",
        "events":      events,
        "log":         None,
        "reads":       reads,
        "summary": {
            "write_concern":        "1 (async)",
            "versions_seen":        versions,
            "monotonic":            monotonic,
            "consistency_model":    "Monotonic Reads",
            "consistency_achieved": monotonic,
            "final_version":        max(versions) if versions else None,
        },
    }
