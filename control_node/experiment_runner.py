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
    """Eventual consistency demonstrasyonu:

    1. w=majority ile v1 insert → secondary sync bekle (baseline).
    2. Secondary'den 1 baseline read (v1 görünmeli).
    3. w=1 ile v2 update → OK hemen döner (no secondary wait).
    4. Birden fazla secondary read → stale (v1) → ... → fresh (v2).
    """
    # ── Phase 1: Establish v1 on both nodes ───────────────────────
    operations.set_write_concern("majority")
    item_id, _ = operations.insert_position(
        "EC-BASE", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    # Wait until secondary has confirmed v1
    for _ in range(100):
        sec = db.get_secondary()["positions"].find_one({"_id": item_id})
        if sec and sec.get("version") == 1:
            break
        time.sleep(0.015)

    t0 = _ms()

    # ── Phase 2: One baseline read from secondary (shows v1) ───────
    t_b = _ms() - t0
    baseline_doc = db.get_secondary()["positions"].find_one({"_id": item_id})
    t_b_end = _ms() - t0
    baseline_version = baseline_doc.get("version") if baseline_doc else None

    # ── Phase 3: w=1 update → v2 (no secondary wait) ──────────────
    operations.set_write_concern(1)
    write_start_ms = _ms() - t0
    operations.update_item(item_id, {
        "vehicle_id": "EC-BASE",
        "lat": 41.0082, "lng": 28.9784,
        "city": "Istanbul", "district": "Besiktas",
        "speed_kmh": 90,
    }, collection="positions")
    write_returned_ms = _ms() - t0
    operations.set_write_concern("majority")

    # ── Phase 4: Multiple secondary reads at increasing intervals ──
    TARGET = 2
    reads = []
    for delay_ms in [0, 10, 20, 40, 80, 160, 320, 640]:
        target_abs = t0 + write_returned_ms + delay_ms
        sleep_s = (target_abs - _ms()) / 1000
        if sleep_s > 0:
            time.sleep(sleep_s)

        t_r = _ms() - t0
        doc = db.get_secondary()["positions"].find_one({"_id": item_id})
        t_r_end = _ms() - t0

        version = doc.get("version") if doc else None
        is_stale = (version != TARGET)
        reads.append({
            "t_ms":             t_r,
            "t_end_ms":         t_r_end,
            "found":            doc is not None,
            "version":          version,
            "stale_after_write": is_stale,
            "status":           f"v{version}" if version else "—",
        })
        if not is_stale:
            break  # secondary caught up

    stale_observed = any(r["stale_after_write"] for r in reads)
    first_fresh    = next((r for r in reads if not r["stale_after_write"]), None)
    synced_ms      = first_fresh["t_ms"] if first_fresh else None

    log     = _get_log(item_id)
    repl_ms = log.get("replication_delay_ms") if log else synced_ms

    # ── Build timeline events — sadece gerçek ölçülen değerler ───────
    update_duration = write_returned_ms - write_start_ms

    # Yarı-RTT: Python'un gönderim ve alım zamanları arasındaki gerçek farkın yarısı.
    # Bu tek varsayım; bunun dışında hiçbir şey uydurulmaz.
    half_write = max(update_duration / 2, 1.0)

    # Oplog gecikmesi için en iyi kaynak: operation_logs'taki repl_ms.
    # Bu değer MongoDB'nin kendi server-side timestamp'larından hesaplanır:
    #   replication_delay_ms = follower_visible_time - leader_write_time
    # Yoksa Python'un secondary'de v2 gördüğü anı (synced_ms) kullan.
    primary_commit_ms = write_start_ms + half_write
    if repl_ms:
        oplog_arrive = primary_commit_ms + repl_ms
    elif synced_ms:
        oplog_arrive = synced_ms
    else:
        oplog_arrive = write_returned_ms + 50

    # Baseline read half-RTT (gerçek ölçüm)
    half_b = max((t_b_end - t_b) / 2, 0.5)

    events = [
        # 1. Baseline: update'ten önce secondary'den 1 read
        #    t_b          → Python isteği gönderdi
        #    t_b + half_b → secondary aldı ve yanıtladı
        #    t_b_end      → Python yanıtı aldı
        {"t_ms": t_b,
         "latency_ms": half_b,
         "from": "client", "to": "secondary",
         "label": "read (baseline)", "type": "read"},
        {"t_ms": t_b + half_b,
         "latency_ms": half_b,
         "from": "secondary", "to": "client",
         "label": f"v{baseline_version} (synced)", "type": "fresh_response"},

        # 2. w=1 update
        #    write_start_ms           → Python isteği gönderdi
        #    write_start_ms+half_write → primary aldı
        #    write_returned_ms-half_write → primary OK gönderdi (secondary beklemedi)
        #    write_returned_ms        → Python OK aldı
        {"t_ms": write_start_ms,
         "latency_ms": half_write,
         "from": "client", "to": "primary",
         "label": "update (w=1, v1→v2)", "type": "write"},
        {"t_ms": write_returned_ms - half_write,
         "latency_ms": half_write,
         "from": "primary", "to": "client",
         "label": f"ok ({update_duration:.1f}ms) — no secondary wait", "type": "ok"},

        # 3. Oplog async
        #    primary_commit_ms → primary commit etti, oplog'u gönderdi
        #    oplog_arrive      → secondary aldı (repl_ms veya synced_ms ile ölçüldü)
        {"t_ms": primary_commit_ms,
         "latency_ms": max(oplog_arrive - primary_commit_ms, 2),
         "from": "primary", "to": "secondary",
         "label": f"oplog async — {max(oplog_arrive - primary_commit_ms, 0):.1f}ms",
         "type": "replicate"},
    ]

    # ── Post-update secondary reads ────────────────────────────────
    for r in reads:
        half_r = max((r["t_end_ms"] - r["t_ms"]) / 2, 0.5)
        if r["stale_after_write"]:
            resp_label = f"⚡ stale (v{r['version']} — old)"
            resp_type  = "stale_response"
        else:
            resp_label = f"✓ fresh (v{r['version']})"
            resp_type  = "fresh_response"

        events.append({"t_ms": r["t_ms"],          "latency_ms": half_r,
                        "from": "client",    "to": "secondary",
                        "label": "read",              "type": "read"})
        events.append({"t_ms": r["t_ms"] + half_r, "latency_ms": half_r,
                        "from": "secondary", "to": "client",
                        "label": resp_label,           "type": resp_type})

    return {
        "experiment":  "eventual_consistency",
        "title":       "Eventual Consistency (w=1)",
        "description": "Primary w=1 ile günceller — OK secondary beklenmeden döner. Secondary stale (eski) veriyi gösterir, sonra yakalar.",
        "events":      events,
        "log":         _serialize_log(log),
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
