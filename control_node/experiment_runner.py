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

import time
from datetime import timezone
from bson import ObjectId

import db
import operations


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
    return fn()


# ── Experiment 1: Synchronous Replication (w=majority) ────────────────────────

def _run_sync_replication():
    """w=majority: Primary follower'ın ACK'ini bekledikten sonra istemciye döner.

    Diagram:
      Client  --- write (w=majority) ----------> ok -->
      Primary --- oplog --> secondary --> ACK --> response
      Secondary  <-- applies oplog, sends ACK
    """
    operations.set_write_concern("majority")

    t0 = _ms()
    item_id, delay_ms = operations.insert_position(
        "SYNC-EXP", 41.0082, 28.9784, "Istanbul", "Besiktas", 65
    )
    t1 = _ms()

    d = delay_ms if delay_ms is not None else (t1 - t0)
    log = _get_log(item_id)

    # Estimated sub-timings (proportional to measured delay)
    cli_to_pri  = 3.0             # client → primary: local network
    oplog_send  = d * 0.12        # primary starts sending oplog
    sec_apply   = d * 0.72        # secondary applies
    sec_ack     = d * 0.80        # secondary sends ACK
    pri_to_cli  = d               # primary → client: final ok

    events = [
        # Write request
        {"t_ms": 0,          "latency_ms": cli_to_pri,         "from": "client",    "to": "primary",   "label": "write (w=majority)",   "type": "write"},
        # Horizontal "waiting" marker on primary
        {"t_ms": cli_to_pri, "latency_ms": 0,                  "from": "primary",   "to": "primary",   "label": "⏳ waiting for follower's ok", "type": "waiting"},
        # Oplog to secondary
        {"t_ms": oplog_send, "latency_ms": sec_apply - oplog_send, "from": "primary",   "to": "secondary", "label": "oplog → apply",        "type": "replicate"},
        # ACK back
        {"t_ms": sec_ack,    "latency_ms": pri_to_cli - sec_ack,   "from": "secondary", "to": "primary",   "label": "ACK ✓",               "type": "ack"},
        # Final ok to client
        {"t_ms": pri_to_cli, "latency_ms": 2,                  "from": "primary",   "to": "client",    "label": f"ok ({d:.1f} ms)",     "type": "ok"},
    ]

    return {
        "experiment":  "sync_replication",
        "title":       "Synchronous Replication (w=majority)",
        "description": "Primary, follower ACK'ini bekler. İstemci ancak secondary onayladıktan sonra 'ok' alır. Daha güvenli ama daha yavaş.",
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":          "majority",
            "write_returned_ms":      round(d, 2),
            "replication_delay_ms":   round(d, 2),
            "consistency_model":      "Synchronous",
            "version_after":          1,
            "consistency_achieved":   True,
            "stale_read_possible":    False,
        },
    }


# ── Experiment 2: Eventual Consistency (w=1) ──────────────────────────────────

def _run_eventual_consistency():
    """w=1: Primary hemen döner, secondary async olarak güncellenir.

    Diagram:
      Client  ──write──>                                ────────>
                      ok (immediately) ↑
                                       |
      Primary ─────────────────────────────────────────────────>
                      ↓ oplog (async)
      Secondary ──────────────────↓ apply ──────────────────────>

      İlk read (stale): Client → Secondary → "not synced yet!"
      İkinci read (fresh): Client → Secondary → fresh ✓
    """
    operations.set_write_concern(1)

    t0 = _ms()
    item_id, _ = operations.insert_position(
        "EC-EXP", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    t1 = _ms()
    write_returned_ms = t1 - t0  # should be ~2-5ms (fire-and-forget)

    # Stale read: immediate secondary read
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

    events = [
        # Write (fire-and-forget)
        {"t_ms": 0,                "latency_ms": 3,                  "from": "client",    "to": "primary",   "label": "write (w=1)",               "type": "write"},
        # Primary immediately returns ok
        {"t_ms": write_returned_ms,"latency_ms": 2,                  "from": "primary",   "to": "client",    "label": f"ok ({write_returned_ms:.1f}ms) — immediately", "type": "ok"},
        # Async oplog to secondary
        {"t_ms": write_returned_ms,"latency_ms": sync_t * 0.7,       "from": "primary",   "to": "secondary", "label": "oplog (async)",              "type": "replicate"},
    ]

    # Immediate stale read
    if stale:
        events += [
            {"t_ms": t_r1,          "latency_ms": 2,  "from": "client",    "to": "secondary", "label": "read (fleet overview)",  "type": "read"},
            {"t_ms": t_r1_end,      "latency_ms": 2,  "from": "secondary", "to": "client",    "label": "⚡ stale! (not synced)", "type": "stale_response"},
        ]

    # Secondary catches up + fresh read
    if synced_ms:
        events += [
            {"t_ms": sync_t * 0.7, "latency_ms": sync_t * 0.2,  "from": "secondary", "to": "primary",   "label": "oplog applied",              "type": "ack"},
            {"t_ms": sync_t + 5,   "latency_ms": 2,              "from": "client",    "to": "secondary", "label": "read again",                 "type": "read"},
            {"t_ms": sync_t + 8,   "latency_ms": 2,              "from": "secondary", "to": "client",    "label": f"✓ fresh ({(repl_ms or sync_t):.1f}ms total)", "type": "fresh_response"},
        ]

    return {
        "experiment":  "eventual_consistency",
        "title":       "Eventual Consistency (w=1)",
        "description": "Primary hemen döner. Secondary geride kalabilir ama eninde sonunda yakalar. Stale read gözlemlenebilir.",
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":           "1 (async)",
            "write_returned_ms":       round(write_returned_ms, 2),
            "replication_delay_ms":    round(repl_ms, 2) if repl_ms else None,
            "consistency_model":       "Eventual",
            "stale_read_observed":     stale,
            "consistency_window_ms":   round(synced_ms, 2) if synced_ms else None,
            "consistency_achieved":    synced_ms is not None,
        },
    }


# ── Experiment 3: Read-After-Write ────────────────────────────────────────────

def _run_read_after_write():
    """Kullanıcı kendi yazdığını primary'den anında okur.

    Senaryo: Dispatcher critical incident girer → hemen doğrulamak ister.
    Primary'den okursa (RAW path): her zaman taze.
    Secondary'den okursa: w=majority ise taze, w=1 ise stale olabilir.
    """
    operations.set_write_concern("majority")

    t0 = _ms()
    item_id, delay_ms = operations.insert_incident(
        "RAW-EXP", "breakdown", "critical",
        "Dispatcher files incident — Read-After-Write experiment"
    )
    t1 = _ms()
    write_ms = delay_ms if delay_ms is not None else (t1 - t0)

    # Read from PRIMARY (read-after-write path)
    t_rp1 = _ms() - t0
    pri_doc = db.get_primary()["incidents"].find_one({"_id": item_id})
    t_rp2 = _ms() - t0

    # Read from SECONDARY (comparison)
    time.sleep(0.005)
    t_rs1 = _ms() - t0
    sec_doc = db.get_secondary()["incidents"].find_one({"_id": item_id})
    t_rs2 = _ms() - t0

    log = _get_log(item_id)
    d = write_ms

    events = [
        # Write
        {"t_ms": 0,         "latency_ms": 3,        "from": "client",    "to": "primary",   "label": "write incident (w=majority)",   "type": "write"},
        {"t_ms": d * 0.12,  "latency_ms": d * 0.62, "from": "primary",   "to": "secondary", "label": "oplog → apply",                 "type": "replicate"},
        {"t_ms": d * 0.80,  "latency_ms": d * 0.18, "from": "secondary", "to": "primary",   "label": "ACK ✓",                        "type": "ack"},
        {"t_ms": d,         "latency_ms": 2,         "from": "primary",   "to": "client",    "label": f"ok ({d:.1f}ms)",               "type": "ok"},
        # READ-AFTER-WRITE: primary read
        {"t_ms": t_rp1,     "latency_ms": 2,         "from": "client",    "to": "primary",   "label": "read — RAW path (primary)",     "type": "read"},
        {"t_ms": t_rp2,     "latency_ms": 2,         "from": "primary",   "to": "client",    "label": f"✓ fresh ({t_rp2-t_rp1:.1f}ms) — always", "type": "fresh_response"},
        # Secondary comparison read
        {"t_ms": t_rs1,     "latency_ms": 2,         "from": "client",    "to": "secondary", "label": "read — secondary (comparison)", "type": "read"},
        {"t_ms": t_rs2,     "latency_ms": 2,         "from": "secondary", "to": "client",
         "label": f"{'✓ fresh' if sec_doc else '⚡ stale'} ({t_rs2-t_rs1:.1f}ms)",
         "type": "fresh_response" if sec_doc else "stale_response"},
    ]

    return {
        "experiment":  "read_after_write",
        "title":       "Read-After-Write Consistency",
        "description": "Kullanıcı yazdığı veriyi primary'den her zaman anında okur. Secondary'den stale gelebilir (özellikle w=1 ile).",
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":         "majority",
            "write_returned_ms":     round(d, 2),
            "primary_read_ms":       round(t_rp2 - t_rp1, 2),
            "primary_fresh":         pri_doc is not None,
            "secondary_fresh":       sec_doc is not None,
            "consistency_model":     "Read-After-Write",
            "consistency_achieved":  pri_doc is not None,
        },
    }


# ── Experiment 4: Monotonic Reads ─────────────────────────────────────────────

def _run_monotonic_reads():
    """Secondary'den okunan version numarası asla geri gidemez.

    w=1 kullanarak lag oluşturur, sonra secondary'den ardışık okumalar yapar.
    Her okuma önceki okumadan küçük bir version dönemez.
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
        reads.append({
            "t_ms":    t_r,
            "version": doc.get("version") if doc else None,
            "status":  doc.get("value", {}).get("status") if doc else "—",
        })
        time.sleep(0.08)

    versions = [r["version"] for r in reads if r["version"] is not None]
    monotonic = all(versions[i] <= versions[i+1] for i in range(len(versions)-1))

    operations.set_write_concern("majority")

    # Build events
    events = []
    labels = ["insert (pending)", "update: in_transit", "update: in_transit", "update: delivered"]
    for i, (t_ms, lbl) in enumerate(zip(write_times, labels)):
        events.append({"t_ms": t_ms,     "latency_ms": 3, "from": "client",  "to": "primary",   "label": lbl,          "type": "write"})
        events.append({"t_ms": t_ms + 2, "latency_ms": 2, "from": "primary", "to": "client",    "label": f"ok v{i+1}", "type": "ok"})
        events.append({"t_ms": t_ms + 3, "latency_ms": 30, "from": "primary", "to": "secondary", "label": f"oplog v{i+1}", "type": "replicate"})

    for r in reads:
        v = r["version"]
        s = r["status"]
        events.append({"t_ms": r["t_ms"],     "latency_ms": 2, "from": "client",    "to": "secondary", "label": "read",               "type": "read"})
        events.append({"t_ms": r["t_ms"] + 3, "latency_ms": 2, "from": "secondary", "to": "client",
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
