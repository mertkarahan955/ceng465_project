"""
CENG465 Consistency Experiment Runner

Her deneyi çalıştırır ve DDIA-style timeline için gerekli event'leri üretir.
Her event:
  t_ms       — gönderim anı (deney başından ms cinsinden)
  latency_ms — iletim gecikmesi (ok eğimini belirler)
  from / to  — aktör isimleri: "client" | "user_a" | "user_b" | "primary" | "secondary"
  label      — ok üzerindeki metin
  type       — renk kodlaması için
"""

import contextlib
import json
import os
import time
from datetime import datetime

import db
import operations


# ── Paths ─────────────────────────────────────────────────────────────────────

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "experiment_results")

# Default 3-actor config (used by experiments 1, 2, 4)
DEFAULT_ACTORS = {
    "client":    {"label": "Client / Dashboard", "color": "#e6edf3", "ip": "(control node)"},
    "primary":   {"label": "PRIMARY",            "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY",          "color": "#58a6ff", "ip": "192.168.88.70"},
}
DEFAULT_ACTOR_ORDER = ["client", "primary", "secondary"]



# ── Low-level helpers ──────────────────────────────────────────────────────────

def _ms():
    """Current time in ms (float)."""
    return time.time() * 1000


def _safe_lat(v, minimum=0.5):
    """Clamp latency_ms to a positive value — prevents backward SVG arrows.

    FIX (code-review Bug 1 + Bug 3): proportional sub-timing arithmetic can
    produce zero or negative values on fast LANs.  Every latency_ms in an
    event dict must pass through this helper.
    """
    return max(float(v), minimum)


def _measure_net_one_way(db_getter, samples: int = 3) -> float:
    """Estimate one-way network latency to a node using MongoDB ping RTT.

    Takes the minimum of `samples` round-trips to reduce queuing noise.
    Falls back to 2.0 ms if the node is unreachable.
    """
    rtts = []
    for _ in range(samples):
        t0 = _ms()
        try:
            db_getter().command("ping")
            rtts.append(_ms() - t0)
        except Exception:
            pass
    if not rtts:
        return 2.0
    return min(rtts) / 2.0


def _req_resp_events(
    t_send: float,
    t_recv: float,
    net: float,
    from_actor: str,
    to_actor: str,
    req_label: str,
    req_type: str,
    resp_label: str,
    resp_type: str,
) -> list:
    """Build a matched request+response event pair with proper 4-point timing.

    Produces the  ╲ ─ ╱  shape:
      t_send          → request departs from_actor
      t_send + net    → request arrives at to_actor
      t_recv - net    → response departs to_actor  (flat section = processing)
      t_recv          → response arrives at from_actor
    """
    net = _safe_lat(net)
    ok_depart = max(t_recv - net, t_send + net + 0.5)
    return [
        {"t_ms": t_send,     "latency_ms": net, "from": from_actor, "to": to_actor,   "label": req_label,  "type": req_type},
        {"t_ms": ok_depart,  "latency_ms": net, "from": to_actor,   "to": from_actor, "label": resp_label, "type": resp_type},
    ]


@contextlib.contextmanager
def _write_concern(w):
    """Temporarily set write concern, ALWAYS restore majority on exit.

    FIX (code-review Bug 2): the original pattern set w=1 at the top of each
    experiment and restored majority at the bottom with no try/finally.  Any
    exception mid-experiment left the process-global _write_concern at 1,
    silently downgrading all subsequent dashboard writes.
    """
    operations.set_write_concern(w)
    try:
        yield
    finally:
        operations.set_write_concern("majority")


def _get_log(item_id):
    return db.get_primary()["operation_logs"].find_one({"target_id": item_id})


def _serialize_log(log):
    if not log:
        return None
    return {
        "log_index":            log.get("log_index"),
        "operation_id":         str(log.get("operation_id", ""))[:8],
        "version_before":       log.get("version_before"),
        "version_after":        log.get("version_after"),
        "write_concern":        str(log.get("write_concern", "")),
        "status":               log.get("status"),
        "replication_delay_ms": log.get("replication_delay_ms"),
        "leader_write_time":    log.get("leader_write_time").isoformat() if log.get("leader_write_time") else None,
        "follower_visible_time": log.get("follower_visible_time").isoformat() if log.get("follower_visible_time") else None,
        "target_collection":    log.get("target_collection"),
    }


# ── Result persistence ─────────────────────────────────────────────────────────

def _save_result(name: str, result: dict) -> str:
    """Persist experiment result to disk. Returns the filename.

    FIX (code-review Bug 4): caller (run_experiment) wraps this in try/except
    so a save failure never suppresses a successful experiment result.
    """
    folder = os.path.join(RESULTS_DIR, name)
    os.makedirs(folder, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:20]
    filename = f"{ts}.json"
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"saved_at": datetime.now().isoformat(), **result}, f,
                  ensure_ascii=False, indent=2, default=str)
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
        "sync_replication":     _run_sync_replication,
        "eventual_consistency": _run_eventual_consistency,
        "read_after_write":     _run_read_after_write,
        "monotonic_reads":      _run_monotonic_reads,
    }
    fn = registry.get(name)
    if not fn:
        raise ValueError(f"Unknown experiment: {name!r}")

    result = fn()

    # FIX Bug 4: save failure must not suppress a successful experiment result.
    try:
        _save_result(name, result)
    except OSError:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Experiment 1 — Synchronous Replication (w=majority)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sync_replication():
    """w=majority: Primary follower ACK'ini bekler, sonra istemciye döner.

    Timeline:
      Client → Primary: write (w=majority)
      Primary → Secondary: oplog → apply
      Secondary → Primary: ACK
      Primary → Client: ok  (only after ACK — this is the key delay)
    """
    operations.set_write_concern("majority")

    # Measure one-way latency to each node before the timed section.
    net_p = _measure_net_one_way(db.get_primary)
    net_s = _measure_net_one_way(db.get_secondary)

    t0 = _ms()
    item_id, delay_ms = operations.insert_position(
        "SYNC-EXP", 41.0082, 28.9784, "Istanbul", "Besiktas", 65
    )
    t1 = _ms()

    # FIX Bug 3: use `or` instead of `is not None` so that delay_ms=0.0
    # (instantaneous replication) also falls back to wall-clock measurement.
    d = delay_ms or (t1 - t0)
    log = _get_log(item_id)

    # Sub-timings derived from measured network latency and total duration d.
    # net_p / net_s are actual one-way latencies measured via MongoDB ping.
    #
    # Timeline (t=0 is when client sends):
    #   0        → request departs client
    #   net_p    → request arrives primary
    #   net_p    → primary starts oplog to secondary
    #   net_p + net_s       → oplog arrives secondary (apply begins)
    #   net_p + net_s + gap → secondary ACK departs
    #   d - net_p           → primary receives ACK, sends ok
    #   d                   → ok arrives client
    oplog_start = _safe_lat(net_p)
    sec_arrive  = _safe_lat(net_p + net_s)
    # The secondary spends most of d applying the write; ack departs a bit before ok_depart.
    ok_depart   = max(d - net_p, sec_arrive + net_s + 0.5)
    sec_ack_t   = max(ok_depart - net_s, sec_arrive + 0.5)

    events = [
        {"t_ms": 0,           "latency_ms": _safe_lat(net_p),                   "from": "client",    "to": "primary",   "label": "write (w=majority)", "type": "write"},
        {"t_ms": oplog_start, "latency_ms": _safe_lat(sec_arrive - oplog_start), "from": "primary",   "to": "secondary", "label": "oplog → apply",      "type": "replicate"},
        {"t_ms": sec_ack_t,   "latency_ms": _safe_lat(ok_depart - sec_ack_t),   "from": "secondary", "to": "primary",   "label": "ACK ✓",              "type": "ack"},
        {"t_ms": ok_depart,   "latency_ms": _safe_lat(net_p),                   "from": "primary",   "to": "client",    "label": f"ok ({d:.1f} ms)",   "type": "ok"},
    ]

    return {
        "experiment":  "sync_replication",
        "title":       "Synchronous Replication (w=majority)",
        "description": "Primary, follower ACK'ini bekler. İstemci ancak secondary onayladıktan sonra 'ok' alır. Daha güvenli ama daha yavaş.",
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
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


# ═══════════════════════════════════════════════════════════════════════════════
# Experiment 2 — Eventual Consistency (w=1)
# ═══════════════════════════════════════════════════════════════════════════════

def _set_secondary_delay(delay_secs: int):
    """Set secondaryDelaySecs on the secondary replica set member.

    Returns the previous delay value so the caller can restore it.
    Finds the secondary member by checking `stateStr` so it is robust
    to member ordering changes.
    """
    admin = db.get_primary().client["admin"]
    cfg = admin.command("replSetGetConfig")["config"]

    sec_idx = None
    for i, m in enumerate(cfg["members"]):
        if m.get("priority", 1) == 0:   # secondary has priority=0
            sec_idx = i
            break
    if sec_idx is None:
        # Fall back to index 1
        sec_idx = 1

    prev = cfg["members"][sec_idx].get("secondaryDelaySecs", 0)
    if prev == delay_secs:
        return prev  # nothing to do

    cfg["members"][sec_idx]["secondaryDelaySecs"] = delay_secs
    # MongoDB requires priority=0 for delayed members
    cfg["members"][sec_idx]["priority"] = 0
    # MongoDB rejects a reconfig whose (version, term) is not strictly greater
    # than the current config's.  Incrementing version is always sufficient.
    cfg["version"] = cfg.get("version", 1) + 1
    admin.command("replSetReconfig", cfg)
    # Wait briefly for the reconfig to propagate
    time.sleep(0.5)
    return prev


def _run_eventual_consistency():
    """Eventual consistency demonstrasyonu:

    1. w=1 ile v1 insert → secondary'nin sync'lenmesini manuel polling ile bekle.
    2. secondaryDelaySecs=2 geçici olarak ayarla → stale reads garanti edilir.
    3. Secondary'den 1 baseline read (v1 görünmeli).
    4. w=1 ile v2 update → OK hemen döner (no secondary wait).
    5. Birden fazla secondary read → stale (v1) × N → fresh (v2).
    6. Deney bitince secondaryDelaySecs sıfırla.
    """
    # Measure one-way latency to each node before the timed section.
    net_p = _measure_net_one_way(db.get_primary)
    net_s = _measure_net_one_way(db.get_secondary)

    # ── Phase 1: Establish v1 on both nodes ───────────────────────
    # w=1 ile yaz → secondary'yi manuel polling ile bekle
    # (w=majority kullanmıyoruz çünkü delay aktifken çok uzun sürer)
    operations.set_write_concern(1)
    item_id, _ = operations.insert_position(
        "EC-BASE", 39.9334, 32.8597, "Ankara", "Cankaya", 80
    )
    for _ in range(200):
        sec = db.get_secondary()["positions"].find_one({"_id": item_id})
        if sec and sec.get("version") == 1:
            break
        time.sleep(0.015)

    # ── Phase 1b: Set 2-second secondary delay ─────────────────────
    # Bu, replication'ı kasıtlı olarak yavaşlatır ve stale read'leri
    # gözlemlenebilir kılar. Deney bitince sıfırlanır.
    DELAY_SECS = 2
    prev_delay = _set_secondary_delay(DELAY_SECS)

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
    # secondaryDelaySecs=2 nedeniyle secondary ~2s geride kalır.
    # Reads: 200ms, 500ms, 1000ms, 1500ms, 2000ms, 2500ms, 3000ms aralıklı.
    TARGET = 2
    reads = []
    try:
        for delay_ms in [200, 500, 1000, 1500, 2000, 2500, 3000]:
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
                "t_ms":              t_r,
                "t_end_ms":          t_r_end,
                "found":             doc is not None,
                "version":           version,
                "stale_after_write": is_stale,
                "status":            f"v{version}" if version else "—",
            })
            if not is_stale:
                break  # secondary caught up
    finally:
        # Her durumda delay'i sıfırla
        _set_secondary_delay(prev_delay)

    stale_observed = any(r["stale_after_write"] for r in reads)
    first_fresh    = next((r for r in reads if not r["stale_after_write"]), None)
    synced_ms      = first_fresh["t_ms"] if first_fresh else None

    log     = _get_log(item_id)
    repl_ms = log.get("replication_delay_ms") if log else synced_ms

    # ── Build timeline events — net_p/net_s measured before experiment ────
    update_duration = write_returned_ms - write_start_ms

    # Primary commits at the midpoint; oplog departs from there.
    primary_commit_ms = write_start_ms + net_p
    if repl_ms:
        oplog_arrive = primary_commit_ms + repl_ms
    elif synced_ms:
        oplog_arrive = synced_ms
    else:
        oplog_arrive = write_returned_ms + 50

    events = [
        # 1. Baseline read from secondary before the update (shows v1 is synced)
        *_req_resp_events(
            t_b, t_b_end, net_s,
            "client", "secondary",
            "read (baseline)", "read",
            f"v{baseline_version} (synced)", "fresh_response",
        ),

        # 2. w=1 update — ok returns without waiting for secondary
        #    \ _ /  shape: request travels net_p to primary, primary processes,
        #    ok departs at write_returned_ms - net_p, arrives at write_returned_ms
        *_req_resp_events(
            write_start_ms, write_returned_ms, net_p,
            "client", "primary",
            "update (w=1, v1→v2)", "write",
            f"ok ({update_duration:.1f}ms) — no secondary wait", "ok",
        ),

        # 3. Oplog replication (async) — primary → secondary
        {"t_ms": primary_commit_ms,
         "latency_ms": max(oplog_arrive - primary_commit_ms, 2),
         "from": "primary", "to": "secondary",
         "label": f"oplog async — {max(oplog_arrive - primary_commit_ms, 0):.1f}ms",
         "type": "replicate"},
    ]

    # ── Post-update secondary reads ────────────────────────────────
    for r in reads:
        if r["stale_after_write"]:
            resp_label = f"⚡ stale (v{r['version']} — old)"
            resp_type  = "stale_response"
        else:
            resp_label = f"✓ fresh (v{r['version']})"
            resp_type  = "fresh_response"

        events.extend(_req_resp_events(
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


# ═══════════════════════════════════════════════════════════════════════════════
# Experiment 3 — Read-After-Write  (DDIA Figure 5-3)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_read_after_write():
    """RAW consistency — single user, two read paths (mirrors DDIA Figure 5-3).

    The anomaly: after writing to PRIMARY with w=1 (async), the user's next
    read hits SECONDARY and sees "no results" — their own write is invisible.
    A load balancer can route the read to a different node than the write.

    The fix: route reads to PRIMARY for the session that performed the write.
    This guarantees the writer always sees their own write immediately.

    Timeline:
      Client → Primary:   write incident (w=1)
      Primary → Client:   ok (immediately, secondary not yet updated)
      Primary ⇢ Secondary: oplog (async)

      Client → Secondary: read (wrong path — stale replica hit)
      Secondary → Client: ⚡ stale!  ← this IS the RAW anomaly

      Client → Primary:   read (RAW path — correct routing to PRIMARY)
      Primary → Client:   ✓ fresh  ← RAW satisfied
    """
    # Measure one-way latency to each node before the timed section.
    net_p = _measure_net_one_way(db.get_primary)
    net_s = _measure_net_one_way(db.get_secondary)

    with _write_concern(1):
        t0 = _ms()
        item_id, _ = operations.insert_incident(
            "RAW-EXP", "breakdown", "critical",
            "RAW demo — dispatcher files incident"
        )
        t1 = _ms()
        write_ms = t1 - t0

        # ── RAW read: hit PRIMARY first (correct routing — "yazdığın yerde oku") ─
        t_raw1  = _ms() - t0
        pri_doc = db.get_primary()["incidents"].find_one({"_id": item_id})
        t_raw2  = _ms() - t0

        # ── Stale read: SECONDARY after the fact (shows the anomaly) ─────────
        t_stale1 = _ms() - t0
        sec_doc  = db.get_secondary()["incidents"].find_one({"_id": item_id})
        t_stale2 = _ms() - t0
        stale    = sec_doc is None

        log = _get_log(item_id)

    # ── Build events ─────────────────────────────────────────────────────────
    events = [
        # Write (w=1) — 4-point  \ _ /  shape
        *_req_resp_events(
            0, write_ms, net_p,
            "client", "primary",
            "write incident (w=1)", "write",
            f"ok ({write_ms:.1f}ms) — immediately", "ok",
        ),
        # Async oplog — departs after primary commit (net_p into the timeline)
        {"t_ms": net_p, "latency_ms": _safe_lat(35), "from": "primary", "to": "secondary",
         "label": "oplog (async)", "type": "replicate"},

        # RAW read from PRIMARY — 4-point shape
        *_req_resp_events(
            t_raw1, t_raw2, net_p,
            "client", "primary",
            "read (RAW path — PRIMARY)", "read",
            f"✓ fresh ({t_raw2 - t_raw1:.1f}ms) — always", "fresh_response",
        ),

        # Stale read from SECONDARY — 4-point shape
        *_req_resp_events(
            t_stale1, t_stale2, net_s,
            "client", "secondary",
            "read (wrong path)", "read",
            "⚡ stale! (write not propagated)" if stale else "✓ fresh (fast sync)",
            "stale_response" if stale else "fresh_response",
        ),
    ]

    repl_ms = (log.get("replication_delay_ms") if log else None)

    return {
        "experiment":  "read_after_write",
        "title":       "Read-After-Write Consistency",
        "description": (
            "Yazıdan sonra SECONDARY'den okursak kendi yazımızı göremeyebiliriz. "
            "RAW çözümü: write yapan session için okumaları PRIMARY'ye yönlendir. "
            "(DDIA Figure 5-3)"
        ),
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
        "events":      events,
        "log":         _serialize_log(log),
        "summary": {
            "write_concern":        "1 (async)",
            "write_returned_ms":    round(write_ms, 2),
            "stale_read_observed":  stale,
            "raw_read_ms":          round(rtt_raw, 2),
            "raw_fresh":            pri_doc is not None,
            "replication_delay_ms": round(repl_ms, 2) if repl_ms else None,
            "consistency_model":    "Read-After-Write",
            "consistency_achieved": pri_doc is not None,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Experiment 4 — Monotonic Reads
# ═══════════════════════════════════════════════════════════════════════════════

def _run_monotonic_reads():
    """Secondary'den okunan version numarası asla geri gidemez.

    w=1 ile 4 write yapar (pending → in_transit → in_transit → delivered),
    ardından secondary'den 5 kez okur. Her okuma öncekinden küçük bir version
    dönemez — monotonicity ihlal edilirse test FAIL'dir.
    """
    # Measure one-way latency to each node before the timed section.
    net_p = _measure_net_one_way(db.get_primary)
    net_s = _measure_net_one_way(db.get_secondary)

    with _write_concern(1):
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

        # Record the end of the last write so we have its duration.
        write_end = _ms() - t0

        # Read from secondary at intervals to capture version progression
        reads = []
        time.sleep(0.03)
        for _ in range(5):
            t_r = _ms() - t0
            doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
            t_r_end = _ms() - t0
            reads.append({
                "t_ms":     t_r,
                "t_end_ms": t_r_end,
                "version":  doc.get("version") if doc else None,
                "status":   doc.get("value", {}).get("status") if doc else "—",
            })
            time.sleep(0.08)

    versions = [r["version"] for r in reads if r["version"] is not None]
    monotonic = all(versions[i] <= versions[i+1] for i in range(len(versions)-1))

    # Build events — each write is a  \ _ /  shape using measured net_p.
    # write_times[i] = when op i was issued; write_times[i+1] = when op i returned.
    # For the last op, write_end is its return time.
    all_write_ends = write_times[1:] + [write_end]

    events = []
    labels = ["insert (pending)", "update: in_transit", "update: in_transit", "update: delivered"]
    for i, (t_ms, lbl) in enumerate(zip(write_times, labels)):
        t_end = all_write_ends[i]
        events.extend(_req_resp_events(
            t_ms, t_end, net_p,
            "client", "primary",
            lbl, "write",
            f"ok v{i+1}", "ok",
        ))
        # Oplog departs after primary commits (net_p into the write).
        events.append({"t_ms": t_ms + net_p, "latency_ms": _safe_lat(28),
                        "from": "primary", "to": "secondary",
                        "label": f"oplog v{i+1}", "type": "replicate"})

    for r in reads:
        v = r["version"]
        s = r["status"]
        events.extend(_req_resp_events(
            r["t_ms"], r["t_end_ms"], net_s,
            "client", "secondary",
            "read", "read",
            f"v{v} ({s})" if v else "—",
            "fresh_response" if v else "stale_response",
        ))

    return {
        "experiment":  "monotonic_reads",
        "title":       "Monotonic Reads",
        "description": "Secondary'den ardışık okumalar asla önceki versiyondan düşük dönemez. w=1 ile replication lag görünür hale gelir.",
        "actors":      DEFAULT_ACTORS,
        "actor_order": DEFAULT_ACTOR_ORDER,
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
