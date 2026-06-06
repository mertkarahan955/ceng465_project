"""
Shared helpers for consistency experiment modules.

Each experiment module owns only its scenario-specific flow. Timing helpers,
actor metadata, temporary write concern handling, and operation-log
serialization stay here so the experiment files do not duplicate plumbing.
"""

import contextlib
import time

import db
import operations


# Default 3-actor config (used by experiments 1, 2, 4)
DEFAULT_ACTORS = {
    "client":    {"label": "Client / Dashboard", "color": "#e6edf3", "ip": "(control node)"},
    "primary":   {"label": "PRIMARY",            "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY",          "color": "#58a6ff", "ip": "192.168.88.70"},
}
DEFAULT_ACTOR_ORDER = ["client", "primary", "secondary"]


def ms():
    """Current time in ms (float)."""
    return time.time() * 1000


def safe_lat(v, minimum=0.5):
    """Clamp latency_ms to a positive value to prevent backward SVG arrows."""
    return max(float(v), minimum)


def measure_net_one_way(db_getter, samples: int = 3) -> float:
    """Estimate one-way network latency to a node using MongoDB ping RTT."""
    rtts = []
    for _ in range(samples):
        t0 = ms()
        try:
            db_getter().command("ping")
            rtts.append(ms() - t0)
        except Exception:
            pass
    if not rtts:
        return 2.0
    return min(rtts) / 2.0


def req_resp_events(
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
    """Build a matched request+response event pair with proper 4-point timing."""
    net = safe_lat(net)
    ok_depart = max(t_recv - net, t_send + net + 0.5)
    return [
        {"t_ms": t_send,     "latency_ms": net, "from": from_actor, "to": to_actor,   "label": req_label,  "type": req_type},
        {"t_ms": ok_depart,  "latency_ms": net, "from": to_actor,   "to": from_actor, "label": resp_label, "type": resp_type},
    ]


@contextlib.contextmanager
def write_concern(w):
    """Temporarily set write concern, always restoring majority on exit."""
    operations.set_write_concern(w)
    try:
        yield
    finally:
        operations.set_write_concern("majority")


def get_log(item_id):
    return db.get_primary()["operation_logs"].find_one({"target_id": item_id})


def serialize_log(log):
    if not log:
        return None
    return {
        "log_index":             log.get("log_index"),
        "operation_id":          str(log.get("operation_id", ""))[:8],
        "version_before":        log.get("version_before"),
        "version_after":         log.get("version_after"),
        "write_concern":         str(log.get("write_concern", "")),
        "status":                log.get("status"),
        "replication_delay_ms":  log.get("replication_delay_ms"),
        "leader_write_time":     log.get("leader_write_time").isoformat() if log.get("leader_write_time") else None,
        "follower_visible_time": log.get("follower_visible_time").isoformat() if log.get("follower_visible_time") else None,
        "target_collection":     log.get("target_collection"),
    }
