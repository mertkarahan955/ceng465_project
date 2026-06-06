"""
Shared helpers for consistency experiment modules.

Each experiment module owns only its scenario-specific flow. Timing helpers,
actor metadata, temporary write concern handling, and operation-log
serialization stay here so the experiment files do not duplicate plumbing.
"""

import contextlib
import json
import time
import urllib.request

import db
import operations


# Default 3-actor config (used by experiments 1, 2, 4)
DEFAULT_ACTORS = {
    "client":    {"label": "Client / Dashboard", "color": "#e6edf3", "ip": "(control node)"},
    "primary":   {"label": "PRIMARY",            "color": "#3fb950", "ip": "192.168.88.30"},
    "secondary": {"label": "SECONDARY",          "color": "#58a6ff", "ip": "192.168.88.70"},
}
DEFAULT_ACTOR_ORDER = ["client", "primary", "secondary"]


def ms() -> float:
    """Current time in ms (float)."""
    return time.time() * 1000


def safe_lat(v, minimum: float = 0.5) -> float:
    """Clamp latency_ms to a positive value to prevent backward SVG arrows."""
    return max(float(v), minimum)


# ── Network latency measurement ────────────────────────────────────────────────

def _timed_ping_http(url: str) -> float | None:
    """4-point clock measurement via node_server /timed_ping.

    Protocol (no clock sync required):
        T1 = before HTTP request  (client clock)
        T2 = server received      (server clock, returned in body)
        T3 = server sent          (server clock, returned in body)
        T4 = after HTTP response  (client clock)

        net_one_way = ((T4 - T1) - (T3 - T2)) / 2

    Returns one-way latency in ms, or None if the server is unreachable.
    """
    try:
        T1 = ms()
        with urllib.request.urlopen(f"{url}/timed_ping", timeout=1) as resp:
            T4 = ms()
            data = json.loads(resp.read())
        T2 = data["T2"]
        T3 = data["T3"]
        net = ((T4 - T1) - (T3 - T2)) / 2.0
        return net if net > 0 else None
    except Exception:
        return None


def _timed_ping_mongo(db_getter) -> float | None:
    """One-way latency estimate via MongoDB ping RTT / 2."""
    try:
        t0 = ms()
        db_getter().command("ping")
        return (ms() - t0) / 2.0
    except Exception:
        return None


def measure_net_one_way(
    db_getter,
    node_server_url: str | None = None,
    samples: int = 3,
) -> float:
    """Estimate one-way network latency to a node.

    Strategy (in order):
      1. node_server /timed_ping — 4-point protocol, removes server processing
         time so only pure network latency remains.
      2. MongoDB ping RTT / 2 — less accurate (includes DB processing) but
         works without the node_server running.

    Takes the minimum of `samples` measurements to reduce queuing noise.
    Falls back to 2.0 ms if both methods fail.
    """
    results = []
    for _ in range(samples):
        if node_server_url:
            v = _timed_ping_http(node_server_url)
            if v is not None:
                results.append(v)
                continue
        v = _timed_ping_mongo(db_getter)
        if v is not None:
            results.append(v)

    return min(results) if results else 2.0


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
    req_meta: dict | None = None,
    resp_meta: dict | None = None,
) -> list:
    """Build a matched request+response event pair with proper 4-point timing.

    Produces the  ╲ ─ ╱  shape on the SVG timeline:

      t_send              → request departs from_actor
      t_send + net        → request ARRIVES at to_actor
          ← flat section: processing time at to_actor →
      t_recv - net        → response DEPARTS to_actor
      t_recv              → response arrives at from_actor

    The renderer draws a shaded span line on to_actor's lane between
    (t_send + net) and (t_recv - net), making the processing time visible.
    """
    net = safe_lat(net)
    ok_depart = max(t_recv - net, t_send + net + 0.5)
    return [
        {"t_ms": t_send,    "latency_ms": net, "from": from_actor, "to": to_actor,   "label": req_label,  "type": req_type,  **(req_meta or {})},
        {"t_ms": ok_depart, "latency_ms": net, "from": to_actor,   "to": from_actor, "label": resp_label, "type": resp_type, **(resp_meta or {})},
    ]


# ── Write concern context manager ──────────────────────────────────────────────

@contextlib.contextmanager
def write_concern(w):
    """Temporarily set write concern, always restoring majority on exit."""
    operations.set_write_concern(w)
    try:
        yield
    finally:
        operations.set_write_concern("majority")


# ── Operation log helpers ──────────────────────────────────────────────────────

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
