"""
CENG465 Node Timing Server
==========================
Run this on PRIMARY and SECONDARY machines so the control node can measure
real network latency using the 4-point timestamp protocol (NTP-style).

Usage:
    # On primary   (192.168.88.30):
    python node_server.py --role primary

    # On secondary (192.168.88.70):
    python node_server.py --role secondary

The server listens on port 5002 by default.

4-point timing protocol
-----------------------
The control node sends a request and records T1 (sent) and T4 (received).
This server records T2 (received) and T3 (sent), and returns them in the
response body.

    control node → [T1] ──── network ──── [T2] → node_server
    control node ← [T4] ──── network ──── [T3] ← node_server

One-way latency (no clock sync required):
    latency = ((T4 - T1) - (T3 - T2)) / 2

T2 in control-node clock:  T2_c = T1 + latency
T3 in control-node clock:  T3_c = T4 - latency

These values tell experiment_runner.py exactly when the request arrived at
the node and when the response departed — enabling the  \\ _ /  timeline shape.
"""

import argparse
import time

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = Flask(__name__)

# MongoDB connects to local instance; no replica-set routing needed.
_mongo_client = None
_role = "unknown"


def _ms() -> float:
    return time.time() * 1000


def _get_client():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=500)
    return _mongo_client


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/ping")
def ping():
    """Basic liveness check — returns server time immediately."""
    T2 = _ms()
    T3 = _ms()
    return jsonify({"ok": True, "role": _role, "T2": T2, "T3": T3})


@app.route("/timed_ping", methods=["GET", "POST"])
def timed_ping():
    """4-point timing endpoint.

    Records T2 on arrival, does a local MongoDB ping to simulate real
    operation overhead, records T3 just before responding.  The caller
    (experiment_runner.py) wraps this call with its own T1/T4 measurements
    to derive one-way network latency without requiring clock synchronization.
    """
    T2 = _ms()

    # Perform a local MongoDB round-trip so T3-T2 reflects real DB latency
    # (experiment_runner can subtract this to isolate pure network time).
    db_latency_ms = 0.0
    try:
        t_db0 = _ms()
        _get_client()["admin"].command("ping")
        db_latency_ms = _ms() - t_db0
    except PyMongoError:
        db_latency_ms = -1.0

    T3 = _ms()
    return jsonify({
        "ok":             True,
        "role":           _role,
        "T2":             T2,
        "T3":             T3,
        "db_latency_ms":  db_latency_ms,
    })


@app.route("/timed_write", methods=["POST"])
def timed_write():
    """Execute a small write on the local MongoDB and return timing.

    Body (JSON): {"collection": "timing_probe", "doc": {...}}

    Response includes T2 (request received), T_write_start, T_write_end,
    T3 (response sent) — giving experiment_runner full server-side timing.
    """
    T2 = _ms()
    payload = request.get_json(silent=True) or {}
    collection = payload.get("collection", "timing_probe")
    doc = payload.get("doc", {"probe": True})

    result = {"ok": False, "T2": T2, "error": None,
              "T_write_start": None, "T_write_end": None, "inserted_id": None}
    try:
        db = _get_client()["ceng465"]
        T_write_start = _ms()
        ins = db[collection].insert_one(doc)
        T_write_end = _ms()
        result.update({
            "ok":            True,
            "T_write_start": T_write_start,
            "T_write_end":   T_write_end,
            "inserted_id":   str(ins.inserted_id),
        })
    except PyMongoError as exc:
        result["error"] = str(exc)

    result["T3"] = _ms()
    return jsonify(result)


@app.route("/timed_read", methods=["POST"])
def timed_read():
    """Execute a small read on the local MongoDB and return timing.

    Body (JSON): {"collection": "items", "filter": {"key": "..."}}
    """
    T2 = _ms()
    payload = request.get_json(silent=True) or {}
    collection = payload.get("collection", "items")
    filt = payload.get("filter", {})

    result = {"ok": False, "T2": T2, "error": None,
              "T_read_start": None, "T_read_end": None, "found": False}
    try:
        db = _get_client()["ceng465"]
        T_read_start = _ms()
        doc = db[collection].find_one(filt)
        T_read_end = _ms()
        result.update({
            "ok":           True,
            "T_read_start": T_read_start,
            "T_read_end":   T_read_end,
            "found":        doc is not None,
        })
    except PyMongoError as exc:
        result["error"] = str(exc)

    result["T3"] = _ms()
    return jsonify(result)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CENG465 Node Timing Server")
    parser.add_argument("--role", default="unknown", help="primary | secondary")
    parser.add_argument("--port", type=int, default=5002, help="Listen port (default: 5002)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    args = parser.parse_args()

    _role = args.role
    print(f"[node_server] Starting as role={_role!r} on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
