"""
Flask dashboard for CENG465 replication demo.

    python app.py
    open http://localhost:5001
"""

from bson import ObjectId
from flask import Flask, jsonify, render_template, request
from pymongo.errors import PyMongoError

import db
import operations
from operations import insert_item, update_item, delete_item
import experiment_runner

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jsonable_health(health):
    result = {}
    for key, value in health.items():
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def _serialize_list(docs):
    """Convert a list of MongoDB documents to JSON-serialisable dicts."""
    out = []
    for doc in docs:
        if doc is None:
            continue
        out.append(operations._serialize_doc(doc))
    return out


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    primary_status = {"host": "mongo-primary.lan:27017", "writable": False, "reachable": False}
    secondary_status = {"host": "mongo-secondary.lan:27017", "secondary": False, "reachable": False}
    try:
        health = operations.refresh_secondary_health_once()
    except Exception:
        health = operations.get_secondary_health()

    try:
        info = db.get_primary().command("hello")
        primary_status.update({
            "writable": info.get("isWritablePrimary", False),
            "reachable": True,
        })
    except Exception as e:
        primary_status["error"] = str(e)

    secondary_status.update({
        "secondary": bool(health.get("reachable")),
        "reachable": bool(health.get("reachable")),
    })
    if health.get("last_error"):
        secondary_status["error"] = health["last_error"]

    status_code = 200 if primary_status["reachable"] else 500
    return jsonify({
        "primary": primary_status,
        "secondary": secondary_status,
        "secondary_health": _jsonable_health(health),
    }), status_code


@app.route("/api/items")
def api_items():
    """Return the latest 50 documents from the requested collection.

    Query param: ?collection=vehicles  (default: vehicles)
    Supported: vehicles, drivers, depots, shipments, positions, incidents, items
    """
    collection = request.args.get("collection", "vehicles")
    pdb = db.get_primary()
    primary_docs = list(pdb[collection].find().sort("last_updated", -1).limit(50))

    op_ids = [d.get("last_operation_id") for d in primary_docs if d.get("last_operation_id")]
    log_indexes = [d.get("last_log_index") for d in primary_docs if d.get("last_log_index") is not None]
    logs_by_op = {}
    logs_by_index = {}
    if op_ids:
        for log in pdb["operation_logs"].find({"operation_id": {"$in": op_ids}}):
            logs_by_op[log.get("operation_id")] = log
    if log_indexes:
        for log in pdb["operation_logs"].find({"log_index": {"$in": log_indexes}}):
            logs_by_index[(log.get("log_index"), str(log.get("target_id")))] = log

    try:
        sdb = db.get_secondary()
        sec_map = {str(d["_id"]): d for d in sdb[collection].find()}
        secondary_reachable = True
        operations.mark_secondary_reachable()
    except PyMongoError:
        sec_map = {}
        secondary_reachable = False

    result = []
    for doc in primary_docs:
        oid = str(doc["_id"])
        sec = sec_map.get(oid)
        log = (
            logs_by_op.get(doc.get("last_operation_id"))
            or logs_by_index.get((doc.get("last_log_index"), oid))
        )
        secondary_version = sec.get("version") if sec else None
        synced = sec.get("version") == doc.get("version") if sec else False

        if not secondary_reachable and log and log.get("status") == "visible_on_follower":
            synced = True
            secondary_version = log.get("version_after", doc.get("version"))

        if synced:
            try:
                operations.mark_pending_logs_visible_for_item(
                    doc["_id"],
                    sec.get("version") if sec else None,
                    sec.get("last_log_index") if sec else None,
                )
            except Exception:
                pass

        result.append({
            "id": oid,
            "key": doc.get("key", ""),
            "value": doc.get("value", {}),
            "version": doc.get("version", 0),
            "deleted": doc.get("deleted", False),
            "last_updated": doc.get("last_updated").isoformat() if doc.get("last_updated") else "",
            "operation_id": doc.get("last_operation_id", "")[:8],
            "log_index": doc.get("last_log_index"),
            "leader_term": doc.get("leader_term"),
            "secondary_version": secondary_version,
            "synced": synced,
            "secondary_reachable": secondary_reachable,
            "collection": collection,
        })
    return jsonify(result)


@app.route("/api/logs")
def api_logs():
    reconcile_error = None
    try:
        operations.drain_pending_logs()
        operations.sweep_synced_items_from_secondary()
    except Exception as e:
        reconcile_error = str(e)

    pdb = db.get_primary()
    logs = list(pdb["operation_logs"].find().sort("log_index", -1).limit(200))
    result = []
    for log in logs:
        result.append({
            "log_index": log.get("log_index"),
            "operation_id": log.get("operation_id", "")[:8],
            "operation_type": log.get("operation_type"),
            "target_collection": log.get("target_collection", "items"),
            "status": log.get("status"),
            "replication_delay_ms": log.get("replication_delay_ms"),
            "version_before": log.get("version_before"),
            "version_after": log.get("version_after"),
            "leader_write_time": log.get("leader_write_time").isoformat() if log.get("leader_write_time") else "",
            "follower_visible_time": log.get("follower_visible_time").isoformat() if log.get("follower_visible_time") else None,
            "write_concern": str(log.get("write_concern", "")),
            "reconcile_error": reconcile_error,
        })
    return jsonify(result)


@app.route("/api/write-concern", methods=["GET", "POST"])
def api_write_concern():
    if request.method == "POST":
        w = request.json.get("w", "majority")
        if w not in ("majority", "1", 1):
            return jsonify({"ok": False, "error": "w must be 'majority' or '1'"}), 400
        operations.set_write_concern(w)
    return jsonify({"ok": True, "write_concern": str(operations.get_write_concern())})


@app.route("/api/insert", methods=["POST"])
def api_insert():
    data = request.json
    key = data.get("key", "item")
    value = data.get("value", {})
    collection = data.get("collection", "items")
    try:
        item_id, delay = insert_item(key, value, collection=collection)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "item_id": str(item_id), "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/update", methods=["POST"])
def api_update():
    data = request.json
    collection = data.get("collection", "items")
    try:
        item_id = ObjectId(data["item_id"])
        value = data.get("value", {})
        delay = update_item(item_id, value, collection=collection)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/delete", methods=["POST"])
def api_delete():
    data = request.json
    collection = data.get("collection", "items")
    try:
        item_id = ObjectId(data["item_id"])
        delay = delete_item(item_id, collection=collection)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# Fleet access pattern endpoints (consistency experiment read paths)
# ---------------------------------------------------------------------------

@app.route("/api/fleet/overview")
def api_fleet_overview():
    """Eventual Consistency demo — reads fleet overview from SECONDARY."""
    try:
        result = operations.get_fleet_overview(limit=20)
        out = []
        for row in result:
            out.append({
                "vehicle": operations._serialize_doc(row["vehicle"]),
                "latest_position": operations._serialize_doc(row["latest_position"]),
            })
        return jsonify({"ok": True, "source": "secondary", "data": out})
    except PyMongoError as e:
        return jsonify({"ok": False, "source": "secondary", "error": str(e), "data": []}), 200


@app.route("/api/fleet/vehicle-history/<vehicle_id>")
def api_vehicle_history(vehicle_id):
    """Monotonic Reads demo — position history from SECONDARY."""
    try:
        docs = operations.get_vehicle_history(vehicle_id, limit=10)
        return jsonify({"ok": True, "source": "secondary", "vehicle_id": vehicle_id,
                        "data": _serialize_list(docs)})
    except PyMongoError as e:
        return jsonify({"ok": False, "source": "secondary", "error": str(e), "data": []}), 200


@app.route("/api/fleet/current-position/<vehicle_id>")
def api_current_position(vehicle_id):
    """Read-After-Write demo — latest position from PRIMARY."""
    try:
        doc = operations.get_current_position(vehicle_id)
        return jsonify({"ok": True, "source": "primary", "vehicle_id": vehicle_id,
                        "data": operations._serialize_doc(doc)})
    except PyMongoError as e:
        return jsonify({"ok": False, "source": "primary", "error": str(e), "data": None}), 200


@app.route("/api/fleet/active-shipments")
def api_active_shipments():
    """Active shipments from SECONDARY (eventual consistency)."""
    try:
        docs = operations.get_active_shipments(limit=20)
        return jsonify({"ok": True, "source": "secondary", "data": _serialize_list(docs)})
    except PyMongoError as e:
        return jsonify({"ok": False, "source": "secondary", "error": str(e), "data": []}), 200


@app.route("/api/fleet/open-incidents")
def api_open_incidents():
    """Open incidents from PRIMARY (read-after-write, safety-critical)."""
    try:
        docs = operations.get_open_incidents(limit=20)
        return jsonify({"ok": True, "source": "primary", "data": _serialize_list(docs)})
    except PyMongoError as e:
        return jsonify({"ok": False, "source": "primary", "error": str(e), "data": []}), 200


# ---------------------------------------------------------------------------
# Consistency Experiment Runner
# ---------------------------------------------------------------------------

@app.route("/experiments")
def experiments_page():
    """Dedicated experiment runner with DDIA-style timeline visualization."""
    return render_template("experiments.html")


@app.route("/api/experiment/run", methods=["POST"])
def api_run_experiment():
    """Run a named consistency experiment and return timeline event data.

    Body: { "experiment": "sync_replication" | "eventual_consistency"
                          | "read_after_write" | "monotonic_reads" }
    """
    name = (request.json or {}).get("experiment")
    if not name:
        return jsonify({"ok": False, "error": "Missing 'experiment' field"}), 400
    try:
        result = experiment_runner.run_experiment(name)
        return jsonify({"ok": True, "result": result})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/experiment/results")
def api_experiment_results_all():
    """List saved results for all experiment types."""
    names = ["sync_replication", "eventual_consistency", "read_after_write", "monotonic_reads", "concurrent_writes"]
    data = {n: experiment_runner.list_results(n) for n in names}
    return jsonify({"ok": True, "results": data})


@app.route("/api/experiment/results/<name>")
def api_experiment_results(name):
    """List saved results for a specific experiment."""
    try:
        results = experiment_runner.list_results(name)
        return jsonify({"ok": True, "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/experiment/results/<name>/<filename>")
def api_experiment_result_file(name, filename):
    """Load a specific saved experiment result."""
    if ".." in filename or "/" in filename:
        return jsonify({"ok": False, "error": "Invalid filename"}), 400
    try:
        data = experiment_runner.load_result(name, filename)
        return jsonify({"ok": True, "result": data})
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "Not found"}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db.ensure_indexes()
    operations.start_reconciler()
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
