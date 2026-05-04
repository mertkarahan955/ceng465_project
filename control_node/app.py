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

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def _jsonable_health(health):
    result = {}
    for key, value in health.items():
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result

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
    pdb = db.get_primary()
    primary_docs = list(pdb["items"].find().sort("last_updated", -1).limit(50))
    try:
        sdb = db.get_secondary()
        sec_map = {str(d["_id"]): d for d in sdb["items"].find()}
        secondary_reachable = True
        operations.mark_secondary_reachable()
    except PyMongoError:
        sec_map = {}
        secondary_reachable = False

    result = []
    for doc in primary_docs:
        oid = str(doc["_id"])
        sec = sec_map.get(oid)
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
            "secondary_version": sec.get("version") if sec else None,
            "synced": sec.get("version") == doc.get("version") if sec else False,
            "secondary_reachable": secondary_reachable,
        })
    return jsonify(result)


@app.route("/api/logs")
def api_logs():
    operations.drain_pending_logs()
    pdb = db.get_primary()
    logs = list(pdb["operation_logs"].find().sort("log_index", -1).limit(200))
    result = []
    for log in logs:
        result.append({
            "log_index": log.get("log_index"),
            "operation_id": log.get("operation_id", "")[:8],
            "operation_type": log.get("operation_type"),
            "status": log.get("status"),
            "replication_delay_ms": log.get("replication_delay_ms"),
            "version_before": log.get("version_before"),
            "version_after": log.get("version_after"),
            "leader_write_time": log.get("leader_write_time").isoformat() if log.get("leader_write_time") else "",
            "follower_visible_time": log.get("follower_visible_time").isoformat() if log.get("follower_visible_time") else None,
            "write_concern": str(log.get("write_concern", "")),
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
    try:
        item_id, delay = insert_item(key, value)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "item_id": str(item_id), "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/update", methods=["POST"])
def api_update():
    data = request.json
    try:
        item_id = ObjectId(data["item_id"])
        value = data.get("value", {})
        delay = update_item(item_id, value)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/delete", methods=["POST"])
def api_delete():
    data = request.json
    try:
        item_id = ObjectId(data["item_id"])
        delay = delete_item(item_id)
        status = "pending_follower" if str(operations.get_write_concern()) == "1" and delay is None else (
            "visible_on_follower" if delay is not None else "timeout"
        )
        return jsonify({"ok": True, "delay_ms": delay, "status": status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    operations.start_reconciler()
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
