"""
Flask dashboard for CENG465 replication demo.

    python app.py
    open http://localhost:5000
"""

from bson import ObjectId
from flask import Flask, jsonify, render_template, request

import db
from operations import insert_item, update_item, delete_item

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    try:
        info = db.get_primary().command("hello")
        sinfo = db.get_secondary().command("hello")
        return jsonify({
            "primary": {"host": "mongo-primary.lan:27017", "writable": info.get("isWritablePrimary", False)},
            "secondary": {"host": "mongo-secondary.lan:27017", "secondary": sinfo.get("secondary", False)},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/items")
def api_items():
    pdb = db.get_primary()
    sdb = db.get_secondary()
    primary_docs = list(pdb["items"].find().sort("last_updated", -1).limit(50))
    sec_map = {str(d["_id"]): d for d in sdb["items"].find()}
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
        })
    return jsonify(result)


@app.route("/api/logs")
def api_logs():
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
        })
    return jsonify(result)


@app.route("/api/insert", methods=["POST"])
def api_insert():
    data = request.json
    key = data.get("key", "item")
    value = data.get("value", {})
    try:
        item_id, delay = insert_item(key, value)
        return jsonify({"ok": True, "item_id": str(item_id), "delay_ms": delay})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/update", methods=["POST"])
def api_update():
    data = request.json
    try:
        item_id = ObjectId(data["item_id"])
        value = data.get("value", {})
        delay = update_item(item_id, value)
        return jsonify({"ok": True, "delay_ms": delay})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/delete", methods=["POST"])
def api_delete():
    data = request.json
    try:
        item_id = ObjectId(data["item_id"])
        delay = delete_item(item_id)
        return jsonify({"ok": True, "delay_ms": delay})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
