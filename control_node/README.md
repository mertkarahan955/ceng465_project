# CENG465 — Replication Control Node

Single-Leader Replication experiment driver for MongoDB Replica Set (rs0).

## Prerequisites

- Python 3.11+
- MongoDB installed and running on both machines (see setup below)
- `/etc/hosts` entries on both machines:

```
192.168.88.146  mongo-primary.lan
192.168.88.105  mongo-secondary.lan
```

---

## Quick Start

```bash
# 1. Install dependencies
make install

# 2. Check replica set status
make status

# 3. Start the web dashboard
make dashboard
# → open http://localhost:5001
```

---

## Commands

| Command | Description |
|---------|-------------|
| `make install` | Create virtualenv and install dependencies |
| `make dashboard` | Start web dashboard at http://localhost:5001 |
| `make demo` | Run insert/update/delete demo + save chart PNG |
| `make test` | Run full test suite (13 assertions) |
| `make benchmark` | Run 1000-op benchmark, save chart PNG |
| `make trace` | Live operation log trace (run in second terminal) |
| `make status` | Show PRIMARY / SECONDARY replica set state |
| `make clean` | Drop all collections (fresh start) |

---

## Two-Machine Benchmark

Run simultaneously on both machines:

**Primary machine:**
```bash
make benchmark-writer RUN_ID=run1 OPS=1000
# waits for Enter — start reader first, then press Enter
```

**Secondary machine (at the same time):**
```bash
make benchmark-reader RUN_ID=run1
```

This measures true cross-machine replication delay as seen from the secondary.

---

## Web Dashboard

```bash
make dashboard
```

Open **http://localhost:5001** (or http://192.168.88.146:5001 from secondary machine).

**What you can do:**
- **Insert** — add a new item with a key and JSON value
- **Update** — pick an existing item and change its value
- **Delete** — soft-delete an item (marks `deleted: true`, keeps the record)

**What you can see:**
- Primary vs Secondary version comparison per item
- Replication delay per operation (ms)
- Operation log with `leader_write_time` and `follower_visible_time`
- Secondary health state and pending log backlog
- Live delay chart by operation type (insert / update / delete)
- p50 / p95 / timeout stats in the header

### Write Concern Modes

The dashboard supports two write concern modes:

| Mode | Behavior |
|------|----------|
| `w=majority` | The write waits for replica-set acknowledgement. The dashboard then polls the secondary and records `visible_on_follower` or `timeout`. |
| `w=1` | The write is acknowledged by the leader only. The request returns immediately after the primary write and operation log insert. The log status starts as `pending_follower`. |

For `w=1`, the dashboard starts a background reconciler. If the secondary is offline, writes still complete on the primary and appear in `operation_logs` as `pending_follower`. When the secondary comes back and catches up from MongoDB's oplog, the reconciler scans pending logs, detects that the expected version is visible on the secondary, fills:

- `follower_visible_time`
- `replication_delay_ms`
- `status: visible_on_follower`

At that point, the item table's `sync` field changes to a checkmark because the primary and secondary versions match.

Important MongoDB deployment note: with only two voting replica-set members, if the secondary `mongod` process is fully stopped, the primary can lose majority and step down. In that state even `w=1` cannot write because MongoDB no longer has a writable primary. For the "secondary down, leader still accepts w=1 writes" demo, use one of these replica-set layouts:

- Primary voting member + secondary non-voting member (`votes: 0`, `priority: 0`), or
- Primary + secondary + a small arbiter as the third voting member.

If the dashboard shows `ReplicaSetNoPrimary` after disconnecting the secondary, reconnect the secondary first, wait until `db.hello()` reports `isWritablePrimary: true`, then run:

```js
cfg = rs.conf()
cfg.members[1].priority = 0
cfg.members[1].votes = 0
rs.reconfig(cfg)
```

The code path here assumes the leader remains writable while the secondary/read replica is unavailable.

The control node also runs a secondary healthcheck loop. It pings the secondary once per second, updates the dashboard health panel, and drains old `pending_follower` / `timeout` log entries when the secondary becomes reachable again. This makes recovery observable from the dashboard:

- `DOWN`: secondary is not reachable.
- `CATCHING`: secondary is reachable, but pending logs still need confirmation.
- `UP`: secondary is reachable and no pending logs remain.

Secondary health and secondary-version reads use a direct connection to `mongo-secondary.lan:27017`, not a replica-set-routed connection. This is intentional: if the secondary is offline, the dashboard must show `DOWN` instead of accidentally routing a ping/read to the primary.

If item rows show secondary versions but log rows remain `pending_follower`, restart the dashboard so the direct secondary connection settings are reloaded:

```bash
# stop the current dashboard with Ctrl+C, then:
make dashboard
```

The `/api/status`, `/api/items`, and `/api/logs` endpoints all trigger bounded reconciliation checks, so once the secondary is reachable again the pending log statuses should converge to `visible_on_follower`.

### Manual Async Catch-Up Demo

1. Start the dashboard on the control/primary machine:

   ```bash
   make dashboard
   ```

2. In the dashboard, switch write concern to `w=1`.
3. Stop or disconnect the secondary/read replica.
4. Insert or update an item.
5. Expected immediate result:
   - the HTTP request completes,
   - the item is visible on the primary,
   - the item table shows no secondary version yet,
   - the log status is `pending_follower`.
6. Start or reconnect the secondary.
7. Wait for the dashboard refresh/background reconciler.
8. Expected catch-up result:
   - `operation_logs.status` becomes `visible_on_follower`,
   - `follower_visible_time` and `replication_delay_ms` are filled,
   - the item table `sync` column changes to a checkmark.

---

## Live Trace (two terminals)

**Terminal 1 — run operations:**
```bash
make demo
# or: make benchmark
# or: use the dashboard
```

**Terminal 2 — watch logs live:**
```bash
make trace
```

On the secondary machine:
```bash
make trace-secondary
```

---

## File Structure

```
control_node/
├── config.py          — MongoDB hosts, ports, timeouts
├── db.py              — primary and secondary connections
├── operations.py      — insert / update / delete + replication logging
├── app.py             — Flask web dashboard server
├── templates/
│   └── index.html     — dashboard UI
├── visualize.py       — rich terminal demo + matplotlib chart
├── test_replication.py — full test suite (13 tests)
├── trace.py           — live operation log watcher
├── benchmark.py       — 1000-op benchmark (single + two-machine)
├── requirements.txt
└── Makefile
```

---

## Schema

### `items` collection
| Field | Type | Description |
|-------|------|-------------|
| `key` | string | item name |
| `value` | object | application data |
| `version` | int | increments on every write |
| `leader_term` | int | Raft-inspired term number |
| `last_log_index` | int | operation ordering index |
| `last_operation_id` | string | UUID of the write |
| `last_updated` | date | timestamp of last write |
| `deleted` | bool | soft delete flag |

### `operation_logs` collection
| Field | Type | Description |
|-------|------|-------------|
| `operation_type` | string | insert / update / delete |
| `log_index` | int | global operation order |
| `leader_write_time` | date | when primary confirmed the write |
| `follower_visible_time` | date | when secondary returned the updated version |
| `replication_delay_ms` | float | follower_visible_time − leader_write_time |
| `status` | string | pending_follower / visible_on_follower / timeout |
| `version_before` | int | version before the operation |
| `version_after` | int | version after the operation |
