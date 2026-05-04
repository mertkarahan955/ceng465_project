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
- Live delay chart by operation type (insert / update / delete)
- p50 / p95 / timeout stats in the header

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
| `status` | string | visible_on_follower / timeout |
| `version_before` | int | version before the operation |
| `version_after` | int | version after the operation |
