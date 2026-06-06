# CENG465 — Single-Leader Replication Project

**Course:** CENG465 Principles of Data-Intensive Systems  
**Topic:** Data Replication in a Single-Leader Environment  
**Members:** Mert Karahan, Doğukan Topçu  
**Final Due:** 08.06.2026

---

## Project Structure

```
ceng465_project/
├── control_node/          — Python experiment driver + web dashboard
│   ├── config.py          — MongoDB hosts, ports, timeouts
│   ├── db.py              — primary & secondary connections
│   ├── operations.py      — insert / update / delete + replication logging
│   ├── app.py             — Flask web dashboard (port 5001)
│   ├── templates/
│   │   └── index.html     — dashboard UI
│   ├── visualize.py       — rich terminal demo + matplotlib chart
│   ├── test_replication.py — full test suite (13 assertions)
│   ├── trace.py           — live change stream watcher
│   ├── benchmark.py       — 1000-op benchmark
│   ├── requirements.txt
│   └── Makefile
├── presentation/
│   ├── main.tex           — LaTeX Beamer presentation
│   ├── main.pdf           — compiled presentation PDF
│   ├── first.png          — dashboard overview screenshot
│   ├── insert.png         — insert operation screenshot
│   ├── update.png         — update operation screenshot
│   └── delete.png         — delete operation screenshot
├── MONGODB_MACOS_REPLICA_SET_SETUP.md
├── PROJECT_FINDINGS.md
└── SETUP_PROGRESS.md
```

---

## Nodes

| Role | Hostname | IP |
|------|----------|----|
| Primary / Leader | `mongo-primary.lan` | `192.168.88.30` |
| Secondary / Follower | `mongo-secondary.lan` | `192.168.88.70` |
| Replica set name | `rs0` | port `27017` |

Both machines must have these entries in `/etc/hosts`:

```
192.168.88.30  mongo-primary.lan
192.168.88.70  mongo-secondary.lan
```

---

## 1. MongoDB Setup

### Install MongoDB (both machines)

```bash
brew tap mongodb/brew
brew install mongodb-community
```

### Configure Primary (`192.168.88.30`)

```bash
nano /opt/homebrew/etc/mongod.conf
```

```yaml
systemLog:
  destination: file
  path: /opt/homebrew/var/log/mongodb/mongo.log
  logAppend: true

storage:
  dbPath: /opt/homebrew/var/mongodb

net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.30

replication:
  replSetName: rs0
```

> **Important:** Write the config manually in nano — do NOT copy-paste from a browser or chat.
> Copy-paste can introduce non-breaking spaces (`0xC2A0`) which cause `mongod` to fail with
> `"Unrecognized option: systemLog"`.

### Configure Secondary (`192.168.88.70`)

Same as above but with the secondary IP:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.70
```

### Start MongoDB (both machines)

```bash
mkdir -p /opt/homebrew/var/log/mongodb
mkdir -p /opt/homebrew/var/mongodb
brew services start mongodb/brew/mongodb-community
brew services list | grep mongo
```

If the service shows `error`:

```bash
mongod --config /opt/homebrew/etc/mongod.conf
# This prints the actual startup error
```

### Initialize Replica Set (primary machine only)

```bash
mongosh --host mongo-primary.lan --port 27017
```

```js
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo-primary.lan:27017", priority: 2, votes: 1 },
    { _id: 1, host: "mongo-secondary.lan:27017", priority: 1, votes: 1 }
  ]
})
```

Verify:

```js
rs.status().members.forEach(m => print(m.name, m.stateStr))
// mongo-primary.lan:27017   PRIMARY
// mongo-secondary.lan:27017 SECONDARY
```

### Make Secondary Non-Voting (required for w=1 demo)

With two voting members, stopping the secondary causes `ReplicaSetNoPrimary` — the primary
steps down and even `w=1` writes fail. Make the secondary non-voting so the primary stays
writable when the secondary is offline:

```js
cfg = rs.conf()
cfg.members[1].priority = 0
cfg.members[1].votes = 0
rs.reconfig(cfg)
```

Verify:

```js
rs.conf().members.map(m => ({ host: m.host, priority: m.priority, votes: m.votes }))
// [ { host: "mongo-primary.lan:27017",   priority: 2, votes: 1 },
//   { host: "mongo-secondary.lan:27017", priority: 0, votes: 0 } ]
```

---

## 2. Control Node Setup

```bash
cd control_node
make install
```

This creates a Python virtualenv and installs all dependencies from `requirements.txt`:

```
flask==3.1.1
matplotlib==3.10.9
numpy==2.4.4
pymongo==4.17.0
rich==15.0.0
```

---

## 3. All Commands

### Replica set status

```bash
make status
```

### Web dashboard

```bash
make dashboard
# → http://localhost:5001
# → http://192.168.88.30:5001  (accessible from secondary machine too)
```

### Terminal demo (insert/update/delete + PNG chart)

```bash
make demo
```

### Full test suite

```bash
make test
# Runs 13 assertions across 5 test categories
# Expected: 13/13 PASS
```

### Live operation trace

Run in a second terminal while other operations are happening:

```bash
make trace              # connect via primary
make trace-secondary    # connect via secondary (run on secondary machine)
```

### 1000-op benchmark (single machine)

```bash
make benchmark
# default: OPS=1000
make benchmark OPS=500  # custom op count
```

Saves chart PNG to `control_node/benchmark_<run_id>_single.png`.

### 1000-op benchmark (two machines)

**Primary machine:**
```bash
make benchmark-writer RUN_ID=run1 OPS=1000
# Prompts: "Start the reader on the secondary machine now, then press Enter..."
```

**Secondary machine (simultaneously):**
```bash
make benchmark-reader RUN_ID=run1
```

This measures true cross-machine replication delay — the delay as seen from the secondary machine's perspective, not just polling from the primary side.

### Clear all data

```bash
make clean
# Drops: items, operation_logs, benchmark_results, benchmark_manifest
```

---

## 4. Schema

The system uses **6 fleet domain collections** (plus `operation_logs`). All 6 share the same replication metadata envelope; only the `value` field differs per collection.

### Replication envelope (all 6 collections)

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | human-readable identifier (e.g. `VHC-TRK-001`) |
| `value` | object | domain-specific payload (see below) |
| `version` | int | increments on every write |
| `leader_term` | int | Raft-inspired term number |
| `last_log_index` | int | global operation ordering index |
| `last_operation_id` | string | UUID unique per write |
| `last_updated` | date | timestamp of last write on leader |
| `deleted` | bool | soft-delete flag |

### `vehicles` — vehicle registry

| Field | Type | Description |
|-------|------|-------------|
| `vehicle_id` | string | e.g. `TRK-001` |
| `plate` | string | e.g. `34 ABC 001` |
| `vehicle_type` | string | `truck` / `van` / `motorcycle` |
| `max_payload_kg` | int | max cargo weight |
| `manufacture_year` | int | year of manufacture |
| `is_active` | bool | whether vehicle is in service |

Indexes: `(vehicle_type, last_updated)`, `(is_active, last_updated)`

### `drivers` — driver profiles

| Field | Type | Description |
|-------|------|-------------|
| `driver_id` | string | e.g. `DRV-001` |
| `name` | string | full name |
| `license_class` | string | `B` / `C` / `D` / `E` |
| `phone` | string | contact phone |
| `assigned_vehicle_id` | string | vehicle currently assigned to |

Indexes: `(assigned_vehicle_id)`, `(license_class)`

### `depots` — warehouse/hub locations

| Field | Type | Description |
|-------|------|-------------|
| `depot_id` | string | e.g. `DEP-IST` |
| `name` | string | depot name |
| `city` | string | city |
| `lat` / `lng` | float | GPS coordinates |
| `capacity_vehicles` | int | max vehicles at depot |

Index: `(city)`

### `shipments` — cargo shipments

| Field | Type | Description |
|-------|------|-------------|
| `shipment_id` | string | e.g. `SHP-2026-001` |
| `origin_depot` | string | origin depot ID |
| `destination_depot` | string | destination depot ID |
| `customer` | string | customer name |
| `weight_kg` | float | cargo weight |
| `package_count` | int | number of packages |
| `status` | string | `pending` / `in_transit` / `delivered` / `cancelled` |
| `assigned_vehicle_id` | string | assigned vehicle |

Indexes: `(status, last_updated)`, `(assigned_vehicle_id, last_updated)`

### `positions` — live GPS stream (high-frequency writes)

| Field | Type | Description |
|-------|------|-------------|
| `vehicle_id` | string | which vehicle |
| `lat` / `lng` | float | GPS coordinates |
| `city` | string | city name |
| `district` | string | district name |
| `speed_kmh` | float | current speed |

Indexes: `(vehicle_id, last_updated)`, `(city, last_updated)`

### `incidents` — safety events

| Field | Type | Description |
|-------|------|-------------|
| `vehicle_id` | string | vehicle involved |
| `incident_type` | string | `breakdown` / `accident` / `delay` / `fuel_low` |
| `severity` | string | `low` / `medium` / `high` / `critical` |
| `description` | string | free-text description |
| `lat` / `lng` | float | location (optional) |
| `resolved` | bool | whether incident is resolved |

Indexes: `(resolved, severity)`, `(vehicle_id, last_updated)`

### Consistency experiment mapping

| Collection | Read Path | Experiment |
|---|---|---|
| `positions` | SECONDARY | Eventual Consistency |
| `shipments` | SECONDARY (version history) | Monotonic Reads |
| `incidents` | PRIMARY | Read-After-Write |
| `vehicles`, `drivers`, `depots` | SECONDARY | Eventual Consistency |

### `operation_logs` collection

| Field | Type | Description |
|-------|------|-------------|
| `operation_type` | string | insert / update / delete |
| `log_index` | int | global operation order |
| `leader_write_time` | date | when primary confirmed the write |
| `follower_visible_time` | date | when secondary returned the correct version |
| `replication_delay_ms` | float | `follower_visible_time − leader_write_time` |
| `status` | string | `visible_on_follower` / `timeout` |
| `version_before` | int | version before the operation |
| `version_after` | int | version after the operation |

---

## 5. Presentation PDF

### Requirements

Install MacTeX (one-time):

```bash
brew install --cask mactex-no-gui
sudo installer -pkg /opt/homebrew/Caskroom/mactex-no-gui/2026.0324/mactex-20260324.pkg -target /
```

After installation, add to PATH (or add to `~/.zshrc`):

```bash
export PATH="/Library/TeX/texbin:$PATH"
```

Verify:

```bash
pdflatex --version
# pdfTeX 3.141592653-2.6-1.40.29 (TeX Live 2026)
```

### Compile

```bash
cd presentation
export PATH="/Library/TeX/texbin:$PATH"
pdflatex -interaction=nonstopmode main.tex
# Run twice if cross-references changed:
pdflatex -interaction=nonstopmode main.tex
```

Output: `presentation/main.pdf`

### Open

```bash
open presentation/main.pdf
```

### Add screenshots

Place PNG files in `presentation/`:

```
presentation/
├── first.png    # dashboard overview
├── insert.png   # insert operation
├── update.png   # update operation
└── delete.png   # delete operation
```

Then recompile.

---

## 6. Known Issues & Fixes

| Problem | Cause | Fix |
|---------|-------|-----|
| `mongod` fails silently | Duplicate `bindIp` key in `mongod.conf` | Remove duplicate, keep one `bindIp` line |
| `Unrecognized option: systemLog` | Non-breaking spaces in config from copy-paste | Write config manually in nano, not from clipboard |
| `brew services` shows `error` after `sudo` | `sudo brew services` changes file ownership to root | `sudo chown -R $(whoami)` on affected paths, restart without sudo |
| `mongod` not listening on LAN IP | Log/data directories missing | `mkdir -p /opt/homebrew/var/log/mongodb /opt/homebrew/var/mongodb` |
| Flask port 5000 in use | macOS AirPlay Receiver holds port 5000 | Dashboard runs on port **5001** |
| `pdflatex: command not found` after install | MacTeX `.pkg` not executed by Homebrew automatically | Run `sudo installer -pkg ...` manually, then add `/Library/TeX/texbin` to PATH |
| `brew services` shows `error 62` | Data directory contains files from an older MongoDB version (e.g. 7.0 left from a previous install) | `brew services stop mongodb-community && rm -rf /opt/homebrew/var/mongodb/* && brew services start mongodb-community` — then re-initiate the replica set |
