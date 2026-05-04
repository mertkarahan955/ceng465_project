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
| Primary / Leader | `mongo-primary.lan` | `192.168.88.146` |
| Secondary / Follower | `mongo-secondary.lan` | `192.168.88.105` |
| Replica set name | `rs0` | port `27017` |

Both machines must have these entries in `/etc/hosts`:

```
192.168.88.146  mongo-primary.lan
192.168.88.105  mongo-secondary.lan
```

---

## 1. MongoDB Setup

### Install MongoDB (both machines)

```bash
brew tap mongodb/brew
brew install mongodb-community
```

### Configure Primary (`192.168.88.146`)

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
  bindIp: 127.0.0.1,192.168.88.146

replication:
  replSetName: rs0
```

> **Important:** Write the config manually in nano — do NOT copy-paste from a browser or chat.
> Copy-paste can introduce non-breaking spaces (`0xC2A0`) which cause `mongod` to fail with
> `"Unrecognized option: systemLog"`.

### Configure Secondary (`192.168.88.105`)

Same as above but with the secondary IP:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.105
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
    { _id: 0, host: "mongo-primary.lan:27017", priority: 2 },
    { _id: 1, host: "mongo-secondary.lan:27017", priority: 1 }
  ]
})
```

Verify:

```js
rs.status().members.forEach(m => print(m.name, m.stateStr))
// mongo-primary.lan:27017   PRIMARY
// mongo-secondary.lan:27017 SECONDARY
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
# → http://192.168.88.146:5001  (accessible from secondary machine too)
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

### `items` collection

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | human-readable item name |
| `value` | object | application payload |
| `version` | int | increments on every write |
| `leader_term` | int | Raft-inspired term number |
| `last_log_index` | int | global operation ordering index |
| `last_operation_id` | string | UUID unique per write |
| `last_updated` | date | timestamp of last write on leader |
| `deleted` | bool | soft-delete flag |

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
