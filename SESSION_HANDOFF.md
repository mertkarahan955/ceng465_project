# CENG465 Session Handoff

Last updated: 2026-05-04 23:45

## Current Project State

The project is a MongoDB single-leader replication demo with a Python/Flask control node and dashboard.

Current working architecture:

```text
Control Node / Dashboard
  runs on primary-side Mac
  Flask port: 5001

MongoDB Primary / Leader
  host: mongo-primary.lan:27017
  IP: 192.168.88.146
  votes: 1
  priority: 2

MongoDB Secondary / Read Replica
  host: mongo-secondary.lan:27017
  IP: 192.168.88.105
  votes: 0
  priority: 0
```

Replica set name: `rs0`

The secondary was intentionally made non-voting so the primary remains writable when the secondary is disconnected. This is necessary for the `w=1` async recovery demo.

## MongoDB Replica Set Config

The current replica set config should look like:

```js
[
  { host: "mongo-primary.lan:27017", priority: 2, votes: 1 },
  { host: "mongo-secondary.lan:27017", priority: 0, votes: 0 }
]
```

Command used:

```js
cfg = rs.conf()
cfg.members[1].priority = 0
cfg.members[1].votes = 0
rs.reconfig(cfg)
```

Important lesson:

- With two voting members, disconnecting the secondary causes `ReplicaSetNoPrimary`.
- `w=1` still needs a writable primary.
- Making the secondary non-voting keeps the primary writable while the read replica is unavailable.

## Write Concern Semantics

Dashboard has two modes:

### `w=majority`

Project semantics:

- Secondary must be reachable before the write.
- If secondary is down, the operation is rejected before mutating primary.
- Expected UI/API error:

```text
w=majority requires the secondary to be reachable; write rejected before primary mutation
```

This means:

- no primary write,
- no pending log,
- no automatic replay when secondary returns.

### `w=1`

Project semantics:

- Primary write succeeds even if secondary is down.
- `operation_logs` receives a `pending_follower` record.
- Dashboard request returns quickly as "queued on leader".
- When secondary returns, MongoDB catches up via oplog.
- Control node reconciles pending logs and marks them `visible_on_follower`.

Expected recovery:

```text
pending_follower -> visible_on_follower
follower_visible_time filled
replication_delay_ms filled
sync column becomes checkmark
```

## Dashboard Behavior

Dashboard URL:

```text
http://localhost:5001
http://192.168.88.146:5001
```

Run:

```bash
cd control_node
make dashboard
```

Dashboard panels:

- operations: insert/update/delete
- write concern switch: `w=majority` / `w=1`
- replica health:
  - `UP`
  - `DOWN`
  - `CATCHING`
  - pending log count
  - last catch-up count
  - last check time
- item table:
  - primary version
  - secondary version
  - sync checkmark
- operation log table:
  - operation type
  - status
  - delay
  - leader write time
  - follower visible time

## Important UI Semantics

When secondary is down:

- Old records that were already proven synced should remain sync checkmark.
- Only records whose latest operation is still `pending_follower` should show unsynced.
- The current code infers this from `operation_logs` when live secondary reads are unavailable.

## Control Node Implementation Notes

Important files:

```text
control_node/config.py
control_node/db.py
control_node/operations.py
control_node/app.py
control_node/templates/index.html
control_node/trace.py
control_node/README.md
```

Current notable implementation decisions:

- Primary connection uses replica set URI.
- Secondary health/read uses direct connection:

```python
SECONDARY_URI = "mongodb://mongo-secondary.lan:27017/?directConnection=true&readPreference=secondaryPreferred"
```

This avoids accidentally routing secondary health checks to the primary.

- `w=majority` has a pre-write secondary reachability guard.
- `w=1` skips the guard.
- Pending logs are reconciled by:
  - direct secondary read,
  - version comparison,
  - `last_log_index` fallback,
  - `last_operation_id` fallback,
  - item-table sweep when sync is already visible.

## Bugs Found And Fixed

1. Secondary disconnect caused primary down.
   - Cause: both members were voting.
   - Fix: make secondary non-voting.

2. Secondary health showed `UP` while secondary internet was off.
   - Cause: PyMongo replica-set-routed connection could route ping to primary.
   - Fix: secondary reads/health use `directConnection=true`.

3. Data synced but logs stayed `pending_follower`.
   - Cause: reconciliation was too strict.
   - Fix: added ObjectId normalization, version/log-index/operation-id fallbacks, and item-table sweep.

4. `/api/items` crashed with timezone error.
   - Cause: subtracting offset-naive MongoDB timestamp from offset-aware UTC timestamp.
   - Fix: normalize old timestamps to UTC-aware before computing delay.

5. Log panel appeared empty.
   - Cause: frontend silently swallowed `/api/logs` errors.
   - Fix: render explicit error/no-log messages.

6. `w=majority` allowed primary write while secondary was down.
   - Cause: secondary was non-voting, so MongoDB majority could be satisfied by primary alone.
   - Fix: control node rejects `w=majority` writes unless secondary direct ping succeeds.

## Current Manual Test Matrix

### Test A: `w=majority`, secondary up

Expected:

- insert/update/delete succeeds,
- status becomes `visible_on_follower`,
- sync checkmark appears.

### Test B: `w=majority`, secondary down

Expected:

- operation rejected,
- primary data does not change,
- no pending log is created.

### Test C: `w=1`, secondary down

Expected:

- operation succeeds on primary,
- log status is `pending_follower`,
- new item/latest update shows unsynced,
- old already-synced rows remain synced.

### Test D: `w=1`, secondary returns

Expected:

- secondary catches up from MongoDB oplog,
- control node marks pending logs `visible_on_follower`,
- `replication_delay_ms` becomes filled,
- sync checkmark appears.

## Presentation State

Presentation assets:

```text
presentation/first.png
presentation/insert.png
presentation/update.png
presentation/delete.png
presentation/dashboard_async.png
```

`dashboard_async.png` is the latest screenshot showing the modern dashboard with `w=1`, health panel, pending logs, and async recovery context.

Important: this machine currently has no LaTeX compiler available:

```text
pdflatex not found
xelatex not found
lualatex not found
tectonic not found
```

So `presentation/main.tex` can be edited here, but `presentation/main.pdf` must be regenerated on a machine with LaTeX or via Overleaf.

## Last Known Git State

At handoff time:

```text
main...origin/main
modified:
  control_node/README.md
  control_node/app.py
  control_node/operations.py
```

There are also modified `__pycache__` files generated by local Python runs. These should normally not be committed unless the repo intentionally tracks them.

## Recommended Next Session Steps

1. Confirm the dashboard still passes the four manual tests above.
2. Clean or ignore generated `__pycache__` changes.
3. Finish/update `presentation/main.tex` with the final demo story:
   - two macOS machines,
   - MongoDB replica set,
   - secondary non-voting read replica,
   - `w=majority` reject behavior,
   - `w=1` async pending/catch-up behavior,
   - dashboard healthcheck and sync evidence.
4. Rebuild `presentation/main.pdf` on Overleaf or a LaTeX-enabled machine.
5. Commit the final code/docs/presentation changes.
