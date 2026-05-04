# Setup Progress

Last updated: 2026-05-04

## Current Goal

Set up a MongoDB replica set on two separate macOS devices connected to the same local network.

The current project scope is:

- Environment Setup and Role Assignment
- Data Schema and Replication Logging

## Current Node Plan

```text
Primary / Leader
  hostname: mongo-primary.lan
  IP: 192.168.88.146

Secondary / Follower
  hostname: mongo-secondary.lan
  IP: 192.168.88.105

Replica set name
  rs0

MongoDB port
  27017
```

## Completed

- Read and summarized the project PDF.
- Decided to use MongoDB Replica Set.
- Decided to use two separate macOS devices on the same local network.
- Decided to use a Python control node later for:
  - insert/update/delete operations,
  - operation logging,
  - secondary polling,
  - replication delay measurement.
- Decided to use Raft-inspired metadata in the schema:
  - `leader_term`,
  - `log_index`,
  - `operation_id`,
  - `version`.
- Added project notes:
  - `PROJECT_FINDINGS.md`
  - `MONGODB_MACOS_REPLICA_SET_SETUP.md`
- Added `/etc/hosts` planning:

  ```text
  192.168.88.146 mongo-primary.lan
  192.168.88.105 mongo-secondary.lan
  ```

- Verified that the primary-side machine can resolve and ping `mongo-secondary.lan`.

  Observed:

  ```text
  mongo-secondary.lan -> 192.168.88.105
  packet loss: 0%
  ```

- Initially downloaded/extracted MongoDB tarball `mongodb-macos-arm64-8.3.1.tgz`.
- Verified tarball binary once with:

  ```bash
  ./bin/mongod --version
  ```

  Result:

  ```text
  db version v8.3.1
  distarch: arm64
  target_arch: arm64
  ```

- Then switched to Homebrew installation path instead of tarball.
- Installed MongoDB through Homebrew.

  Homebrew suggested:

  ```bash
  brew services start mongodb/brew/mongodb-community
  ```

## Current Problem

`brew services list` currently shows:

```text
Name              Status   User File
mongodb-community error  2 bos  ~/Library/LaunchAgents/homebrew.mxcl.mongodb-community.plist
```

The likely cause is an invalid MongoDB config file.

Current primary config was observed as:

```yaml
systemLog:
  destination: file
  path: /opt/homebrew/var/log/mongodb/mongo.log
  logAppend: true
storage:
  dbPath: /opt/homebrew/var/mongodb
net:
  bindIp: 127.0.0.1, ::1
  ipv6: true
  port: 27017
  bindIp: 127.0.0.1,192.168.88.146

replication:
  replSetName: rs0
```

The issue is that `net.bindIp` is defined twice.

MongoDB config should not contain duplicate keys under the same YAML block. This can prevent `mongod` from starting and explains the Homebrew service error.

The MongoDB log file was not present yet:

```text
/opt/homebrew/var/log/mongodb/mongo.log
```

This suggests `mongod` may be failing before it can create/write the normal log file.

## Current Network Issue

Primary-side ping to secondary works.

Secondary-side ping to primary did not work yet.

This may be caused by:

- missing or incorrect `/etc/hosts` entry on the secondary,
- wrong current IP for the primary,
- macOS firewall on the primary,
- network isolation between the devices,
- ICMP ping being blocked even though TCP connections may still work.

After the MongoDB service starts, the more important test is TCP connectivity to MongoDB port `27017`.

## Next Required Manual Fix

On the primary Mac, edit:

```bash
nano /opt/homebrew/etc/mongod.conf
```

Replace the config with:

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

Important:

- Remove `bindIp: 127.0.0.1, ::1`.
- Remove `ipv6: true`.
- Keep only one `bindIp` entry.

Then restart:

```bash
brew services restart mongodb/brew/mongodb-community
brew services list
```

If it still fails:

```bash
tail -n 80 /opt/homebrew/var/log/mongodb/mongo.log
mongod --config /opt/homebrew/etc/mongod.conf
```

The foreground `mongod --config ...` command should print the real startup error if the service still cannot start.

## Next Required Secondary Config

On the secondary Mac, edit:

```bash
nano /opt/homebrew/etc/mongod.conf
```

Use:

```yaml
systemLog:
  destination: file
  path: /opt/homebrew/var/log/mongodb/mongo.log
  logAppend: true

storage:
  dbPath: /opt/homebrew/var/mongodb

net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.105

replication:
  replSetName: rs0
```

Then:

```bash
brew services restart mongodb/brew/mongodb-community
brew services list
```

## Next Network Tests

Run on both Macs:

```bash
grep mongo-primary /etc/hosts
grep mongo-secondary /etc/hosts
ping mongo-primary.lan
ping mongo-secondary.lan
```

After `mongod` starts on both machines, test MongoDB TCP connectivity.

From primary to secondary:

```bash
mongosh --host mongo-secondary.lan --port 27017
```

From secondary to primary:

```bash
mongosh --host mongo-primary.lan --port 27017
```

If ping fails but `mongosh` works, the setup can continue because MongoDB uses TCP, not ICMP.

## Replica Set Initialization Step

Only after both MongoDB services are running and both nodes can reach each other on port `27017`, connect to the intended primary:

```bash
mongosh --host mongo-primary.lan --port 27017
```

Then run:

```js
rs.initiate({
  _id: "rs0",
  members: [
    {
      _id: 0,
      host: "mongo-primary.lan:27017",
      priority: 2
    },
    {
      _id: 1,
      host: "mongo-secondary.lan:27017",
      priority: 1
    }
  ]
})
```

Then verify:

```js
rs.status()
rs.conf()
db.hello()
```

Expected:

- `mongo-primary.lan:27017` becomes `PRIMARY`.
- `mongo-secondary.lan:27017` becomes `SECONDARY`.

## After Replica Set Works

Start implementing the Python control node:

- config file for MongoDB hosts,
- insert operation,
- update operation,
- soft delete operation,
- `operation_logs` collection,
- follower polling,
- `replication_delay_ms` calculation.
