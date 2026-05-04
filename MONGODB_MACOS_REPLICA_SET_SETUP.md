# MongoDB Replica Set Setup on Two macOS Devices

This guide describes the planned setup for the CENG465 project using two separate macOS devices on the same local network.

For the latest current-state notes and where the setup stopped, see `SETUP_PROGRESS.md`.

Official references:

- MongoDB Replication Overview: https://www.mongodb.com/docs/manual/replication/
- Deploy a Self-Managed Replica Set: https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/
- Install MongoDB Community Edition on macOS: https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-os-x/
- MongoDB Versioning: https://www.mongodb.com/docs/manual/reference/versioning/

## Target Architecture

```text
macOS Device 1
  hostname: mongo-primary.lan
  MongoDB role: Primary / Leader
  port: 27017

macOS Device 2
  hostname: mongo-secondary.lan
  MongoDB role: Secondary / Follower
  port: 27017

Control Node
  Python experiment driver
  can run on either Mac
```

## Values To Fill

Current planned values:

```text
PRIMARY_IP=192.168.88.146
SECONDARY_IP=192.168.88.105
PRIMARY_HOST=mongo-primary.lan
SECONDARY_HOST=mongo-secondary.lan
REPLICA_SET_NAME=rs0
MONGO_PORT=27017
```

If the physical role assignment is reversed, swap `PRIMARY_IP` and `SECONDARY_IP` consistently in this document and in `/etc/hosts` on both Macs.

## Version Decision

Prefer a stable MongoDB release series for the demo setup. As of the current MongoDB documentation, the 8.2 series is listed as the current stable release, while 8.3 documentation is under the upcoming release section and marked as release-candidate-oriented.

If using a manually downloaded tarball such as:

```text
https://fastdl.mongodb.org/osx/mongodb-macos-arm64-8.3.1.tgz
```

then both Macs must use the exact same MongoDB server version. Do not mix `8.3.1` on one Mac with `8.0.x` or `8.2.x` on the other Mac.

For the safest project demo path:

- use Homebrew with the same MongoDB version on both Macs, or
- use the same downloaded `.tgz` binary on both Macs and keep the setup fully manual.

Homebrew is easier because it installs service files, config paths, and related tooling. The `.tgz` method is workable, but it requires manual binary placement, config management, and usually a separate `mongosh` install.

### Tarball Path Troubleshooting

After extracting the `.tgz`, confirm where the `mongod` binary actually is:

```bash
pwd
find . -maxdepth 4 -type f -name mongod
```

If the current directory is the extracted MongoDB directory:

```bash
./bin/mongod --version
```

If the current directory is the parent directory that contains `mongodb-macos-arm64-8.3.1`:

```bash
./mongodb-macos-arm64-8.3.1/bin/mongod --version
```

If the shell reports `no such file or directory`, check the actual extracted folder name:

```bash
ls
```

Then replace `mongodb-macos-arm64-8.3.1` with the exact folder name printed by `ls`.

`install_compass` is not required for this project. The project only needs `mongod`, and optionally `mongosh` for manual inspection.

To find each Mac's local IP address:

```bash
ipconfig getifaddr en0
```

If the Mac is connected by Ethernet instead of Wi-Fi, `en0` may not be the correct interface. In that case:

```bash
ifconfig
```

## 1. Add Stable Hostnames

MongoDB recommends hostnames for replica set members. Add the same entries to `/etc/hosts` on both Macs:

```text
<PRIMARY_IP> mongo-primary.lan
<SECONDARY_IP> mongo-secondary.lan
```

Example:

```text
192.168.1.20 mongo-primary.lan
192.168.1.21 mongo-secondary.lan
```

Verify from both Macs:

```bash
ping mongo-primary.lan
ping mongo-secondary.lan
```

## 2. Install MongoDB on Both Macs

Install MongoDB Community Edition with Homebrew on both devices:

```bash
brew tap mongodb/brew
brew update
brew install mongodb-community
```

Verify:

```bash
mongod --version
mongosh --version
brew services list
```

Both Macs should use the same MongoDB version. If a versioned formula is preferred, use the same versioned formula on both Macs, for example `mongodb-community@8.0`.

## 3. Configure `mongod.conf`

Find the MongoDB config file:

Apple Silicon:

```text
/opt/homebrew/etc/mongod.conf
```

Intel Mac:

```text
/usr/local/etc/mongod.conf
```

On both Macs, update the config with the same replica set name and a network-accessible bind IP.

Primary Mac example:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,<PRIMARY_IP>

replication:
  replSetName: rs0
```

Secondary Mac example:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,<SECONDARY_IP>

replication:
  replSetName: rs0
```

The replica set member `host` values can still use stable hostnames such as `mongo-primary.lan:27017` and `mongo-secondary.lan:27017`. The `bindIp` value should include the local machine's actual LAN IP address so other devices on the same network can connect to `mongod`.

Do not define `bindIp` twice under `net`. If the Homebrew default config contains this:

```yaml
net:
  bindIp: 127.0.0.1, ::1
  ipv6: true
  port: 27017
```

replace the `net` block with one clean version for each machine.

Primary:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.146
```

Secondary:

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,192.168.88.105
```

For this project setup, IPv6 is not needed. Keeping the config IPv4-only reduces confusion while both Macs communicate over the `192.168.88.x` LAN addresses.

## 4. Restart MongoDB on Both Macs

```bash
brew services restart mongodb/brew/mongodb-community
```

Check status:

```bash
brew services list
```

## 5. Check Network Connectivity

From the primary Mac:

```bash
mongosh --host mongo-secondary.lan --port 27017
```

From the secondary Mac:

```bash
mongosh --host mongo-primary.lan --port 27017
```

If either connection fails:

- confirm both Macs are on the same network,
- check `/etc/hosts`,
- check macOS firewall settings,
- confirm MongoDB is bound to the hostname, not only `localhost`,
- confirm port `27017` is reachable.

### One-Way Ping Troubleshooting

If the primary can ping the secondary but the secondary cannot ping the primary:

1. On the secondary, verify hostname resolution:

   ```bash
   ping 192.168.88.146
   ping mongo-primary.lan
   grep mongo-primary /etc/hosts
   ```

2. On the primary, confirm the IP is still correct:

   ```bash
   ipconfig getifaddr en0
   ```

3. On the primary, check macOS firewall settings. Temporarily disable blocking for local testing if needed:

   ```text
   System Settings -> Network -> Firewall
   ```

4. Even if ICMP ping is blocked, MongoDB may still be reachable. Test the actual database port from the secondary:

   ```bash
   mongosh --host mongo-primary.lan --port 27017
   ```

## 6. Initiate the Replica Set

Run this only once, from `mongosh` connected to the intended primary:

```bash
mongosh --host mongo-primary.lan --port 27017
```

Then:

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

Check status:

```js
rs.status()
rs.conf()
db.hello()
```

Expected result:

- `mongo-primary.lan:27017` should become `PRIMARY`.
- `mongo-secondary.lan:27017` should become `SECONDARY`.

## 7. Test Basic Replication

Connect to the primary:

```bash
mongosh "mongodb://mongo-primary.lan:27017/?replicaSet=rs0"
```

Run:

```js
use ceng465
db.items.insertOne({
  key: "manual-test-1",
  value: { message: "hello from primary" },
  version: 1,
  leader_term: 1,
  last_log_index: 1,
  last_operation_id: "manual-op-1",
  last_updated: new Date(),
  deleted: false,
  created_by: "manual-test"
})
```

Connect to the secondary with secondary read preference:

```bash
mongosh "mongodb://mongo-secondary.lan:27017/?replicaSet=rs0&readPreference=secondary"
```

Run:

```js
use ceng465
db.items.find({ key: "manual-test-1" })
```

If MongoDB blocks the read because this shell is connected directly to a secondary, run:

```js
db.getMongo().setReadPref("secondary")
```

Then try the `find` again.

## 8. Notes About Two-Node Replica Sets

The project uses two physical Macs because the assignment requires a distributed environment. This is enough to demonstrate primary-to-secondary replication and replication lag.

However, MongoDB generally recommends an odd number of voting replica set members for smoother elections. With only two voting members, failover behavior is limited. For this project stage, that is acceptable because the focus is:

- data schema,
- operation logging,
- follower visibility,
- replication delay.

If later experiments need reliable failover/election behavior, add a third node as an arbiter.

## 9. Next Step After Setup

After `rs.status()` shows one Primary and one Secondary, implement the Python control node:

- connect to the replica set,
- perform insert/update/delete on the Primary,
- log each operation in `operation_logs`,
- poll the Secondary,
- calculate `replication_delay_ms`.
