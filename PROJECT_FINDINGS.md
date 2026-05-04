# CENG465 Project Findings

## Project Context

- Course project: CENG465 Principles of Data-Intensive Systems
- Topic: Data Replication in a Single-Leader Environment
- Final due date: 08.06.2026 23:55
- Progress presentation date: 05.05.2026
- Current scope:
  - Environment Setup and Role Assignment
  - Data Schema and Replication Logging

## Key Requirements From PDF

- A distributed database system must be selected.
- The system must use a single-leader replication setup.
- The project must be done by exactly two group members.
- One member/node should act as Leader / Primary.
- One member/node should act as Follower / Secondary.
- Replication must be configured and verified.
- Data changes on the leader must become visible on the follower.
- The environment must be truly distributed:
  - multiple physical/local machines on the same network, or
  - cloud VMs such as AWS, GCP, or Azure.
- A local-only setup with Docker containers or local VMs on a single machine is not sufficient.

## Database Choice

MongoDB is a good fit because MongoDB Replica Sets naturally provide:

- a Primary node for writes,
- Secondary nodes for replicated reads,
- leader-based replication behavior,
- observable replication lag,
- support for insert, update, and delete operations.

## Useful MongoDB Documentation

- MongoDB Replication Overview: https://www.mongodb.com/docs/manual/replication/
- Deploy a Self-Managed Replica Set: https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/
- Self-Managed Replication Reference: https://www.mongodb.com/docs/manual/reference/replication/
- Local setup guide in this repo: `MONGODB_MACOS_REPLICA_SET_SETUP.md`
- Current setup progress in this repo: `SETUP_PROGRESS.md`

Important points from the official documentation:

- A replica set is a group of `mongod` processes that maintain the same data set.
- A replica set has one Primary and one or more Secondaries.
- The Primary receives write operations.
- Secondaries replicate the Primary's operation log, also called the `oplog`, and apply those operations to their own data sets.
- Replication to secondaries is asynchronous, which is exactly why replication lag can be observed and measured.
- For deployment, MongoDB recommends separate machines for replica set members and the default MongoDB port `27017`.
- Replica set members should be reachable by stable hostnames or DNS entries. For the local macOS setup, `/etc/hosts` entries can be used if needed.
- MongoDB binds to `localhost` by default, so each node must be configured with `net.bindIp` or `--bind_ip` to accept connections from the other Mac and from the control node.

## Proposed Architecture

```text
Control Node
  Python experiment driver
  - sends insert/update/delete operations
  - writes operation logs
  - reads from Primary and Secondary
  - measures follower visibility time
  - calculates replication delay

MongoDB Node A
  Primary / Leader

MongoDB Node B
  Secondary / Follower
```

The control node is not responsible for MongoDB leader election. MongoDB handles primary/secondary roles internally through the replica set. The control node acts as an experiment coordinator and replication logger.

## Deployment Decision

The project will be deployed on two separate macOS devices connected to the same local network.

Planned node assignment:

- macOS Device 1: MongoDB Primary / Leader, planned IP `192.168.88.146`
- macOS Device 2: MongoDB Secondary / Follower, planned IP `192.168.88.105`
- Control Node: Python experiment driver, can run on either macOS device as long as it can reach both MongoDB nodes over the network.

This satisfies the project requirement for a truly distributed environment because the database nodes run on separate physical machines rather than local-only containers or VMs on a single machine.

Important setup notes:

- Both Macs must be on the same network.
- Each MongoDB node must bind to a network-accessible IP address, not only `localhost`.
- macOS firewall settings may need to allow inbound MongoDB traffic on port `27017`.
- Replica set configuration should use stable LAN IP addresses or resolvable hostnames.

## Raft-Inspired Decision

A full Raft implementation is not recommended for the current scope because it would require implementing leader election, heartbeat, log replication, commit index, terms, and failure handling manually.

Instead, the project can use MongoDB Replica Set as the actual replication system and add Raft-inspired metadata for analysis:

- `leader_term`
- `log_index`
- `operation_id`
- `version`

This gives the project a strong ordering and replication-analysis model without replacing MongoDB's built-in replication.

Suggested report wording:

> The system uses MongoDB Replica Set as the actual single-leader replication mechanism. The control node maintains Raft-inspired metadata such as leader_term, log_index, and operation_id to analyze update ordering, visibility, and replication delay.

## Proposed Data Schema

### `items` Collection

```js
{
  _id: ObjectId,
  key: string,
  value: object,
  version: number,
  leader_term: number,
  last_log_index: number,
  last_operation_id: string,
  last_updated: Date,
  deleted: boolean,
  created_by: string
}
```

Purpose:

- stores the actual application data,
- supports insert/update/delete,
- tracks version changes,
- keeps the latest operation metadata visible on both primary and secondary.

### `operation_logs` Collection

```js
{
  _id: ObjectId,
  operation_id: string,
  leader_term: number,
  log_index: number,
  operation_type: "insert" | "update" | "delete",
  target_collection: "items",
  target_id: ObjectId,
  leader_write_time: Date,
  follower_visible_time: Date | null,
  replication_delay_ms: number | null,
  version_before: number | null,
  version_after: number | null,
  client_id: string,
  status: "written_on_leader" | "visible_on_follower" | "timeout"
}
```

Purpose:

- logs every write operation,
- records operation ordering,
- measures when an update becomes visible on the follower,
- provides evidence for replication behavior in the presentation and report.

## MVP For Progress Presentation

- [x] Decide the deployment environment: two macOS devices on the same local network.
- [x] Add local MongoDB setup guide for two macOS devices.
- [ ] Install MongoDB on both database nodes.
- [ ] Configure MongoDB Replica Set.
- [ ] Verify Primary and Secondary roles using `rs.status()` and `db.hello()`.
- [ ] Create Python control node project.
- [ ] Add MongoDB connection configuration.
- [ ] Implement `items` schema usage.
- [ ] Implement `operation_logs` schema usage.
- [ ] Implement insert operation.
- [ ] Implement update operation.
- [ ] Implement soft delete operation.
- [ ] Log every write with:
  - timestamp,
  - operation ID,
  - log index,
  - leader term,
  - version before/after.
- [ ] Poll the secondary until the written state becomes visible.
- [ ] Calculate `replication_delay_ms`.
- [ ] Prepare a demo script for:
  - insert,
  - update,
  - delete,
  - log inspection.

## Presentation Evidence To Prepare

- Schema design.
- Explanation of tracking fields:
  - `version`,
  - `last_updated`,
  - `operation_id`,
  - `log_index`,
  - `leader_term`.
- Logging mechanism.
- Sample insert/update/delete operations.
- Evidence that a write first appears on the leader and later becomes visible on the follower.
- Sample replication delay measurements.

## Suggested Role Assignment

- Member 1:
  - MongoDB Primary setup,
  - write operation implementation,
  - leader-side verification.
- Member 2:
  - MongoDB Secondary setup,
  - follower read verification,
  - replication delay measurement.
- Shared:
  - Python control node,
  - schema design,
  - logging design,
  - demo and report.

## Later Project Phases

- [ ] Eventual consistency experiment.
- [ ] Monotonic reads experiment.
- [ ] Read-after-write consistency experiment.
- [ ] Concurrent writes experiment.
- [ ] Export experiment logs as CSV or JSON.
- [ ] Produce tables/graphs for the final report.
- [ ] Prepare final presentation PDF, maximum 10 pages.
- [ ] Prepare final report PDF.
- [ ] Package submission as `Gxx_CENG465_Project.zip`.
