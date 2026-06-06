PRIMARY_HOST = "mongo-primary.lan"
SECONDARY_HOST = "mongo-secondary.lan"
PORT = 27017
REPLICA_SET = "rs0"
DATABASE = "ceng465"

PRIMARY_URI = f"mongodb://{PRIMARY_HOST}:{PORT}/?replicaSet={REPLICA_SET}"
SECONDARY_URI = f"mongodb://{SECONDARY_HOST}:{PORT}/?directConnection=true&readPreference=secondaryPreferred"

POLL_INTERVAL_MS = 10
POLL_TIMEOUT_MS = 5000
WRITE_TIMEOUT_MS = 5000

# Keep dashboard reads responsive when the secondary is intentionally down.
SERVER_SELECTION_TIMEOUT_MS = 800
CONNECT_TIMEOUT_MS = 800
SOCKET_TIMEOUT_MS = 1200

# Background worker cadence for completing w=1 pending follower checks.
RECONCILE_INTERVAL_MS = 1000
RECONCILE_BATCH_SIZE = 100
RECONCILE_DRAIN_BATCHES = 20

# Secondary health monitoring cadence.
HEALTHCHECK_INTERVAL_MS = 1000

# Fleet domain — the 6 collections that replace the generic "items" collection.
FLEET_COLLECTIONS = ["vehicles", "drivers", "depots", "shipments", "positions", "incidents"]
