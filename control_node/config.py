PRIMARY_HOST = "mongo-primary.lan"
SECONDARY_HOST = "mongo-secondary.lan"
PORT = 27017
REPLICA_SET = "rs0"
DATABASE = "ceng465"

PRIMARY_URI = f"mongodb://{PRIMARY_HOST}:{PORT}/?replicaSet={REPLICA_SET}"
SECONDARY_URI = f"mongodb://{SECONDARY_HOST}:{PORT}/?readPreference=secondary&replicaSet={REPLICA_SET}"

POLL_INTERVAL_MS = 10
POLL_TIMEOUT_MS = 5000
