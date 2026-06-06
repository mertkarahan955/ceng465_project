from pymongo import MongoClient, ASCENDING, DESCENDING
import config

_primary_client = None
_secondary_client = None


def get_primary():
    global _primary_client
    if _primary_client is None:
        _primary_client = MongoClient(
            config.PRIMARY_URI,
            serverSelectionTimeoutMS=config.SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=config.CONNECT_TIMEOUT_MS,
            socketTimeoutMS=config.SOCKET_TIMEOUT_MS,
        )
    return _primary_client[config.DATABASE]


def get_secondary():
    global _secondary_client
    if _secondary_client is None:
        _secondary_client = MongoClient(
            config.SECONDARY_URI,
            directConnection=True,
            readPreference="secondaryPreferred",
            serverSelectionTimeoutMS=config.SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=config.CONNECT_TIMEOUT_MS,
            socketTimeoutMS=config.SOCKET_TIMEOUT_MS,
        )
    return _secondary_client[config.DATABASE]


def ensure_indexes():
    """Create compound indexes on all 6 fleet collections for access pattern optimization.

    Each collection maps to a specific consistency experiment:
      vehicles  — low-write registry, eventually consistent reads OK
      drivers   — low-write, look up by assigned vehicle
      depots    — near-static, city-based queries
      shipments — status changes drive fleet ops, monotonic read matters
      positions — highest write frequency, eventual consistency fine for overview
      incidents — safety-critical, read-after-write required
    """
    pdb = get_primary()

    # ── vehicles ──────────────────────────────────────────────
    # Fleet overview filter: active trucks/vans by type
    pdb["vehicles"].create_index(
        [("value.vehicle_type", ASCENDING), ("last_updated", DESCENDING)],
        name="vehicles_type_updated",
    )
    # Soft-delete sweep
    pdb["vehicles"].create_index(
        [("value.is_active", ASCENDING), ("last_updated", DESCENDING)],
        name="vehicles_active_updated",
    )

    # ── drivers ───────────────────────────────────────────────
    # Dispatch look-up: which driver is assigned to vehicle X?
    pdb["drivers"].create_index(
        [("value.assigned_vehicle_id", ASCENDING)],
        name="drivers_assigned_vehicle",
    )
    pdb["drivers"].create_index(
        [("value.license_class", ASCENDING)],
        name="drivers_license_class",
    )

    # ── depots ────────────────────────────────────────────────
    # City-based depot look-up (origin/destination routing)
    pdb["depots"].create_index(
        [("value.city", ASCENDING)],
        name="depots_city",
    )

    # ── shipments ─────────────────────────────────────────────
    # Fleet ops dashboard: active shipments sorted by recency
    pdb["shipments"].create_index(
        [("value.status", ASCENDING), ("last_updated", DESCENDING)],
        name="shipments_status_updated",
    )
    # Which shipments does vehicle X carry?
    pdb["shipments"].create_index(
        [("value.assigned_vehicle_id", ASCENDING), ("last_updated", DESCENDING)],
        name="shipments_vehicle_updated",
    )

    # ── positions ─────────────────────────────────────────────
    # Latest position for vehicle X (high-frequency write stream)
    pdb["positions"].create_index(
        [("value.vehicle_id", ASCENDING), ("last_updated", DESCENDING)],
        name="positions_vehicle_updated",
    )
    # City-level fleet heatmap on secondary
    pdb["positions"].create_index(
        [("value.city", ASCENDING), ("last_updated", DESCENDING)],
        name="positions_city_updated",
    )

    # ── incidents ─────────────────────────────────────────────
    # Safety dashboard: open incidents ordered by severity
    pdb["incidents"].create_index(
        [("value.resolved", ASCENDING), ("value.severity", ASCENDING)],
        name="incidents_resolved_severity",
    )
    # Per-vehicle incident history
    pdb["incidents"].create_index(
        [("value.vehicle_id", ASCENDING), ("last_updated", DESCENDING)],
        name="incidents_vehicle_updated",
    )
