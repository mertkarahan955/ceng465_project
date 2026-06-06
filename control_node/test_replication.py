"""
Full replication test suite for CENG465.

Run on the primary / control node:
    python test_replication.py

While this runs, start the live tracer on either machine:
    python trace.py
"""

import time
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from bson import ObjectId

import db
import config
import operations
from operations import insert_item, update_item, delete_item

console = Console()
results = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ok(name, detail=""):
    results.append(("PASS", name, detail))
    console.print(f"  [bold green]PASS[/bold green]  {name}" + (f"  [dim]{detail}[/dim]" if detail else ""))


def fail(name, detail=""):
    results.append(("FAIL", name, detail))
    console.print(f"  [bold red]FAIL[/bold red]  {name}" + (f"  [dim]{detail}[/dim]" if detail else ""))


def assert_secondary_has(item_id, expected_version, label, collection="items"):
    doc = db.get_secondary()[collection].find_one({"_id": item_id})
    if doc and doc.get("version") == expected_version:
        ok(label, f"version={expected_version} visible on secondary [{collection}]")
    else:
        got = doc.get("version") if doc else "not found"
        fail(label, f"expected version={expected_version}, got {got} [{collection}]")


def section(title):
    console.print()
    console.rule(f"[bold yellow]{title}[/bold yellow]")


# ---------------------------------------------------------------------------
# Test 1 — Basic CRUD on legacy items collection
# ---------------------------------------------------------------------------

def test_basic_crud():
    section("Test 1: Basic CRUD (items collection)")

    item_id, delay = insert_item("crud_test", {"x": 1})
    if delay is not None:
        ok("INSERT visible on secondary", f"{delay:.1f} ms")
    else:
        fail("INSERT visible on secondary", "timeout")

    assert_secondary_has(item_id, 1, "Secondary version=1 after INSERT")

    delay = update_item(item_id, {"x": 2})
    if delay is not None:
        ok("UPDATE visible on secondary", f"{delay:.1f} ms")
    else:
        fail("UPDATE visible on secondary", "timeout")

    assert_secondary_has(item_id, 2, "Secondary version=2 after UPDATE")

    delay = delete_item(item_id)
    if delay is not None:
        ok("DELETE visible on secondary", f"{delay:.1f} ms")
    else:
        fail("DELETE visible on secondary", "timeout")

    doc = db.get_secondary()["items"].find_one({"_id": item_id})
    if doc and doc.get("deleted") is True:
        ok("Soft delete flag visible on secondary")
    else:
        fail("Soft delete flag visible on secondary")


# ---------------------------------------------------------------------------
# Test 2 — Fleet domain: vehicles + drivers + depots
# ---------------------------------------------------------------------------

def test_fleet_static_collections():
    section("Test 2: Fleet Static Collections (vehicles / drivers / depots)")

    # vehicles
    vid, delay = operations.insert_vehicle("TST-001", "34 TST 001", "truck", 5000, 2021)
    if delay is not None:
        ok("Vehicle INSERT visible on secondary", f"{delay:.1f} ms")
    else:
        fail("Vehicle INSERT visible on secondary", "timeout")
    assert_secondary_has(vid, 1, "Vehicle v=1 on secondary", collection="vehicles")

    # drivers
    did, delay = operations.insert_driver("TST-DRV", "Test Surucu", "E", "+90 555 000 0000", "TST-001")
    if delay is not None:
        ok("Driver INSERT visible on secondary", f"{delay:.1f} ms")
    else:
        fail("Driver INSERT visible on secondary", "timeout")
    assert_secondary_has(did, 1, "Driver v=1 on secondary", collection="drivers")

    # depots
    dep_id, delay = operations.insert_depot("TST-DEP", "Test Depo", "Istanbul", 41.01, 28.97, 20)
    if delay is not None:
        ok("Depot INSERT visible on secondary", f"{delay:.1f} ms")
    else:
        fail("Depot INSERT visible on secondary", "timeout")
    assert_secondary_has(dep_id, 1, "Depot v=1 on secondary", collection="depots")


# ---------------------------------------------------------------------------
# Test 3 — Shipments: status monotonicity on secondary
# ---------------------------------------------------------------------------

def test_shipment_monotonic_reads():
    section("Test 3: Shipments — Monotonic Reads Experiment")

    shp_id, _ = operations.insert_shipment(
        "MON-001", "DEP-IST", "DEP-ANK", "MonotonicCo", 800, 10,
        status="pending", assigned_vehicle_id="TST-001",
    )

    statuses = ["pending", "in_transit", "in_transit", "delivered"]
    versions_seen_secondary = []

    for new_status in statuses:
        delay = update_item(shp_id, {"status": new_status, "shipment_id": "MON-001",
                                     "origin_depot": "DEP-IST", "destination_depot": "DEP-ANK",
                                     "customer": "MonotonicCo", "weight_kg": 800,
                                     "package_count": 10, "assigned_vehicle_id": "TST-001"},
                            collection="shipments")
        doc = db.get_secondary()["shipments"].find_one({"_id": shp_id})
        if doc:
            versions_seen_secondary.append(doc["version"])

    monotonic = all(
        versions_seen_secondary[i] <= versions_seen_secondary[i + 1]
        for i in range(len(versions_seen_secondary) - 1)
    )
    if monotonic:
        ok("Shipment versions are monotonically non-decreasing on secondary", str(versions_seen_secondary))
    else:
        fail("Non-monotonic shipment versions on secondary", str(versions_seen_secondary))


# ---------------------------------------------------------------------------
# Test 4 — Positions: high-frequency burst + eventual consistency
# ---------------------------------------------------------------------------

def test_position_burst(n=10):
    section(f"Test 4: Position Burst ({n} GPS updates) — Eventual Consistency")

    ids = []
    delays = []
    timeouts = 0

    for i in range(n):
        pid, delay = operations.insert_position(
            "BURST-001",
            lat=39.9 + i * 0.01,
            lng=32.8 + i * 0.01,
            city="Ankara",
            district="Test",
            speed_kmh=60 + i,
        )
        ids.append(pid)
        if delay is not None:
            delays.append(delay)
        else:
            timeouts += 1

    avg = sum(delays) / len(delays) if delays else 0
    mx  = max(delays) if delays else 0

    if timeouts == 0:
        ok(f"All {n} position inserts visible on secondary", f"avg={avg:.1f} ms  max={mx:.1f} ms")
    else:
        fail(f"Position burst: {timeouts} timeouts out of {n}")

    # All positions must eventually appear on secondary
    time.sleep(0.5)
    missing = sum(1 for pid in ids if not db.get_secondary()["positions"].find_one({"_id": pid}))
    if missing == 0:
        ok(f"All {n} position docs found on secondary (eventual consistency satisfied)")
    else:
        fail(f"Missing {missing}/{n} position docs on secondary")


# ---------------------------------------------------------------------------
# Test 5 — Incidents: read-after-write from primary
# ---------------------------------------------------------------------------

def test_incident_read_after_write():
    section("Test 5: Incidents — Read-After-Write Experiment")

    inc_id, delay = operations.insert_incident(
        "RAW-001", "breakdown", "critical",
        "Test incident for read-after-write demo",
    )
    if delay is not None:
        ok("Incident INSERT visible on secondary", f"{delay:.1f} ms")
    else:
        fail("Incident INSERT visible on secondary (secondary check)", "timeout — but primary read-after-write still works")

    # Read-After-Write: read from PRIMARY immediately after write
    doc = db.get_primary()["incidents"].find_one({"_id": inc_id})
    if doc and doc.get("value", {}).get("severity") == "critical":
        ok("Read-After-Write: incident immediately visible on PRIMARY", "severity=critical")
    else:
        fail("Read-After-Write: incident not found on PRIMARY after insert")

    # Also verify access pattern function works
    open_incs = operations.get_open_incidents()
    found = any(
        str(d.get("_id")) == str(inc_id) or
        d.get("value", {}).get("vehicle_id") == "RAW-001"
        for d in open_incs
    )
    if found:
        ok("get_open_incidents() returns newly inserted incident from PRIMARY")
    else:
        fail("get_open_incidents() did not return newly inserted incident")


# ---------------------------------------------------------------------------
# Test 6 — Cross-collection operation log: target_collection field
# ---------------------------------------------------------------------------

def test_operation_log_collection_field():
    section("Test 6: Operation Logs — target_collection Field")

    # Insert one doc into each fleet collection
    collections_tested = []
    insert_fns = [
        ("vehicles",  lambda: operations.insert_vehicle("LOG-001", "06 LOG 001", "van", 1500, 2022)),
        ("drivers",   lambda: operations.insert_driver("LOG-DRV", "Log Tester", "C", "+90 555 999 9999")),
        ("positions", lambda: operations.insert_position("LOG-001", 39.9, 32.8, "Ankara", "Test", 55)),
    ]
    for coll, fn in insert_fns:
        try:
            fn()
            collections_tested.append(coll)
        except Exception as e:
            fail(f"Insert into {coll} failed", str(e))

    # Check that operation_logs records the correct target_collection
    pdb = db.get_primary()
    for coll in collections_tested:
        log = pdb["operation_logs"].find_one(
            {"target_collection": coll, "operation_type": "insert"},
            sort=[("log_index", -1)],
        )
        if log and log.get("target_collection") == coll:
            ok(f"operation_logs.target_collection = '{coll}'")
        else:
            fail(f"operation_logs missing target_collection='{coll}'")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary():
    console.print()
    console.rule("[bold white]Test Summary[/bold white]")
    t = Table(box=box.ROUNDED, border_style="white")
    t.add_column("Result", width=6)
    t.add_column("Test")
    t.add_column("Detail", style="dim")

    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")

    for status, name, detail in results:
        color = "green" if status == "PASS" else "red"
        t.add_row(f"[{color}]{status}[/{color}]", name, detail)

    console.print(t)
    total = passed + failed
    color = "green" if failed == 0 else "red"
    console.print(f"\n[{color}]  {passed}/{total} tests passed[/{color}]\n")
    return failed


def main():
    console.print(Panel.fit(
        "[bold white]CENG465 — Fleet Replication Test Suite[/bold white]\n"
        "[dim]Single-Leader Replication · MongoDB rs0 · 6 Collections[/dim]",
        border_style="yellow"
    ))

    # Ensure indexes exist before running tests
    try:
        db.ensure_indexes()
        console.print("[dim]Indexes ensured.[/dim]")
    except Exception as e:
        console.print(f"[yellow]Warning: could not ensure indexes: {e}[/yellow]")

    # Clear fleet collections + legacy items
    pdb = db.get_primary()
    for coll in list(config.FLEET_COLLECTIONS) + ["items", "operation_logs"]:
        pdb[coll].drop()
    console.print("[dim]Collections cleared.[/dim]")

    test_basic_crud()
    test_fleet_static_collections()
    test_shipment_monotonic_reads()
    test_position_burst(n=10)
    test_incident_read_after_write()
    test_operation_log_collection_field()

    failed = print_summary()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
