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
from operations import insert_item, update_item, delete_item

console = Console()
results = []


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def ok(name, detail=""):
    results.append(("PASS", name, detail))
    console.print(f"  [bold green]PASS[/bold green]  {name}" + (f"  [dim]{detail}[/dim]" if detail else ""))


def fail(name, detail=""):
    results.append(("FAIL", name, detail))
    console.print(f"  [bold red]FAIL[/bold red]  {name}" + (f"  [dim]{detail}[/dim]" if detail else ""))


def assert_secondary_has(item_id, expected_version, label):
    secondary = db.get_secondary()
    doc = secondary["items"].find_one({"_id": item_id})
    if doc and doc.get("version") == expected_version:
        ok(label, f"version={expected_version} visible on secondary")
    else:
        got = doc.get("version") if doc else "not found"
        fail(label, f"expected version={expected_version}, got {got}")


def section(title):
    console.print()
    console.rule(f"[bold yellow]{title}[/bold yellow]")


# ---------------------------------------------------------------------------
# Test 1 — Basic CRUD
# ---------------------------------------------------------------------------

def test_basic_crud():
    section("Test 1: Basic CRUD")

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

    secondary_doc = db.get_secondary()["items"].find_one({"_id": item_id})
    if secondary_doc and secondary_doc.get("deleted") is True:
        ok("Soft delete flag visible on secondary")
    else:
        fail("Soft delete flag visible on secondary")


# ---------------------------------------------------------------------------
# Test 2 — Burst writes
# ---------------------------------------------------------------------------

def test_burst_writes(n=10):
    section(f"Test 2: Burst Writes ({n} inserts)")

    ids = []
    delays = []
    timeouts = 0
    for i in range(n):
        item_id, delay = insert_item(f"burst_{i}", {"index": i})
        ids.append(item_id)
        if delay is not None:
            delays.append(delay)
        else:
            timeouts += 1

    visible = len(delays)
    avg = sum(delays) / len(delays) if delays else 0
    mx = max(delays) if delays else 0

    if timeouts == 0:
        ok(f"All {n} burst inserts visible on secondary", f"avg={avg:.1f} ms  max={mx:.1f} ms")
    else:
        fail(f"Burst inserts: {timeouts} timeouts out of {n}")

    # verify all items exist on secondary
    missing = 0
    for item_id in ids:
        doc = db.get_secondary()["items"].find_one({"_id": item_id})
        if not doc:
            missing += 1
    if missing == 0:
        ok(f"All {n} items found on secondary after burst")
    else:
        fail(f"Missing {missing}/{n} items on secondary after burst")


# ---------------------------------------------------------------------------
# Test 3 — Large payload
# ---------------------------------------------------------------------------

def test_large_payload():
    section("Test 3: Large Payload")

    large_value = {"data": "x" * 50_000, "records": list(range(1000))}
    item_id, delay = insert_item("large_payload", large_value)

    if delay is not None:
        ok("Large payload visible on secondary", f"{delay:.1f} ms")
    else:
        fail("Large payload visible on secondary", "timeout")

    doc = db.get_secondary()["items"].find_one({"_id": item_id})
    if doc and len(doc["value"]["data"]) == 50_000:
        ok("Large payload data integrity on secondary")
    else:
        fail("Large payload data integrity on secondary")


# ---------------------------------------------------------------------------
# Test 4 — Sequential version tracking
# ---------------------------------------------------------------------------

def test_sequential_versions(updates=5):
    section(f"Test 4: Sequential Version Tracking ({updates} updates)")

    item_id, _ = insert_item("version_test", {"val": 0})
    all_visible = True

    for i in range(1, updates + 1):
        delay = update_item(item_id, {"val": i})
        if delay is None:
            all_visible = False
            fail(f"Version {i+1} not visible on secondary", "timeout")
            break

    if all_visible:
        ok(f"All {updates} sequential updates visible on secondary")

    doc = db.get_secondary()["items"].find_one({"_id": item_id})
    expected_version = updates + 1
    if doc and doc.get("version") == expected_version:
        ok(f"Final version={expected_version} correct on secondary")
    else:
        got = doc.get("version") if doc else "not found"
        fail(f"Final version check", f"expected {expected_version}, got {got}")


# ---------------------------------------------------------------------------
# Test 5 — Consistency: secondary never returns stale version
# ---------------------------------------------------------------------------

def test_monotonic_version():
    section("Test 5: Monotonic Version on Secondary")

    item_id, _ = insert_item("monotonic_test", {"v": 0})
    versions_seen = []

    for i in range(1, 6):
        update_item(item_id, {"v": i})
        doc = db.get_secondary()["items"].find_one({"_id": item_id})
        if doc:
            versions_seen.append(doc["version"])

    # versions_seen should be non-decreasing
    monotonic = all(versions_seen[i] <= versions_seen[i + 1] for i in range(len(versions_seen) - 1))
    if monotonic:
        ok("Secondary versions are monotonically non-decreasing", str(versions_seen))
    else:
        fail("Secondary returned non-monotonic versions", str(versions_seen))


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
        "[bold white]CENG465 — Full Replication Test Suite[/bold white]\n"
        "[dim]Single-Leader Replication · MongoDB rs0[/dim]",
        border_style="yellow"
    ))

    # clear previous data
    db.get_primary()["items"].drop()
    db.get_primary()["operation_logs"].drop()
    console.print("[dim]Collections cleared.[/dim]")

    test_basic_crud()
    test_burst_writes(n=10)
    test_large_payload()
    test_sequential_versions(updates=5)
    test_monotonic_version()

    failed = print_summary()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
