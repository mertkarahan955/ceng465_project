"""
Comprehensive replication benchmark for CENG465.

Single-machine mode (default, run on primary/control node):
    python benchmark.py --ops 1000

Two-machine writer mode (run on primary-side machine):
    python benchmark.py --mode writer --ops 1000 --run-id run1

Two-machine reader mode (run on secondary-side machine simultaneously):
    python benchmark.py --mode reader --run-id run1
"""

import argparse
import os
import random
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pymongo import MongoClient, WriteConcern
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich import box

import config

console = Console()
MACHINE_ID = socket.gethostname()
TIMEOUT_MS = config.POLL_TIMEOUT_MS
POLL_MS = config.POLL_INTERVAL_MS

OP_COLORS = {"insert": "#4CAF50", "update": "#2196F3", "delete": "#FF5722"}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def primary_db():
    client = MongoClient(config.PRIMARY_URI)
    return client[config.DATABASE]


def secondary_db():
    client = MongoClient(config.SECONDARY_URI, readPreference="secondary")
    return client[config.DATABASE]


def now_ms() -> float:
    return time.time() * 1000


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Core ops (low-level, timing-aware)
# ---------------------------------------------------------------------------

def do_insert(pdb, key: str, value: dict, term: int, log_idx: int, op_id: str) -> tuple:
    doc = {
        "key": key, "value": value, "version": 1,
        "leader_term": term, "last_log_index": log_idx,
        "last_operation_id": op_id, "last_updated": utcnow(),
        "deleted": False,
    }
    t0 = now_ms()
    result = pdb["items"].with_options(write_concern=WriteConcern(w="majority")).insert_one(doc)
    write_ms = now_ms() - t0
    return result.inserted_id, write_ms


def do_update(pdb, item_id, new_value: dict, version_after: int, term: int, log_idx: int, op_id: str) -> float:
    t0 = now_ms()
    pdb["items"].with_options(write_concern=WriteConcern(w="majority")).update_one(
        {"_id": item_id},
        {"$set": {
            "value": new_value, "version": version_after,
            "last_log_index": log_idx, "last_operation_id": op_id,
            "last_updated": utcnow(), "leader_term": term,
        }}
    )
    return now_ms() - t0


def do_delete(pdb, item_id, version_after: int, term: int, log_idx: int, op_id: str) -> float:
    t0 = now_ms()
    pdb["items"].with_options(write_concern=WriteConcern(w="majority")).update_one(
        {"_id": item_id},
        {"$set": {
            "deleted": True, "version": version_after,
            "last_log_index": log_idx, "last_operation_id": op_id,
            "last_updated": utcnow(), "leader_term": term,
        }}
    )
    return now_ms() - t0


def poll_secondary(sdb, item_id, expected_version: int) -> tuple[float | None, float | None]:
    """Returns (replication_delay_ms, secondary_read_latency_ms) or (None, None) on timeout."""
    deadline = now_ms() + TIMEOUT_MS
    write_confirmed_at = now_ms()
    while now_ms() < deadline:
        t0 = now_ms()
        doc = sdb["items"].find_one({"_id": item_id})
        read_ms = now_ms() - t0
        if doc and doc.get("version") == expected_version:
            delay = now_ms() - write_confirmed_at
            return delay, read_ms
        time.sleep(POLL_MS / 1000)
    return None, None


def primary_read_latency(pdb, item_id) -> float:
    t0 = now_ms()
    pdb["items"].find_one({"_id": item_id})
    return now_ms() - t0


# ---------------------------------------------------------------------------
# Single-machine benchmark
# ---------------------------------------------------------------------------

def run_single(ops: int, run_id: str):
    pdb = primary_db()
    sdb = secondary_db()

    pdb["items"].drop()
    pdb["benchmark_results"].delete_many({"run_id": run_id})

    active: list[dict] = []   # {id, version}
    term = 1
    log_idx = 0
    results = []

    console.print(Panel.fit(
        f"[bold white]CENG465 Benchmark — Single Mode[/bold white]\n"
        f"[dim]run_id={run_id}  ops={ops}  machine={MACHINE_ID}[/dim]",
        border_style="yellow"
    ))

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Running ops...", total=ops)

        for seq in range(1, ops + 1):
            # Choose operation
            if not active:
                op_type = "insert"
            else:
                op_type = random.choices(
                    ["insert", "update", "delete"],
                    weights=[0.40, 0.40, 0.20]
                )[0]

            log_idx += 1
            op_id = str(uuid.uuid4())
            op_start = utcnow()
            status = "ok"
            item_id = None
            write_ms = None
            replication_delay_ms = None
            primary_read_ms = None
            secondary_read_ms = None
            version_after = None

            try:
                if op_type == "insert":
                    key = f"bench_{seq}"
                    item_id, write_ms = do_insert(
                        pdb, key, {"seq": seq, "payload": "x" * 128},
                        term, log_idx, op_id
                    )
                    version_after = 1
                    active.append({"id": item_id, "version": 1})

                elif op_type == "update":
                    entry = random.choice(active)
                    item_id = entry["id"]
                    version_after = entry["version"] + 1
                    write_ms = do_update(
                        pdb, item_id, {"seq": seq, "payload": "y" * 128},
                        version_after, term, log_idx, op_id
                    )
                    entry["version"] = version_after

                elif op_type == "delete":
                    entry = random.choice(active)
                    item_id = entry["id"]
                    version_after = entry["version"] + 1
                    write_ms = do_delete(
                        pdb, item_id, version_after, term, log_idx, op_id
                    )
                    active.remove(entry)

                primary_read_ms = primary_read_latency(pdb, item_id)
                replication_delay_ms, secondary_read_ms = poll_secondary(sdb, item_id, version_after)
                if replication_delay_ms is None:
                    status = "timeout"

            except Exception as e:
                status = "error"
                console.print(f"[red]  op {seq} error: {e}[/red]")

            record = {
                "run_id": run_id,
                "machine_id": MACHINE_ID,
                "seq": seq,
                "op_type": op_type,
                "item_id": str(item_id) if item_id else None,
                "version_after": version_after,
                "op_start": op_start,
                "write_latency_ms": round(write_ms, 3) if write_ms else None,
                "replication_delay_ms": round(replication_delay_ms, 3) if replication_delay_ms else None,
                "primary_read_latency_ms": round(primary_read_ms, 3) if primary_read_ms else None,
                "secondary_read_latency_ms": round(secondary_read_ms, 3) if secondary_read_ms else None,
                "status": status,
                "mode": "single",
            }
            results.append(record)
            pdb["benchmark_results"].insert_one(dict(record))
            progress.advance(task)

    print_stats(results)
    generate_charts(results, run_id, mode="single")


# ---------------------------------------------------------------------------
# Two-machine: writer
# ---------------------------------------------------------------------------

def run_writer(ops: int, run_id: str):
    pdb = primary_db()
    pdb["items"].drop()
    pdb["benchmark_manifest"].delete_many({"run_id": run_id})

    active: list[dict] = []
    term = 1
    log_idx = 0
    results = []

    console.print(Panel.fit(
        f"[bold white]CENG465 Benchmark — Writer Mode[/bold white]\n"
        f"[dim]run_id={run_id}  ops={ops}  machine={MACHINE_ID}[/dim]\n"
        "[dim]Start the reader on the secondary machine now, then press Enter...[/dim]",
        border_style="green"
    ))
    input()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TaskProgressColumn(), TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Writing ops to PRIMARY...", total=ops)

        for seq in range(1, ops + 1):
            if not active:
                op_type = "insert"
            else:
                op_type = random.choices(
                    ["insert", "update", "delete"],
                    weights=[0.40, 0.40, 0.20]
                )[0]

            log_idx += 1
            op_id = str(uuid.uuid4())
            write_time = utcnow()
            status = "ok"
            item_id = None
            write_ms = None
            version_after = None

            try:
                if op_type == "insert":
                    item_id, write_ms = do_insert(
                        pdb, f"bench_{seq}", {"seq": seq, "payload": "x" * 128},
                        term, log_idx, op_id
                    )
                    version_after = 1
                    active.append({"id": item_id, "version": 1})

                elif op_type == "update":
                    entry = random.choice(active)
                    item_id = entry["id"]
                    version_after = entry["version"] + 1
                    write_ms = do_update(
                        pdb, item_id, {"seq": seq, "payload": "y" * 128},
                        version_after, term, log_idx, op_id
                    )
                    entry["version"] = version_after

                elif op_type == "delete":
                    entry = random.choice(active)
                    item_id = entry["id"]
                    version_after = entry["version"] + 1
                    write_ms = do_delete(pdb, item_id, version_after, term, log_idx, op_id)
                    active.remove(entry)

            except Exception as e:
                status = "error"

            manifest_entry = {
                "run_id": run_id,
                "seq": seq,
                "op_type": op_type,
                "item_id": str(item_id) if item_id else None,
                "expected_version": version_after,
                "writer_write_time": write_time,
                "write_latency_ms": round(write_ms, 3) if write_ms else None,
                "writer_machine_id": MACHINE_ID,
                "status": status,
            }
            pdb["benchmark_manifest"].insert_one(manifest_entry)

            record = {**manifest_entry, "mode": "writer"}
            results.append(record)
            progress.advance(task)

    console.print(f"\n[green]Writer done.[/green] {ops} ops written. Reader can now finish.\n")
    print_stats(results, writer_only=True)


# ---------------------------------------------------------------------------
# Two-machine: reader
# ---------------------------------------------------------------------------

def _process_manifest_doc(doc, sdb, pdb, results, processed, progress, task):
    from bson import ObjectId
    if not doc or doc.get("run_id") != doc.get("run_id"):
        return
    seq = doc["seq"]
    if seq in processed:
        return
    processed.add(seq)

    item_id_str = doc.get("item_id")
    if not item_id_str:
        return

    item_id = ObjectId(item_id_str)
    expected_version = doc["expected_version"]
    writer_write_time = doc["writer_write_time"]
    if writer_write_time.tzinfo is None:
        writer_write_time = writer_write_time.replace(tzinfo=timezone.utc)

    replication_delay_ms, secondary_read_ms = poll_secondary(sdb, item_id, expected_version)
    visible_time = utcnow()

    cross_machine_delay_ms = None
    if replication_delay_ms is not None:
        cross_machine_delay_ms = (visible_time - writer_write_time).total_seconds() * 1000

    record = {
        "run_id": doc["run_id"],
        "seq": seq,
        "op_type": doc["op_type"],
        "item_id": item_id_str,
        "write_latency_ms": doc.get("write_latency_ms"),
        "replication_delay_ms": round(replication_delay_ms, 3) if replication_delay_ms else None,
        "cross_machine_delay_ms": round(cross_machine_delay_ms, 3) if cross_machine_delay_ms else None,
        "secondary_read_latency_ms": round(secondary_read_ms, 3) if secondary_read_ms else None,
        "reader_machine_id": MACHINE_ID,
        "status": "ok" if replication_delay_ms else "timeout",
        "mode": "reader",
    }
    results.append(record)
    pdb["benchmark_results"].insert_one(dict(record))
    progress.advance(task)


def run_reader(run_id: str):
    from bson import ObjectId
    sdb = secondary_db()
    pdb = primary_db()

    console.print(Panel.fit(
        f"[bold white]CENG465 Benchmark — Reader Mode[/bold white]\n"
        f"[dim]run_id={run_id}  machine={MACHINE_ID}[/dim]\n"
        "[dim]Processing existing manifest entries + watching for new ones...[/dim]",
        border_style="blue"
    ))

    results = []
    processed = set()

    # Count total expected ops from manifest
    total_expected = pdb["benchmark_manifest"].count_documents({"run_id": run_id})
    console.print(f"[dim]Found {total_expected} existing manifest entries for run_id={run_id}[/dim]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TaskProgressColumn(), TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Reading from SECONDARY...", total=total_expected or None)

        # First: process all already-written manifest entries
        for doc in pdb["benchmark_manifest"].find({"run_id": run_id}).sort("seq", 1):
            _process_manifest_doc(doc, sdb, pdb, results, processed, progress, task)

        # Then: watch for any new entries (in case writer is still running)
        if len(processed) < total_expected or total_expected == 0:
            with pdb["benchmark_manifest"].watch(
                [{"$match": {"fullDocument.run_id": run_id, "operationType": "insert"}}],
                full_document="updateLookup",
            ) as stream:
                for event in stream:
                    doc = event.get("fullDocument")
                    if doc:
                        _process_manifest_doc(doc, sdb, pdb, results, processed, progress, task)
                    if total_expected and len(processed) >= total_expected:
                        break

    print_stats(results)
    generate_charts(results, run_id, mode="reader")


# ---------------------------------------------------------------------------
# Stats summary
# ---------------------------------------------------------------------------

def print_stats(results: list[dict], writer_only: bool = False):
    console.print()
    console.rule("[bold white]Benchmark Summary[/bold white]")

    total = len(results)
    timeouts = sum(1 for r in results if r["status"] == "timeout")
    errors = sum(1 for r in results if r["status"] == "error")

    delays = [r["replication_delay_ms"] for r in results if r.get("replication_delay_ms") is not None]
    writes = [r["write_latency_ms"] for r in results if r.get("write_latency_ms") is not None]

    t = Table(box=box.ROUNDED, border_style="white", expand=False)
    t.add_column("Metric", style="bold")
    t.add_column("Value", justify="right")

    t.add_row("Total ops", str(total))
    t.add_row("Timeouts", f"[red]{timeouts}[/red]" if timeouts else "0")
    t.add_row("Errors", f"[red]{errors}[/red]" if errors else "0")
    t.add_row("Success rate", f"{(total - timeouts - errors) / total * 100:.1f}%")

    if delays:
        delays_arr = np.array(delays)
        t.add_row("─" * 20, "─" * 12)
        t.add_row("Replication delay — min", f"{delays_arr.min():.2f} ms")
        t.add_row("Replication delay — p50", f"{np.percentile(delays_arr, 50):.2f} ms")
        t.add_row("Replication delay — p95", f"{np.percentile(delays_arr, 95):.2f} ms")
        t.add_row("Replication delay — p99", f"{np.percentile(delays_arr, 99):.2f} ms")
        t.add_row("Replication delay — max", f"{delays_arr.max():.2f} ms")
        t.add_row("Replication delay — avg", f"{delays_arr.mean():.2f} ms")
        t.add_row("Replication delay — std", f"{delays_arr.std():.2f} ms")

    if writes:
        writes_arr = np.array(writes)
        t.add_row("─" * 20, "─" * 12)
        t.add_row("Write latency — avg", f"{writes_arr.mean():.2f} ms")
        t.add_row("Write latency — p99", f"{np.percentile(writes_arr, 99):.2f} ms")
        t.add_row("Write latency — max", f"{writes_arr.max():.2f} ms")

    by_op = {}
    for r in results:
        op = r["op_type"]
        d = r.get("replication_delay_ms")
        if d is not None:
            by_op.setdefault(op, []).append(d)

    for op, vals in sorted(by_op.items()):
        arr = np.array(vals)
        t.add_row("─" * 20, "─" * 12)
        t.add_row(f"{op} count", str(len(vals)))
        t.add_row(f"{op} avg delay", f"{arr.mean():.2f} ms")
        t.add_row(f"{op} p95 delay", f"{np.percentile(arr, 95):.2f} ms")

    console.print(t)


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def generate_charts(results: list[dict], run_id: str, mode: str):
    if not results:
        return

    seqs = [r["seq"] for r in results]
    op_types = [r["op_type"] for r in results]
    delays = [r.get("replication_delay_ms") for r in results]
    writes = [r.get("write_latency_ms") for r in results]
    pri_reads = [r.get("primary_read_latency_ms") for r in results]
    sec_reads = [r.get("secondary_read_latency_ms") for r in results]
    statuses = [r["status"] for r in results]
    cross = [r.get("cross_machine_delay_ms") for r in results]

    valid_delays = [(s, d) for s, d in zip(seqs, delays) if d is not None]
    timeout_seqs = [s for s, st in zip(seqs, statuses) if st == "timeout"]

    fig = plt.figure(figsize=(20, 22))
    fig.suptitle(
        f"CENG465 — Replication Benchmark Results\n"
        f"run_id={run_id}  mode={mode}  ops={len(results)}  machine={MACHINE_ID}",
        fontsize=14, fontweight="bold", y=0.98
    )
    gs = gridspec.GridSpec(4, 2, figure=fig, hspace=0.45, wspace=0.35)

    # ── 1. Replication delay timeline ─────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, :])
    for op, color in OP_COLORS.items():
        xs = [s for s, d, o in [(s, d, op_types[i]) for i, (s, d) in enumerate(zip(seqs, delays))]
              if o == op and d is not None]
        ys = [d for s, d, o in [(s, d, op_types[i]) for i, (s, d) in enumerate(zip(seqs, delays))]
              if o == op and d is not None]
        ax1.scatter(xs, ys, s=6, alpha=0.7, color=color, label=op)

    if valid_delays:
        xs_all, ys_all = zip(*valid_delays)
        window = max(1, len(ys_all) // 50)
        moving_avg = np.convolve(ys_all, np.ones(window) / window, mode="valid")
        ax1.plot(xs_all[window - 1:], moving_avg, color="white", linewidth=1.5,
                 alpha=0.9, label=f"moving avg (w={window})")

    for ts in timeout_seqs:
        ax1.axvline(ts, color="red", alpha=0.4, linewidth=0.5)

    ax1.set_title("Replication Delay per Operation (red lines = timeouts)")
    ax1.set_xlabel("Operation Sequence")
    ax1.set_ylabel("Delay (ms)")
    ax1.legend(loc="upper right", fontsize=8)
    ax1.set_facecolor("#1a1a2e")
    ax1.grid(True, alpha=0.2)

    # ── 2. Histogram + CDF ────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[1, 0])
    d_vals = [d for d in delays if d is not None]
    if d_vals:
        ax2.hist(d_vals, bins=60, color="#9C27B0", edgecolor="none", alpha=0.8, label="count")
        ax2_r = ax2.twinx()
        sorted_d = np.sort(d_vals)
        cdf = np.arange(1, len(sorted_d) + 1) / len(sorted_d)
        ax2_r.plot(sorted_d, cdf, color="#FF9800", linewidth=2, label="CDF")
        ax2_r.set_ylabel("CDF", color="#FF9800")
        ax2_r.tick_params(axis="y", labelcolor="#FF9800")
        ax2_r.set_ylim(0, 1.05)
        p95 = np.percentile(d_vals, 95)
        p99 = np.percentile(d_vals, 99)
        ax2.axvline(p95, color="yellow", linestyle="--", linewidth=1, label=f"p95={p95:.1f}ms")
        ax2.axvline(p99, color="red", linestyle="--", linewidth=1, label=f"p99={p99:.1f}ms")
    ax2.set_title("Replication Delay Distribution + CDF")
    ax2.set_xlabel("Delay (ms)")
    ax2.set_ylabel("Count")
    ax2.legend(fontsize=8)

    # ── 3. Rolling percentiles ────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 1])
    if len(d_vals) >= 20:
        window_size = max(10, len(d_vals) // 20)
        p50s, p95s, p99s, xs_roll = [], [], [], []
        for i in range(window_size, len(d_vals) + 1):
            window_data = d_vals[i - window_size:i]
            p50s.append(np.percentile(window_data, 50))
            p95s.append(np.percentile(window_data, 95))
            p99s.append(np.percentile(window_data, 99))
            xs_roll.append(i)
        ax3.plot(xs_roll, p50s, label="p50", color="#4CAF50", linewidth=1.5)
        ax3.plot(xs_roll, p95s, label="p95", color="#FF9800", linewidth=1.5)
        ax3.plot(xs_roll, p99s, label="p99", color="#F44336", linewidth=1.5)
        ax3.fill_between(xs_roll, p50s, p95s, alpha=0.1, color="#FF9800")
        ax3.fill_between(xs_roll, p95s, p99s, alpha=0.1, color="#F44336")
    ax3.set_title(f"Rolling Percentiles (window={window_size if len(d_vals) >= 20 else 'N/A'})")
    ax3.set_xlabel("Op index")
    ax3.set_ylabel("Delay (ms)")
    ax3.legend(fontsize=8)
    ax3.grid(True, alpha=0.3)

    # ── 4. Op-type box plot ───────────────────────────────────────────────
    ax4 = fig.add_subplot(gs[2, 0])
    by_op_data = {}
    for op in ["insert", "update", "delete"]:
        vals = [d for d, o in zip(delays, op_types) if o == op and d is not None]
        if vals:
            by_op_data[op] = vals
    if by_op_data:
        bp = ax4.boxplot(
            list(by_op_data.values()),
            tick_labels=list(by_op_data.keys()),
            patch_artist=True,
            medianprops={"color": "white", "linewidth": 2},
        )
        for patch, op in zip(bp["boxes"], by_op_data.keys()):
            patch.set_facecolor(OP_COLORS[op])
            patch.set_alpha(0.7)
    ax4.set_title("Replication Delay by Operation Type")
    ax4.set_ylabel("Delay (ms)")
    ax4.grid(True, alpha=0.3, axis="y")

    # ── 5. Write latency timeline ─────────────────────────────────────────
    ax5 = fig.add_subplot(gs[2, 1])
    w_vals = [(s, w) for s, w in zip(seqs, writes) if w is not None]
    if w_vals:
        wx, wy = zip(*w_vals)
        ax5.scatter(wx, wy, s=4, alpha=0.6, color="#00BCD4", label="write latency")
        ww = max(1, len(wy) // 30)
        wma = np.convolve(wy, np.ones(ww) / ww, mode="valid")
        ax5.plot(wx[ww - 1:], wma, color="white", linewidth=1.5, label=f"avg (w={ww})")
    ax5.set_title("Write Latency to Primary (w=majority)")
    ax5.set_xlabel("Operation Sequence")
    ax5.set_ylabel("Latency (ms)")
    ax5.legend(fontsize=8)
    ax5.grid(True, alpha=0.3)

    # ── 6. Primary vs secondary read latency ─────────────────────────────
    ax6 = fig.add_subplot(gs[3, 0])
    pri_vals = [v for v in pri_reads if v is not None]
    sec_vals = [v for v in sec_reads if v is not None]
    cross_vals = [v for v in cross if v is not None]
    labels_box, data_box, colors_box = [], [], []
    if pri_vals:
        labels_box.append("Primary read")
        data_box.append(pri_vals)
        colors_box.append("#4CAF50")
    if sec_vals:
        labels_box.append("Secondary read")
        data_box.append(sec_vals)
        colors_box.append("#2196F3")
    if cross_vals:
        labels_box.append("Cross-machine delay")
        data_box.append(cross_vals)
        colors_box.append("#FF5722")
    if data_box:
        bp2 = ax6.boxplot(data_box, tick_labels=labels_box, patch_artist=True,
                          medianprops={"color": "white", "linewidth": 2})
        for patch, color in zip(bp2["boxes"], colors_box):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
    ax6.set_title("Read Latency: Primary vs Secondary vs Cross-Machine")
    ax6.set_ylabel("Latency (ms)")
    ax6.grid(True, alpha=0.3, axis="y")

    # ── 7. Cumulative timeout / error rate ────────────────────────────────
    ax7 = fig.add_subplot(gs[3, 1])
    cum_timeouts = np.cumsum([1 if s == "timeout" else 0 for s in statuses])
    cum_errors = np.cumsum([1 if s == "error" else 0 for s in statuses])
    ax7.plot(seqs, cum_timeouts, color="#FF9800", linewidth=2, label="cumulative timeouts")
    ax7.plot(seqs, cum_errors, color="#F44336", linewidth=2, label="cumulative errors")
    ax7.fill_between(seqs, cum_timeouts, alpha=0.15, color="#FF9800")
    timeout_rate = len(timeout_seqs) / len(results) * 100
    ax7.set_title(f"Cumulative Timeouts & Errors (timeout rate={timeout_rate:.1f}%)")
    ax7.set_xlabel("Operation Sequence")
    ax7.set_ylabel("Cumulative count")
    ax7.legend(fontsize=8)
    ax7.grid(True, alpha=0.3)

    path = f"benchmark_{run_id}_{mode}.png"
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    console.print(f"\n[bold green]Chart saved:[/bold green] {path}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CENG465 Replication Benchmark")
    parser.add_argument("--mode", choices=["single", "writer", "reader"], default="single")
    parser.add_argument("--ops", type=int, default=1000)
    parser.add_argument("--run-id", default=f"run_{uuid.uuid4().hex[:6]}")
    args = parser.parse_args()

    console.print(f"[dim]run-id: {args.run_id}[/dim]")

    if args.mode == "single":
        run_single(args.ops, args.run_id)
    elif args.mode == "writer":
        run_writer(args.ops, args.run_id)
    elif args.mode == "reader":
        run_reader(args.run_id)


if __name__ == "__main__":
    main()
