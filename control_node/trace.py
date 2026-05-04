"""
Live replication tracer — watch operation_logs in real time.

Run on PRIMARY:
    python trace.py

Run on SECONDARY (pass --secondary flag):
    python trace.py --secondary

Uses MongoDB change streams so events appear the moment they are
written (on primary) or replicated (on secondary).
"""

import argparse
import signal
import sys
from datetime import timezone

from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich import box

import config

console = Console()

OP_COLORS = {
    "insert": "green",
    "update": "blue",
    "delete": "red",
}

rows: list[dict] = []


def build_table() -> Table:
    t = Table(
        title="[bold]Live Replication Trace[/bold]  [dim](Ctrl+C to stop)[/dim]",
        box=box.ROUNDED,
        border_style="cyan",
        expand=True,
    )
    t.add_column("#", width=4, justify="right")
    t.add_column("Type", width=8)
    t.add_column("Status", width=22)
    t.add_column("ver", width=10)
    t.add_column("Delay", width=12, justify="right")
    t.add_column("Written on Leader", width=25)
    t.add_column("Visible on Follower", width=25)

    for r in rows:
        op = r["operation_type"]
        color = OP_COLORS.get(op, "white")
        delay = (
            f"{r['replication_delay_ms']:.2f} ms"
            if r["replication_delay_ms"] is not None
            else ("[yellow]pending[/yellow]" if r["status"] == "pending_follower" else "[red]timeout[/red]")
        )
        status_color = "green" if r["status"] == "visible_on_follower" else ("yellow" if r["status"] == "pending_follower" else "red")
        vb = str(r.get("version_before") or "—")
        va = str(r.get("version_after") or "—")

        leader_t = r["leader_write_time"].astimezone(timezone.utc).strftime("%H:%M:%S.%f")[:-3] if r.get("leader_write_time") else "—"
        follower_t = r["follower_visible_time"].astimezone(timezone.utc).strftime("%H:%M:%S.%f")[:-3] if r.get("follower_visible_time") else "[red]—[/red]"

        t.add_row(
            str(r["log_index"]),
            f"[{color}]{op}[/{color}]",
            f"[{status_color}]{r['status']}[/{status_color}]",
            f"{vb}→{va}",
            delay,
            leader_t,
            follower_t,
        )

    return t


def main():
    parser = argparse.ArgumentParser(description="Live replication tracer")
    parser.add_argument("--secondary", action="store_true", help="Connect via secondary host")
    args = parser.parse_args()

    if args.secondary:
        host = config.SECONDARY_HOST
        label = "SECONDARY"
        uri = f"mongodb://{host}:{config.PORT}/?replicaSet={config.REPLICA_SET}&readPreference=primaryPreferred"
    else:
        host = config.PRIMARY_HOST
        label = "PRIMARY"
        uri = config.PRIMARY_URI

    console.print(Panel.fit(
        f"[bold white]CENG465 Live Tracer[/bold white]  [dim]connected via {label} ({host})[/dim]\n"
        "[dim]Watching operation_logs for changes...[/dim]",
        border_style="cyan",
    ))

    client = MongoClient(uri)
    collection = client[config.DATABASE]["operation_logs"]

    # load existing logs first
    for doc in collection.find().sort("log_index", 1):
        rows.append(doc)

    def handle_sigint(sig, frame):
        console.print("\n[dim]Tracer stopped.[/dim]")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    with Live(build_table(), console=console, refresh_per_second=4, screen=False) as live:
        with collection.watch(
            [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}],
            full_document="updateLookup",
        ) as stream:
            for event in stream:
                doc = event.get("fullDocument")
                if doc:
                    for i, row in enumerate(rows):
                        if row.get("_id") == doc.get("_id"):
                            rows[i] = doc
                            break
                    else:
                        rows.append(doc)
                    rows.sort(key=lambda r: r.get("log_index", 0))
                    live.update(build_table())


if __name__ == "__main__":
    main()
