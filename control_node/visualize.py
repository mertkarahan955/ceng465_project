import time
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich import box
from bson import ObjectId

import db
from operations import insert_item, update_item, delete_item

console = Console()


def fetch_item_primary(item_id):
    return db.get_primary()["items"].find_one({"_id": item_id})


def fetch_item_secondary(item_id):
    return db.get_secondary()["items"].find_one({"_id": item_id})


def doc_table(doc, title, color):
    t = Table(title=title, box=box.ROUNDED, border_style=color, title_style=f"bold {color}", min_width=38)
    t.add_column("Field", style="dim", width=18)
    t.add_column("Value")
    if doc is None:
        t.add_row("—", "[red]Not visible yet[/red]")
        return t
    t.add_row("key", str(doc.get("key", "")))
    t.add_row("value", str(doc.get("value", "")))
    t.add_row("version", str(doc.get("version", "")))
    t.add_row("deleted", str(doc.get("deleted", "")))
    t.add_row("leader_term", str(doc.get("leader_term", "")))
    t.add_row("log_index", str(doc.get("last_log_index", "")))
    t.add_row("operation_id", str(doc.get("last_operation_id", ""))[:16] + "...")
    t.add_row("last_updated", str(doc.get("last_updated", ""))[:23])
    return t


def show_comparison(item_id, op_label, delay_ms):
    primary_doc = fetch_item_primary(item_id)
    secondary_doc = fetch_item_secondary(item_id)

    delay_str = f"{delay_ms:.2f} ms" if delay_ms is not None else "timeout"
    status = "[green]visible_on_follower[/green]" if delay_ms is not None else "[red]timeout[/red]"

    console.print()
    console.rule(f"[bold yellow]{op_label}[/bold yellow]")
    console.print(f"  Replication delay: [bold cyan]{delay_str}[/bold cyan]   Status: {status}")
    console.print()

    primary_table = doc_table(primary_doc, "PRIMARY (Leader)", "green")
    secondary_table = doc_table(secondary_doc, "SECONDARY (Follower)", "blue")
    console.print(Columns([primary_table, secondary_table], equal=True, expand=False))


def show_logs():
    logs = list(db.get_primary()["operation_logs"].find().sort("log_index", 1))
    t = Table(title="Operation Logs", box=box.ROUNDED, border_style="magenta", title_style="bold magenta")
    t.add_column("#", width=4)
    t.add_column("Type", width=8)
    t.add_column("Status", width=22)
    t.add_column("ver before→after", width=16)
    t.add_column("Delay", justify="right", width=12)

    for log in logs:
        delay = f"{log['replication_delay_ms']:.2f} ms" if log["replication_delay_ms"] is not None else "[red]timeout[/red]"
        vb = str(log.get("version_before") or "—")
        va = str(log.get("version_after") or "—")
        status_color = "green" if log["status"] == "visible_on_follower" else "red"
        t.add_row(
            str(log["log_index"]),
            log["operation_type"],
            f"[{status_color}]{log['status']}[/{status_color}]",
            f"{vb} → {va}",
            delay,
        )
    console.print()
    console.rule("[bold magenta]Operation Log Summary[/bold magenta]")
    console.print(t)


def save_chart(logs):
    op_types = [l["operation_type"] for l in logs]
    delays = [l["replication_delay_ms"] or 0 for l in logs]
    indices = [l["log_index"] for l in logs]
    colors = {"insert": "#4CAF50", "update": "#2196F3", "delete": "#FF5722"}

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("MongoDB Single-Leader Replication — Delay Analysis", fontsize=14, fontweight="bold")

    # Bar chart
    bar_colors = [colors[op] for op in op_types]
    axes[0].bar([f"#{i}\n{op}" for i, op in zip(indices, op_types)], delays, color=bar_colors, edgecolor="white", linewidth=0.8)
    axes[0].set_title("Replication Delay per Operation")
    axes[0].set_ylabel("Delay (ms)")
    axes[0].set_xlabel("Operation")
    for i, (d, c) in enumerate(zip(delays, bar_colors)):
        axes[0].text(i, d + 1, f"{d:.1f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axes[0].set_ylim(0, max(delays) * 1.25 + 10)

    # Line chart
    axes[1].plot(indices, delays, marker="o", linewidth=2, color="#9C27B0", markersize=8)
    for i, (idx, d, op) in enumerate(zip(indices, delays, op_types)):
        axes[1].annotate(f"{d:.1f} ms\n({op})", (idx, d),
                         textcoords="offset points", xytext=(0, 12),
                         ha="center", fontsize=8, color=colors[op])
    axes[1].set_title("Delay Over Time (by Log Index)")
    axes[1].set_ylabel("Delay (ms)")
    axes[1].set_xlabel("Log Index")
    axes[1].set_xticks(indices)
    axes[1].set_ylim(0, max(delays) * 1.3 + 10)
    axes[1].grid(True, linestyle="--", alpha=0.5)

    patches = [mpatches.Patch(color=c, label=op) for op, c in colors.items()]
    fig.legend(handles=patches, loc="lower center", ncol=3, fontsize=10, frameon=False)

    plt.tight_layout(rect=[0, 0.06, 1, 1])
    path = "replication_delays.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return path


def main():
    console.print(Panel.fit(
        "[bold white]CENG465 — Data Replication Demo[/bold white]\n"
        "[dim]Single-Leader Replication with MongoDB Replica Set[/dim]",
        border_style="yellow"
    ))

    console.print("\n[bold]Running operations on PRIMARY → polling SECONDARY for visibility...[/bold]\n")

    item_id, delay = insert_item("sensor_1", {"temperature": 22.5, "unit": "C"})
    show_comparison(item_id, "INSERT", delay)

    delay = update_item(item_id, {"temperature": 25.0, "unit": "C"})
    show_comparison(item_id, "UPDATE", delay)

    delay = delete_item(item_id)
    show_comparison(item_id, "SOFT DELETE", delay)

    show_logs()

    logs = list(db.get_primary()["operation_logs"].find().sort("log_index", 1))
    chart_path = save_chart(logs)
    console.print(f"\n[bold green]Chart saved:[/bold green] {chart_path}\n")


if __name__ == "__main__":
    main()
