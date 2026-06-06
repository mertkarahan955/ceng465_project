"""
CENG465 Consistency Experiment Runner

Public entrypoint for the dashboard. Experiment implementations live under
`control_node/experiment/`; this file keeps result persistence and dispatching
stable for existing callers.
"""

import json
import os
from datetime import datetime

from experiment import (
    run_concurrent_writes,
    run_eventual_consistency,
    run_monotonic_reads,
    run_read_after_write,
    run_sync_replication,
)


# ── Paths ─────────────────────────────────────────────────────────────────────

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "experiment_results")


# ── Result persistence ─────────────────────────────────────────────────────────

def _save_result(name: str, result: dict) -> str:
    """Persist experiment result to disk. Returns the filename."""
    folder = os.path.join(RESULTS_DIR, name)
    os.makedirs(folder, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:20]
    filename = f"{ts}.json"
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"saved_at": datetime.now().isoformat(), **result}, f,
                  ensure_ascii=False, indent=2, default=str)
    return filename


def list_results(name: str) -> list:
    """List saved results for a given experiment, newest first."""
    folder = os.path.join(RESULTS_DIR, name)
    if not os.path.isdir(folder):
        return []
    files = sorted(
        [f for f in os.listdir(folder) if f.endswith(".json")],
        reverse=True,
    )
    out = []
    for fname in files[:20]:
        path = os.path.join(folder, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            out.append({
                "filename": fname,
                "saved_at": data.get("saved_at", ""),
                "summary":  data.get("summary", {}),
            })
        except Exception:
            pass
    return out


def load_result(name: str, filename: str) -> dict:
    """Load a specific saved result."""
    path = os.path.join(RESULTS_DIR, name, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Dispatcher ────────────────────────────────────────────────────────────────

def run_experiment(name):
    registry = {
        "sync_replication":     run_sync_replication,
        "eventual_consistency": run_eventual_consistency,
        "read_after_write":     run_read_after_write,
        "monotonic_reads":      run_monotonic_reads,
        "concurrent_writes":    run_concurrent_writes,
    }
    fn = registry.get(name)
    if not fn:
        raise ValueError(f"Unknown experiment: {name!r}")

    result = fn()

    # A save failure must not suppress a successful experiment result.
    try:
        _save_result(name, result)
    except OSError:
        pass

    return result
