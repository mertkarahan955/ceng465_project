from operations import insert_item, update_item, delete_item
import db


def print_logs():
    logs = list(db.get_primary()["operation_logs"].find().sort("log_index", 1))
    print(f"\n{'='*60}")
    print(f"{'OP':6} {'TYPE':8} {'STATUS':22} {'DELAY':>10}")
    print(f"{'='*60}")
    for log in logs:
        delay = f"{log['replication_delay_ms']:.2f} ms" if log["replication_delay_ms"] is not None else "timeout"
        print(f"{log['log_index']:<6} {log['operation_type']:<8} {log['status']:<22} {delay:>10}")
    print(f"{'='*60}\n")


def main():
    print("=== CENG465 Replication Demo ===\n")

    print("[1] INSERT")
    item_id, delay = insert_item("sensor_1", {"temperature": 22.5, "unit": "C"})
    print(f"    inserted _id={item_id}, replication delay={delay:.2f} ms\n")

    print("[2] UPDATE")
    delay = update_item(item_id, {"temperature": 25.0, "unit": "C"})
    print(f"    updated, replication delay={delay:.2f} ms\n")

    print("[3] DELETE (soft)")
    delay = delete_item(item_id)
    print(f"    deleted, replication delay={delay:.2f} ms\n")

    print("[4] Operation Logs")
    print_logs()


if __name__ == "__main__":
    main()
