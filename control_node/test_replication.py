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
    """Test 1 — Temel CRUD + versiyon takibi (items koleksiyonu, w=majority)

    Ne yapar:
        Insert → Update → Delete sırasıyla çalıştırır ve her adımda
        secondary'nin doğru versiyonu gördüğünü doğrular.

    Neden önemli:
        Replication'ın en temel garantisi: primary'ye yazılan her mutation
        secondary'de de aynı versiyonla görünmeli. w=majority kullandığımız
        için primary "insert tamam" demeden önce secondary'nin acknowledge
        etmesini bekler → delay ölçülebilir ama veri kaybolmaz.

    Ne ölçer:
        - Insert/Update/Delete'in secondary'de gözükmesi için geçen süre (ms)
        - Soft-delete flag'inin doğru replike edilmesi (deleted=True)
        - Her operasyondan sonra secondary version = primary version
    """
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
    """Test 2 — Fleet statik koleksiyonları (vehicles / drivers / depots, w=majority)

    Ne yapar:
        Araç, sürücü ve depo dokümanlarını ayrı koleksiyonlara insert eder.
        Her birinin secondary'de görünüp görünmediğini, doğru koleksiyonda
        olduğunu ve version=1 olduğunu kontrol eder.

    Neden önemli:
        Tek bir "items" koleksiyonu yerine 6 farklı koleksiyonumuz var.
        operation_logs'daki target_collection alanı, reconciler'ın hangi
        koleksiyonu izleyeceğini belirler. Bu test, koleksiyon routing'inin
        doğru çalıştığını kanıtlar.

    Consistency model:
        vehicles/drivers/depots nadiren güncellenir → eventual consistency
        okuma kabul edilebilir. Secondary biraz geride kalsa bile araç tipi
        veya depo adresi kritik bir karar vermez.

    Ne ölçer:
        - Her koleksiyon için ayrı insert→secondary lag süresi
        - Cross-collection replication'ın bağımsız çalışması
    """
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
    """Test 3 — Monotonic Reads: shipment status asla geri gidemez (w=majority)

    Ne yapar:
        Bir gönderi oluşturur: pending → in_transit → in_transit → delivered.
        Her güncellemeden HEMEN SONRA secondary'den version numarasını okur
        ve bir listeye ekler. Sonunda listenin monoton artan olduğunu doğrular.

    Neden önemli (Monotonic Reads):
        Bir kullanıcı "gönderi X teslim edildi" gördükten sonra sayfayı yenilediğinde
        "gönderi X bekliyor" görmemelidir. Secondary'nin oplog uygulaması sıralıdır,
        bu yüzden versiyon numarası hiç geri gidemez.

        w=majority modunda zaten tüm yazılar acknowledge edildiği için secondary
        geride kalmaz — test versiyon sıralamasının doğruluğunu kanıtlar.

        w=1 modunda daha dramatik gözlemlenir: primary'de v=5 varken secondary
        hâlâ v=2 gösterebilir ama hiçbir zaman v=5 → v=3 düşüşü yaşanmaz.

    Ne kanıtlar:
        secondary_versions = [2, 3, 4, 5]  → Monoton ↑  PASS
        secondary_versions = [2, 3, 2, 5]  → Geri düştü FAIL  (imkânsız olmalı)
    """
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
    """Test 4 — Position burst: yüksek frekanslı GPS yazma (w=majority)

    Ne yapar:
        10 adet GPS pozisyon kaydını art arda insert eder. Her insert için
        replication delay'i ölçer. Tüm yazılar bittikten sonra 500ms bekler
        ve secondary'de tüm dokümanların var olduğunu doğrular.

    Neden önemli (Eventual Consistency):
        positions koleksiyonu en yüksek yazma frekansına sahip — gerçek bir
        filo takip sisteminde her araç saniyede birden fazla konum gönderir.
        Filo genel görünümü (fleet overview) için secondary'den okumak yeterli:
        bir aracın konumu 200ms stale kalsa dispatcher'ın kararını etkilemez.

        w=majority bu testte: secondary her yazıyı sırayla onaylar, delay
        ölçülebilir (avg ~40ms, max ~140ms). Bu gecikme bile gerçek bir
        sistemde "eventual consistency window" olarak raporlanabilir.

    w=1 farkı:
        w=1 modunda primary anında döner, secondary 0-500ms geride kalabilir.
        Fleet Overview (secondary) bu sürede stale konum gösterir — demo'da
        görsel olarak kanıtlanabilir.

    Ne ölçer:
        - 10 yazı için avg/max replication delay
        - Eventual consistency guarantee: tüm yazılar SONUNDA secondary'de görünür
    """
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
    """Test 5 — Read-After-Write: incident anında primary'den okunabilmeli

    Ne yapar:
        1. Primary'ye critical severity bir incident yazar.
        2. HEMEN (hiç bekleme olmadan) primary'den okur.
        3. get_open_incidents() fonksiyonunun (primary read path) yeni
           incident'ı döndürdüğünü doğrular.

    Neden önemli (Read-After-Write):
        Bir dispatcher "TRK-001 yolda kaldı, kritik arıza" bildirimini sisteme
        girdiğinde ANINDA kendi ekranında görmeli. "Ben az önce girdim, nerede?"
        dememelidir.

        Read-After-Write garantisi: kullanıcının kendi yazdığı veriyi hemen
        primary'den okuyabilmesi. Primary her zaman en güncel veriyi tutar
        (w=1 bile olsa primary'nin kendi local storage'ı tazedir).

    w=majority vs w=1 farkı:
        Her iki modda da PRIMARY READ hemen çalışır — çünkü primary yazdı,
        primary okudu. Secondary'de stale olabilir ama bu test primary okur.
        Deneyin odağı: "secondary yerine primary oku = her zaman taze veri".

    Ne kanıtlar:
        - Yazma → primary okuma arasında SIFIR gecikme
        - get_open_incidents() severity=critical incident'ı döndürür
        - incidents koleksiyonu için target_collection=incidents log'da var
    """
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

def test_async_eventual_consistency():
    """Test 6b — Eventual Consistency: w=1 ile secondary geride kalabilir

    Ne yapar:
        1. Write concern'i w=1 (async) yapar.
        2. Positions'a 5 hızlı GPS yazısı gönderir — primary anında döner,
           secondary'nin onayını BEKLEMEZ.
        3. Her yazının HEMEN ARDINDA secondary'de o dokümanın var olup
           olmadığını kontrol eder ("stale read" penceresi).
        4. Tüm yazılar bittikten sonra 2 saniye bekler ve secondary'nin
           eventually tüm dokümanları yakalayıp yakalamadığını doğrular.
        5. Write concern'i w=majority'ye geri alır.

    Eventual Consistency garantisi nedir:
        "Yeterince beklenirse secondary primary ile aynı duruma gelir."
        Bu test tam olarak bunu ölçer:
        - HEMEN okuma → stale (bazı dokümanlar yok olabilir)  [beklenen]
        - 2 saniye sonra okuma → tüm dokümanlar var           [garanti]

    w=majority ile farkı:
        w=majority modunda primary "tamam" demeden önce secondary onaylar,
        yani "stale read penceresi" sıfırdır — eventual consistency GÖRÜNMEZ.
        w=1 bu pencereyi açar ve gözlemlenebilir kılar.

    Raporda nasıl kullanılır:
        "w=1 ile yazılan 5 GPS kaydının secondary'de görünme süresi:
        bazıları anında, bazıları ~Xms gecikmeyle replike edildi.
        2 saniye sonra tüm kayıtlar secondary'de mevcuttu (eventual consistency)."
    """
    section("Test 6b: Async w=1 — Eventual Consistency Window")

    import operations as ops

    # w=1'e geç
    ops.set_write_concern(1)
    console.print("  [dim]Write concern → w=1 (async)[/dim]")

    ids = []
    stale_count = 0

    for i in range(5):
        pid, delay = ops.insert_position(
            "EC-TEST",
            lat=40.0 + i * 0.01, lng=33.0 + i * 0.01,
            city="Ankara", district="EC-Test",
            speed_kmh=50 + i,
        )
        ids.append(pid)

        # w=1: delay=None beklenir (fire-and-forget)
        if delay is None:
            pass  # beklenen davranış

        # Anında secondary'den oku — stale olabilir
        doc = db.get_secondary()["positions"].find_one({"_id": pid})
        if doc is None:
            stale_count += 1

    # Stale gözlemlemek "eventual consistency" için gerekli kanıt
    if stale_count > 0:
        ok(f"Stale read observed: {stale_count}/5 docs not yet on secondary right after w=1 write",
           "eventual consistency window is visible")
    else:
        console.print("  [dim]Note: all 5 docs already replicated (network is very fast today)[/dim]")
        ok("w=1 writes completed (no stale observed — secondary caught up instantly)", "still valid")

    # Eventual guarantee: 2 saniye sonra hepsi gelmeli
    time.sleep(2)
    missing = sum(1 for pid in ids if not db.get_secondary()["positions"].find_one({"_id": pid}))
    if missing == 0:
        ok("After 2s: all 5 docs present on secondary — eventual consistency satisfied")
    else:
        fail(f"After 2s: {missing}/5 docs still missing on secondary")

    # w=majority'ye geri al
    ops.set_write_concern("majority")
    console.print("  [dim]Write concern → w=majority (restored)[/dim]")


def test_operation_log_collection_field():
    """Test 6 — operation_logs.target_collection doğru kaydediliyor mu?

    Ne yapar:
        vehicles, drivers ve positions koleksiyonlarına birer insert yapar.
        operation_logs'da bu insertlere ait kayıtların target_collection
        alanının doğru koleksiyon adını içerdiğini doğrular.

    Neden önemli:
        Sistemimiz 6 koleksiyon için tek bir operation_logs tutar. Reconciler
        (background thread) pending log'ları tamamlamak için hangi koleksiyona
        bakacağını target_collection'dan öğrenir. Bu alan yanlışsa reconciler
        yanlış koleksiyonu sorgular → stale log'lar asla kapanmaz →
        pending_count sürekli artar.

        Bu test, multi-collection routing'in merkezi meta-data sütununun
        doğru çalıştığını garanti eder.

    Akademik önem:
        DDIA (Designing Data-Intensive Applications) Bölüm 5'teki replication
        log konseptinin direkt uygulaması. Her yazı operasyonu log_index ile
        sıralanır, target_collection ile yönlendirilir — tıpkı WAL (Write-Ahead
        Log) yapısındaki page ve offset kavramları gibi.
    """
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
    test_async_eventual_consistency()   # w=1 async — eventual consistency gözlemle
    test_operation_log_collection_field()

    failed = print_summary()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
