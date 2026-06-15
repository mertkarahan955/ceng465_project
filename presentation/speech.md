# CENG 465 — Sunum Konuşma Metni
**Data Replication in a Single-Leader Environment**
Mert Karahan & Doğukan Topçu · İYTE · Haziran 2026

> **Notlar:**
> - `[SLAYT N]` → slayta geçiş
> - `[ARAYÜZ]` → tarayıcıda dashboard'u göster (http://localhost:5001)
> - `[KOD: dosya]` → kod editöründe ilgili dosyayı göster
> - **Kalın** = vurgu yap, yavaşla
> - *İtalik* = opsiyonel, süre kısa ise atlayabilirsin

---

## Giriş — Mert sunar [SLAYT 1]

Merhaba, ben Mert Karahan — yanımda Doğukan Topçu.

Bu dönem CENG 465 kapsamında yaptığımız proje; gerçek iki makine üzerinde kurduğumuz bir **MongoDB Replica Set** sistemi ve bu sistemin çeşitli tutarlılık davranışlarını ölçen bir Python kontrol düğümünden oluşuyor.

Projenin özü şu: Tek bir lider düğümünün veriyi nasıl çoğalttığını, istemcinin bunu nasıl gördüğünü ve farklı yazma kaygısı (write concern) ayarlarının sisteme ne fark yarattığını **canlı ölçümlerle** göstermek.

Sunumu iki parçada yürüteceğiz:
1. Mimari ve şema tasarımı — **Mert**
2. Deney sonuçları ve arayüz gösterimi — **Doğukan**

---

## Sistem Mimarisi — Mert sunar [SLAYT 2]

**[SLAYT 2]**

Sistemimiz üç bileşenden oluşuyor.

Soldaki **PRIMARY** düğümü — fiziksel makinemiz, hostname'i `mongo-primary.lan`. Tüm yazma operasyonları buraya geliyor. Sağda **SECONDARY** düğümü — `mongo-secondary.lan`, IP `192.168.88.70`. Bu düğüm `votes=0, priority=0` olarak yapılandırılmış; yani seçim sürecine katılmıyor, yalnızca veriyi kopyalıyor.

Ortada **Kontrol Düğümü** — Flask uygulaması, port 5001. Bu düğüm:
- Insert / Update / Delete operasyonlarını PRIMARY'ye gönderiyor,
- Her yazma işlemini `operation_logs` koleksiyonuna kayıt ediyor,
- SECONDARY'yi poll ederek veri ne zaman orada göründüğünü ölçüyor.

İki mod var:
- **Synchronous** — `w=majority`: PRIMARY, SECONDARY'den ACK aldıktan sonra istemciye `ok` dönüyor. Güvenli, ama daha yavaş.
- **Asynchronous** — `w=1`: PRIMARY hemen `ok` dönüyor, SECONDARY kendi oplog'unu asenkron uygulayarak yetişiyor.

**[KOD: control_node/config.py]**

Bağlantı ayarları burada. PRIMARY için normal replica set URI; SECONDARY için `directConnection=true` ve `readPreference=secondaryPreferred` — bu sayede SECONDARY, PRIMARY'nin önüne geçmeden doğrudan okunuyor.

**[KOD: control_node/db.py]**

İki ayrı MongoClient örneği var: biri PRIMARY'ye bağlı, diğeri SECONDARY'ye. Dashboard her sorguyu doğru düğüme yönlendiriyor.

---

## Şema Tasarımı — Mert sunar [SLAYT 3]

**[SLAYT 3]**

Bir filo yönetim senaryosu kurduk. Altı koleksiyon var: `vehicles`, `drivers`, `depots`, `shipments`, `positions`, `incidents`.

Tüm koleksiyonlar ortak bir **document envelope** kullanıyor:
- `key` ve `value` — mantıksal ID ve alan verisi
- `version` — her yazma işleminde artan monoton sayaç; SECONDARY senkronizasyonunun anahtar alanı bu
- `last_updated` — UTC timestamp
- `deleted` — soft-delete bayrağı; hiçbir veri fiziksel olarak silinmiyor

Bunun yanında her yazma işlemi için `operation_logs` koleksiyonuna bir kayıt ekleniyor. Bu kayıt; `leader_write_time`, `follower_visible_time` ve `replication_delay_ms` içeriyor. Yani replikasyon gecikmesi, sistemin **ölçtüğü** bir metrik — görünmez bir yan etki değil.

*[ER diyagramındaki şekle işaret et]*

`operation_logs`, tüm altı fleet koleksiyonuna `target_id` üzerinden bağlı. Her insert / update / delete tam olarak bir log kaydı üretiyor. Bu WAL benzeri bir yapı.

---

## Operasyon Logu & Tutarlılık Haritası — Mert sunar [SLAYT 4]

**[SLAYT 4]**

Bu slayt, koleksiyonlarımızın tutarlılık modeline göre nasıl sınıflandırıldığını gösteriyor.

Tasarım prensibi şu: **yazma sıklığı + kritiklik → tutarlılık modeli**.

- `positions` — GPS verisi, yüksek yazma hızı; birkaç saniyelik gecikme tolere edilebilir → **Eventual**
- `incidents` — araç arızası, güvenlik kritik; dispatcher kendi yazdığını anında görmeli → **Read-after-write**
- `shipments` — kargo durumu; hiçbir zaman v3'ten v2'ye geriye gitmemeli → **Monotonic reads**
- `vehicles`, `drivers`, `depots` — düşük yazma sıklığı → **Synchronous veya Eventual**

Sağdaki tabloda `operation_logs` alanlarının ne anlama geldiğini görüyorsunuz. `status` alanı özellikle önemli: bir log kaydı `pending_follower` başlar, sonra `visible_on_follower` ya da `timeout` olur. Arka planda çalışan bir reconciler worker, 5 saniye sonra hâlâ pending olan kayıtları yeniden kontrol ediyor.

---

## Deney Sonuçları — Doğukan sunar

---

### Deney 1: Senkron Replikasyon [SLAYT 5]

**[SLAYT 5]**

İlk deneyde `w=majority` modunda `positions` koleksiyonuna bir INSERT attık. Araç: `SYNC-EXP`, İstanbul / Beşiktaş.

**Sonuçlar:**
- Toplam yazma gecikmesi: **51.6 ms**
- Bu sürenin ~7ms'i ağ — istek PRIMARY'ye ulaşana kadar
- ~32ms'i SECONDARY'nin oplog'u uygulayıp ACK göndermesi için bekleme
- Geri kalan ağ gecikmesi ile client'a `ok` dönüyor

Kritik nokta: PRIMARY, SECONDARY'den ACK gelmeden istemciye `ok` dönmüyor. Bu da **sıfır stale read** anlamına geliyor. İki düğüm de istemci `ok` almadan önce tutarlı hâle geliyor.

**[ARAYÜZ — Experiments sayfası → Sync Replication → timeline görselleştirmesi]**

*[Timeline'ı göster: Client → Primary → oplog arrow → Secondary → ACK → ok]*

Bu timeline görselleştirmesini biz DDIA kitabındaki Şekil 5 tarzında yazdık. Her ok, gerçek ölçülen gecikme değerlerinden türetildi.

**[KOD: control_node/experiment/sync_replication.py]**

*[`run_sync_replication()` fonksiyonunu göster]* Ağ gecikmesi ölçülüyor, gerçek insert atılıyor, sonra zamanlamalar hesaplanıyor. `net_p` PRIMARY'ye, `net_s` SECONDARY'ye gidiş süresi.

---

### Deney 2: Eventual Consistency [SLAYT 6]

**[SLAYT 6]**

İkinci deneyde `w=1` — asenkron mod. SECONDARY'ye `secondaryDelaySecs=1` koyduk; böylece gecikme penceresi gözlemlenebilir hâle geldi.

**Sonuçlar:**
- Yazma süresi: **38.1 ms** — PRIMARY hemen döndü, SECONDARY'yi beklemedi
- PRIMARY okundu → **v2 anında görüldü** — lider her zaman kendi yazdığını görür
- SECONDARY t=240ms'de okundu → **hâlâ v1** — stale (Ankara'daki eski konum)
- SECONDARY t=743ms'de okundu → **hâlâ v1** — hâlâ stale
- SECONDARY t=1189ms'de okundu → **v2 görüldü** — yakınsadı (convergence)

3 secondary okumasının 2'si stale döndü. **Tutarsızlık penceresi: 1189 ms.**

Bu deney, eventual consistency'nin ne demek olduğunu sayısal olarak gösteriyor: sistem sonunda tutarlı, ama arada bir pencere var.

**[ARAYÜZ — Experiments → Eventual Consistency timeline'ı]**

*[Stale oku kırmızı, fresh oku yeşil göster]*

**[KOD: control_node/experiment/eventual_consistency.py — `_set_secondary_delay` fonksiyonu]**

`secondaryDelaySecs` parametresini replSet config üzerinden programatik olarak değiştiriyoruz. Deney bitince sıfırlıyoruz.

---

### Deney 3: Read-After-Write [SLAYT 7]

**[SLAYT 7]**

Üçüncü deney: `incidents` koleksiyonu. Araç arızası senaryosu — `RAW-EXP`, breakdown, severity critical.

`w=1` ile yazıldı — yani yazma 25.7 ms'de döndü. Soru şu: dispatcher kendi oluşturduğu incident'ı hemen görebiliyor mu?

**Sonuçlar:**
- PRIMARY'den okundu → **7.6 ms**, v1 görüldü ✓ — doğru yönlendirme
- SECONDARY'den okundu → **döküman yok** — t=33ms'de henüz replike olmamış
- SECONDARY t≈62ms'de yakınsadı

**Fix:** Yazma sonrası okuma PRIMARY'ye yönlendirilmeli. Bu, `w=1`'in asenkron hızını korurken `read-after-write` garantisini sağlıyor.

Kodumuzda bu ayrım şöyle yapıldı:

**[KOD: control_node/app.py — `/api/fleet/current-position` ve `/api/fleet/open-incidents` endpoint'leri]**

`current-position` ve `open-incidents` — her ikisi de PRIMARY'den okuyor. `fleet/overview` ise SECONDARY'den — orada eventual consistency kabul edilebilir.

---

### Deney 4: Monotonic Reads [SLAYT 8]

**[SLAYT 8]**

Dördüncü deney, DDIA Şekil 5-4'e karşılık geliyor. `shipments` koleksiyonu — `MON-EXP` kargo, DEP-IST → DEP-ANK.

**Senaryo:** İki eşzamanlı thread — **Writer** ve **Reader**. Writer, `w=1` ile v1 → v2 → v3 yazdı. Reader, SECONDARY'den 5 ardışık okuma yaptı.

**Reader'ın gördükleri:** `[v1, v1, v2, v2, v2]`

- v1'i iki kez gördü — v2 henüz replike olmamış
- v2'ye geçti ve orada kaldı — v3 okuma süresi dolmadan gelmedi
- **En önemli şey: hiç geriye gitmedi.** v2 gördükten sonra v1 dönmedi.

**Monotonic reads garantisi sağlandı: 0 geriye gidiş.**

Tek lider mimarisi bunu nasıl garanti ediyor? PRIMARY oplog'a sıralı log_index atar. SECONDARY bu oplog'u aynı sırada uygular. Yani SECONDARY'de version sırası hiç bozulmuyor.

**[KOD: control_node/experiment/monotonic_reads.py — thread setup]**

`READ_GAP_MS=20`, `WRITE_DELAY_MS=8`, `WRITE_GAP_MS=40` — interleaved timeline burada tanımlanıyor.

---

### Deney 5: Concurrent Writes — Propagation Order [SLAYT 9]

**[SLAYT 9]**

Beşinci deney: iki kullanıcı aynı anda aynı dokümanı güncellerse ne olur?

`vehicles` koleksiyonu — `CW-SHARED`, tek bir paylaşımlı kamyon. User A ve User B eşzamanlı 2'şer yazma atti. Toplam 4 write, `w=1`.

**PRIMARY'nin sıralaması:** A1 (v2) → B1 (v3) → A2 (v4) → B2 (v5)

Bu, arrival order — kim önce PRIMARY'ye ulaştıysa o önce sıralandı. SECONDARY, oplog'u **aynı sırayla** uyguladı. Her iki düğüm de yaklaşık **2.5 ms** içinde v5'te birleşti.

**Last-Write-Wins:** B2 en son PRIMARY'ye ulaştı → en yüksek `log_index`'i aldı → her iki düğümde de `last_writer: user_b` ile sonuçlandı. LWW, tek lider serileştirmesi altında deterministic — çakışma yok.

**[ARAYÜZ — Experiments → Concurrent Writes]**

*[4 okun sıralı olarak geldiğini ve her iki düğümün v5'te birleştiğini göster]*

---

## Canlı Demo — Doğukan yönetir

**[ARAYÜZ — http://localhost:5001 ana sayfa]**

Dashboard'u kısaca gösterelim.

Üstte PRIMARY ve SECONDARY bağlantı durumu görüyorsunuz — yeşil olmalı. Sol tarafta Write Concern seçici var: `majority` veya `1` arasında geçiş yapabiliyoruz.

*[Bir araç seç, Update at, replication_delay_ms'in canlı güncellenmesini göster]*

**[ARAYÜZ — Operation Logs tablosu]**

Her yazma işleminin logu burada: `log_index`, `operation_type`, `status`, `replication_delay_ms`, `version_before / version_after`, hangi `write_concern` kullanıldığı.

`pending_follower` → `visible_on_follower` geçişini canlı gözlemleyebilirsiniz. `w=1` modunda yazma `ok` döndükten sonra arka planda reconciler devreye giriyor ve SECONDARY'de gördüğü anda status'ü güncelliyor.

*[Bir Insert yap, logun önce pending, sonra visible olduğunu göster]*

---

## Sonuç [SLAYT 10]

**[SLAYT 10]**

Beş tutarlılık senaryosunu gerçek iki makine üzerinde ölçtük — container yok, simüle gecikme yok.

| # | Deney | Önemli Bulgu |
|---|-------|--------------|
| 1 | Synchronous Replication | 51.6 ms toplam — istemci `ok` almadan her iki düğüm tutarlı |
| 2 | Eventual Consistency | 2/3 okuma stale; 1163 ms'de yakınsama (1 s yapay gecikme) |
| 3 | Read-After-Write | PRIMARY okuma 7.6 ms — her zaman fresh; SECONDARY geçici olarak stale |
| 4 | Monotonic Reads | `[v1,v1,v2,v2,v2]` — hiç geriye gitme yok |
| 5 | Concurrent Writes | `log_index` korundu — yazma sırası SECONDARY'de birebir aynı |

En büyük design kararımız: `operation_logs` koleksiyonunun replikasyon gecikmesini **birinci sınıf bir ölçüm** haline getirmesi. Gecikme, görünmez bir yan etki değil — her yazma işleminde kayıt altına alınan bir metrik.

Sorularınızı alabiliriz. Teşekkürler.

---

## Soru-Cevap İçin Hazır Cevaplar

**S: Neden Docker kullanmadınız?**
> Proje şartı gerçek dağıtık ortam gerektiriyordu — iki ayrı fiziksel makine, aynı LAN. Docker tek makine üzerinde çalışıyor, bu şartı karşılamıyor.

**S: `directConnection=true` ne anlama geliyor?**
> Normal MongoDB istemcisi, replica set topolojisini keşfedip okuma isteğini otomatik PRIMARY'ye yönlendirebilir. `directConnection=true` ile SECONDARY'ye zorla bağlanıyoruz; böylece stale okuma davranışını kontrol edebiliyoruz.

**S: Monotonic reads MongoDB tarafından otomatik garanti ediliyor mu?**
> Tek lider oplog sıralaması sayesinde SECONDARY'de version regresyonu pratik olarak mümkün değil. Ancak aynı istemcinin farklı SECONDARY örneklerine bağlandığı bir senaryoda garanti bozulabilir. Bizim setup'ımızda tek SECONDARY olduğu için sorun yok.

**S: `operation_logs` production sistemde nasıl ölçeklenir?**
> Şu haliyle her write için bir log kaydı var — yüksek yazma trafiğinde bu tablo büyüyebilir. Production'da TTL index veya log arşivleme eklenebilir. Bu projede gözlem amaçlı kullandığımız için kapsam dışında.

**S: Write concern neden `majority` ve `1` dışında seçenek sunmadınız?**
> `majority` ve `1` iki uç noktayı temsil ediyor: tam senkron vs. tam asenkron. Ara değerler (örn. `w=2`) üç veya daha fazla düğümlü setup'larda anlamlı; bizim iki düğümlü yapımızda `majority` zaten her iki düğümü de kapsıyor.
