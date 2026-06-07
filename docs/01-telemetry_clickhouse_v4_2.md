# Telemetry Normalization Schema — ClickHouse v4.2

> **Định dạng:** Markdown (kèm khối DDL `sql` chạy được).
> **TARGET:** single-node ClickHouse (Docker test env). DDL dùng `MergeTree` / `ReplacingMergeTree`, KHÔNG `ON CLUSTER`, KHÔNG `Replicated*`.

**Để chuyển sang cluster (prod, nhiều shard/replica):**

1. Config macros `{cluster}`, `{shard}`, `{replica}` ở mỗi node CH (`/etc/clickhouse-server/config.d/macros.xml`).
2. Thêm `ON CLUSTER '{cluster}'` vào mỗi `CREATE`.
3. Đổi `MergeTree()` → `ReplicatedMergeTree('/clickhouse/tables/{shard}/ipms/<table>', '{replica}')`.
4. Đổi `ReplacingMergeTree(updated_at)` → `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ipms/<table>', '{replica}', updated_at)`.

---

## Changelog

| Phiên bản | Thay đổi |
| --------- | -------- |
| **v4.2** | **[REMOVED]** Bỏ cột `revision` trong 4 bảng mirror `ipms.tlm_path_aliases`, `ipms.tlm_metric_aliases`, `ipms.tlm_label_aliases`, `ipms.tlm_filter_rules` (đồng bộ với MariaDB v4.2 đã bỏ `revision` và `tlm_revision_counter`). **[CHANGED]** Version key của `ReplacingMergeTree` chuyển từ `revision` → `updated_at` (`DateTime64(3)`). Vì `updated_at` do sync job đặt = thời điểm INSERT (tăng đơn điệu theo mỗi lần mirror một row), nó giữ đúng ngữ nghĩa "bản mới thắng bản cũ" mà `revision` đảm nhiệm trước đây. **[CHANGED]** PHỤ LỤC A: query `argMax`/`FINAL` dùng `updated_at` thay cho `revision`. |
| v4.1 | **[MODIFIED]** `ipms.tlm_metrics`: thêm cột `device_name` (`LowCardinality(String)`) — denormalized từ `cat_device.device_name` tại ingest time bởi Flink. Tránh JOIN `cat_device` tốn kém; ~300 giá trị distinct → LowCardinality gần như miễn phí về storage và cho phép `WHERE device_name = '...'` không cần JOIN. `ORDER BY` giữ nguyên. Cập nhật PHỤ LỤC B. |
| v4.0 | **[ADDED]** `CREATE DATABASE ipms`; `ipms.tlm_metrics_raw` (TTL 7 ngày); 4 bảng MIRROR rule (`ReplacingMergeTree` theo `revision`) đồng bộ từ MariaDB. **[REF]** `ipms.cat_device` do hệ thống ngoài quản lý — chỉ ghi chú reference. |

---

## Triết lý thiết kế

1. KHÔNG dùng ENUM ở MariaDB — TINYINT UNSIGNED + COMMENT + CHECK.
2. KHÔNG có FOREIGN KEY — logic-layer FK, app layer giữ integrity.
3. Mỗi loại rule là một bảng nguồn-duy-nhất riêng (đã bỏ bảng denormalized `flink_rules`).
4. **v4.2: version đơn điệu bằng `updated_at`** (thay `revision` đã bỏ), KHÔNG reset/decrement.
5. `transform_kind` / `lv_kind` là DISPATCHER — Flink switch theo giá trị.
6. `vendor_code` là String, `"All"` = áp mọi vendor.

---

## Giải thích Label Rename — tại sao cần và phạm vi

Label (tag) là các cặp key→value đi kèm mỗi metric, ví dụ:

```text
Cisco:   {"interface_name":"GigabitEthernet0/0/0/0","vrf":"default"}
Juniper: {"interface":"xe-0/0/0.0","routing_instance":"default"}
Nokia:   {"if-name":"1/1/1","vrf-name":"Base"}
```

Vấn đề: cùng khái niệm "tên interface" nhưng 3 vendor dùng 3 key khác nhau → dashboard/alert không thể query thống nhất.

Label rename gồm hai tầng độc lập:

- **TẦNG 1 — Key rename** (bắt buộc): đổi tên key thô → key chuẩn (`interface_name`/`interface`/`if-name` → `if_name`). Sau bước này `labels["if_name"]` tồn tại ở mọi vendor.
- **TẦNG 2 — Value normalize** (tuỳ chọn): chuẩn hoá value sau khi rename key.
  - `lv_kind=1` (REGEX): value có pattern đều đặn, vd Juniper `"xe-0/0/0.0"` với `pattern="(\S+)\.\d+$"`, `replace="$1"` → `"xe-0/0/0"`.
  - `lv_kind=2` (ENUM_MAPPING): value là tập hữu hạn, vd `{"Gi0/0/0/0":"GigabitEthernet0/0/0/0"}`.

**Phạm vi:** hỗ trợ rename key + normalize value từng key riêng lẻ. KHÔNG hỗ trợ tổ hợp nhiều key, KHÔNG thêm/xoá label. Scope: vendor-specific hoặc `"All"`; tuỳ chọn thêm `path_alias_id` scope.

---

## (0) DATABASE — idempotent

```sql
CREATE DATABASE IF NOT EXISTS ipms;
```

---

## (REF) `ipms.cat_device` — KHÔNG ĐỊNH NGHĨA Ở ĐÂY

Bảng `cat_device` do hệ thống ngoài (catalog device) quản lý, đã tồn tại ở cả MariaDB lẫn ClickHouse. Project IPMS-Telemetry chỉ tham chiếu logic-FK qua `device_id`. Schema CH (tham khảo, không chạy lại):

```sql
-- CREATE TABLE IF NOT EXISTS ipms.cat_device (
--     device_id Int32, device_code String, device_name String, device_ip String,
--     device_type_id Int32, network_id Int32, vendor_id Int32, station_id Int32,
--     department_id Int32, location_id Int32, serial String, status Int32,
--     bits Int32, table_syslog String, table_counter String, table_counter_custom String,
--     module String, custom_script String, node_code String, monitor Int32,
--     syslog_time String, rescan_time String, ip_loopback String, type_srt String,
--     sync_time String, sync_version_id Int32, snmp_community String,
--     province_code String, area_code String, monitor_segment String
-- ) ENGINE = Log;
```

> **LƯU Ý:** `cat_device.device_id` là `Int32` (có dấu) còn `tlm_metrics.device_id` là `UInt32`. JOIN dùng `toInt32(m.device_id) = d.device_id`.

---

## (A) `ipms.tlm_metrics` — v4.1: thêm device_name denormalized

**Thiết kế quyết định — tại sao `device_name` mà không phải `device_ip`:**

- `device_ip` thay đổi theo thời gian (re-addressing) → không bake vào hàng trăm triệu row; Flink đã dùng IP để lookup `device_id`, sau đó IP không cần lưu nữa.
- `device_name` là identifier con người dùng khi query (`"HAN-PE-01"`), cardinality thấp (~300 giá trị) → LowCardinality dictionary encoding gần như miễn phí.
- `device_id` giữ nguyên cho exact JOIN / integrity; `device_name` cho human-readable filter không cần JOIN.
- Nếu `device_name` đổi: hàng cũ giữ tên cũ (point-in-time correctness). Backfill bằng `ALTER TABLE ... UPDATE` nếu cần.

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_metrics
(
    event_time      DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'Thời điểm router lấy mẫu. Parser chuẩn hoá về mili-giây UTC.',
    receive_time    DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'Thời điểm Flink consume message từ Kafka. (receive_time - event_time) = tổng lag.',

    -- ── Device identification ─────────────────────────────────────────
    device_id       UInt32
        COMMENT 'Logic-FK tới cat_device.device_id (Int32 ở cat_device — JOIN dùng toInt32).
                 0 = router chưa đăng ký trong cat_device.',

    device_name     LowCardinality(String)
        COMMENT 'Denormalized từ cat_device.device_name tại ingest time bởi Flink.
                 Cardinality thấp (~300 router) → LowCardinality dictionary encoding.
                 Dùng trực tiếp trong WHERE / GROUP BY mà KHÔNG cần JOIN cat_device.
                 Rỗng ("") khi device_id=0. KHÔNG đưa vào ORDER BY — device_id đã đủ.',
    -- ─────────────────────────────────────────────────────────────────

    path_id         UInt32
        COMMENT '= tlm_path_aliases.id trong MariaDB / mirror CH. 0 = không có PathRename rule khớp.',

    raw_path        String
        COMMENT 'Sensor path thô trước PathRename. LƯU LUÔN.',
    raw_metric_name String
        COMMENT 'Tên metric thô trước FieldRename. LƯU LUÔN.',

    metric_name     LowCardinality(String)
        COMMENT 'Tên metric sau FieldRename. Fallback = raw_metric_name nếu không có rule.',

    value_type      UInt8
        COMMENT '1=number (đọc value_number), 2=string (đọc value_string), 3=bool (đọc value_bool).',

    unit            LowCardinality(String)
        COMMENT 'Đơn vị sau chuẩn hoá (UCUM). Rỗng "" khi value_type∈{2,3}. Copy từ metric_aliases.target_unit.',

    value_number    Float64
        COMMENT 'Giá trị số đã transform. Đọc khi value_type=1.',
    value_string    String
        COMMENT 'Giá trị string sau enum_mapping. Đọc khi value_type=2.',
    value_bool      Bool
        COMMENT 'Giá trị bool sau enum_mapping. Đọc khi value_type=3.',

    labels          Map(LowCardinality(String), String)
        COMMENT 'Tag key→value đi kèm metric. Flink apply LabelKeyRename trước khi ghi — keys đã là tên chuẩn.',

    INDEX idx_device_id   device_id         TYPE minmax             GRANULARITY 4,
    INDEX idx_path_id     path_id           TYPE minmax             GRANULARITY 4,
    INDEX idx_device_name device_name       TYPE set(512)           GRANULARITY 4,
    INDEX idx_label_keys  mapKeys(labels)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_label_vals  mapValues(labels) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
-- metric_name đứng đầu → hiệu quả cho alert/threshold scan toàn fleet.
-- device_id thứ hai → range scan per-device trong cùng metric.
-- device_name KHÔNG trong ORDER BY (redundant với device_id).
ORDER BY (metric_name, device_id, path_id, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

---

## (B) `ipms.tlm_metrics_raw` — RAW PAYLOAD (TTL 7 ngày)

Lưu payload Telegraf nguyên bản (JSON text) để debug parser / replay khi rule sai. Append-only, drop tự động sau 7 ngày qua TTL.

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_metrics_raw
(
    ingest_time  DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'Thời điểm Flink nhận message từ Kafka.',
    raw_payload  String
        COMMENT 'Payload thô từ Telegraf, JSON text. Query bằng JSONExtract*().'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingest_time)
ORDER BY ingest_time
TTL toDateTime(ingest_time) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
```

---

## (C) `ipms.tlm_path_aliases` — MIRROR của MariaDB.tlm_path_aliases

Sync job: `SELECT MariaDB.tlm_path_aliases WHERE pushed_at IS NULL` → INSERT vào đây → `UPDATE pushed_at = NOW(6)` ở MariaDB.

> **v4.2:** `ReplacingMergeTree(updated_at)` — cùng `id`, `updated_at` lớn hơn thắng (thay `revision` đã bỏ). `updated_at` = thời điểm sync job INSERT, tăng đơn điệu theo mỗi lần mirror.

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_path_aliases
(
    id              UInt32,
    vendor_code     LowCardinality(String),
    original_path   String,
    alias_path      String,
    status          UInt8
        COMMENT '1=Active, 0=Deprecated.',
    updated_at      DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'v4.2: version key cho ReplacingMergeTree dedup. = thời điểm sync job INSERT row này (KHÔNG phải MariaDB.updated_at gốc).'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
PARTITION BY tuple()
SETTINGS index_granularity = 8192;
```

---

## (D) `ipms.tlm_metric_aliases` — MIRROR của MariaDB.tlm_metric_aliases

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_metric_aliases
(
    id                      UInt32,
    vendor_code             LowCardinality(String),
    path_alias_id           Nullable(UInt32)
        COMMENT 'NULL = any-path scope; NOT NULL = path-scoped (FK logic-layer tới tlm_path_aliases.id).',
    original_name           String,
    alias_metric            LowCardinality(String),
    value_type_override     Nullable(UInt8)
        COMMENT '1=number, 2=string, 3=bool. NULL = giữ kiểu thô.',
    source_unit             LowCardinality(String)
        COMMENT 'Đơn vị thô (UCUM). Rỗng khi không áp dụng.',
    target_unit             LowCardinality(String)
        COMMENT 'Đơn vị sau chuẩn hoá (UCUM). Copy xuống tlm_metrics.unit.',
    transform_kind          UInt8
        COMMENT '0=identity, 1=linear, 2=expression, 3=enum_mapping.',
    scale_factor            Float64,
    offset_value            Float64,
    transform_expression    String
        COMMENT 'NOT empty khi transform_kind=2.',
    enum_mapping            String
        COMMENT 'JSON text khi transform_kind=3.',
    priority                UInt16
        COMMENT 'Cao hơn thắng khi nhiều rule match.',
    status                  UInt8,
    updated_at              DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'v4.2: version key cho ReplacingMergeTree dedup. = thời điểm sync job INSERT.'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
PARTITION BY tuple()
SETTINGS index_granularity = 8192;
```

---

## (E) `ipms.tlm_label_aliases` — MIRROR của MariaDB.tlm_label_aliases

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_label_aliases
(
    id              UInt32,
    vendor_code     LowCardinality(String),
    path_alias_id   Nullable(UInt32),
    original_key    String,
    alias_key       String,
    lv_kind         UInt8
        COMMENT '0=identity, 1=regex, 2=enum_mapping.',
    lv_pattern      String
        COMMENT 'NOT empty khi lv_kind=1.',
    lv_replace      String
        COMMENT 'NOT empty khi lv_kind=1.',
    lv_mapping      String
        COMMENT 'JSON text khi lv_kind=2.',
    priority        UInt16,
    status          UInt8,
    updated_at      DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'v4.2: version key cho ReplacingMergeTree dedup. = thời điểm sync job INSERT.'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
PARTITION BY tuple()
SETTINGS index_granularity = 8192;
```

---

## (F) `ipms.tlm_filter_rules` — MIRROR của MariaDB.tlm_filter_rules

```sql
CREATE TABLE IF NOT EXISTS ipms.tlm_filter_rules
(
    id                  UInt32,
    vendor_code         LowCardinality(String),
    match_path          String
        COMMENT 'Path scope sau PathRename. Rỗng = áp cho mọi path.',
    match_metric        String
        COMMENT 'Metric name để lọc. Rỗng = lọc theo path (mọi metric).',
    filter_expression   String
        COMMENT 'Biểu thức predicate. Rỗng = unconditional (luôn TRUE).',
    filter_action       UInt8
        COMMENT '0=EXCLUDE_IF_MATCH (drop khi predicate TRUE), 1=INCLUDE_IF_MATCH (giữ khi predicate TRUE).',
    priority            UInt16,
    status              UInt8,
    updated_at          DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'v4.2: version key cho ReplacingMergeTree dedup. = thời điểm sync job INSERT.'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
PARTITION BY tuple()
SETTINGS index_granularity = 8192;
```

---

## Phụ lục A — Query patterns trên bảng ReplacingMergeTree

Bản chất: `ReplacingMergeTree` merge eventually; query có thể vẫn thấy bản cũ + mới cùng lúc cho tới khi background merge chạy. 2 cách lấy snapshot rule hiện tại (v4.2 dùng `updated_at` làm version):

```sql
-- 1) FINAL modifier (đơn giản, perf trung bình):
SELECT * FROM ipms.tlm_path_aliases FINAL WHERE status = 1;

-- 2) argMax() group by (perf tốt, tránh quét full-merge) — dùng updated_at:
SELECT id,
       argMax(alias_path,  updated_at) AS alias_path,
       argMax(vendor_code, updated_at) AS vendor_code,
       argMax(status,      updated_at) AS status
  FROM ipms.tlm_path_aliases
 GROUP BY id
HAVING status = 1;

-- 3) Force merge (chỉ chạy thủ công khi cần):
OPTIMIZE TABLE ipms.tlm_path_aliases FINAL;
```

---

## Phụ lục B — Query patterns trên tlm_metrics (v4.1)

**B.1) Trường hợp phổ biến — filter/display theo device, không cần JOIN** (v4.1: dùng `device_name` trực tiếp):

```sql
-- 1 giờ gần nhất của một router, theo metric cụ thể:
SELECT event_time, metric_name, value_number, labels
  FROM ipms.tlm_metrics
 WHERE device_name = 'HAN-PE-01'
   AND metric_name = 'if_in_octets'
   AND event_time >= now() - INTERVAL 1 HOUR
 ORDER BY event_time DESC
 LIMIT 1000;

-- Tất cả router, một metric, groupby device (dashboard panel):
SELECT device_name,
       avg(value_number) AS avg_val,
       max(value_number) AS max_val
  FROM ipms.tlm_metrics
 WHERE metric_name = 'cpu_utilization_5min'
   AND event_time >= now() - INTERVAL 5 MINUTE
 GROUP BY device_name
 ORDER BY avg_val DESC;
```

**B.2) Trường hợp cần enrich thêm từ cat_device** — vẫn dùng JOIN `device_id`:

```sql
SELECT m.event_time, m.device_name, m.metric_name, m.value_number,
       d.province_code, d.network_id
  FROM ipms.tlm_metrics m
  LEFT JOIN ipms.cat_device d
    ON toInt32(m.device_id) = d.device_id
 WHERE m.metric_name = 'if_out_octets'
   AND m.event_time >= now() - INTERVAL 1 HOUR
 LIMIT 500;
```

**B.3) JOIN với path alias để hiển thị alias_path** (v4.2: `argMax` theo `updated_at`):

```sql
SELECT m.event_time, m.device_name, m.metric_name, m.value_number,
       p.alias_path
  FROM ipms.tlm_metrics m
  LEFT JOIN (
      SELECT id, argMax(alias_path, updated_at) AS alias_path
        FROM ipms.tlm_path_aliases
       GROUP BY id
  ) p ON m.path_id = p.id
 WHERE m.device_name = 'SGN-PE-02'
   AND m.event_time >= now() - INTERVAL 1 HOUR
 LIMIT 100;
```

**B.4) Alerting scan — threshold breach trên toàn fleet:**

```sql
SELECT device_name, labels['if_name'] AS interface,
       max(value_number) AS peak_errors
  FROM ipms.tlm_metrics
 WHERE metric_name = 'if_in_errors'
   AND event_time >= now() - INTERVAL 5 MINUTE
 GROUP BY device_name, interface
HAVING peak_errors > 100
 ORDER BY peak_errors DESC;
```

---

## Phụ lục C — Sync job MariaDB → ClickHouse (v4.2)

Job riêng (ngoài Flink pipeline) chạy mỗi 30s, mỗi bảng rule. **v4.2:** không còn cột `revision`; dùng `updated_at` của MariaDB để sắp thứ tự đẩy, và đặt `updated_at` của ClickHouse = thời điểm INSERT làm version key.

```sql
-- Đọc các row chưa mirror, sắp theo updated_at để đẩy đúng thứ tự:
-- rows = SELECT * FROM mariadb.tlm_<rule_table>
--          WHERE pushed_at IS NULL
--          ORDER BY updated_at ASC LIMIT 1000;

-- INSERT vào mirror; updated_at = now64(3) (version cho ReplacingMergeTree):
-- INSERT INTO ipms.tlm_<rule_table> (id, ..., updated_at)
-- VALUES (?, ..., now64(3));

-- Đánh dấu đã mirror ở MariaDB:
-- UPDATE mariadb.tlm_<rule_table>
--    SET pushed_at = NOW(6)
--  WHERE id IN (<đã sync>);
```

Soft-delete (`status=0`) cũng được sync (giữ audit trail trong CH).

> **Lưu ý monotonicity:** Vì mỗi lần mirror một row đều đặt `updated_at = now64(3)` mới hơn lần trước, version luôn tiến → `ReplacingMergeTree` giữ đúng bản mới nhất. Nếu sync nhiều row cùng batch trong cùng mili-giây không thành vấn đề vì version key chỉ so sánh trong cùng `id`.

---

## Phụ lục D — Flink enrichment logic (device_id + device_name)

Flink job duy trì một broadcast `HashMap<String, DeviceInfo>` key=`device_ip`, refresh mỗi 60s từ MariaDB `cat_device`. Chi phí: ~300 entries × ~200 bytes ≈ 60 KB — negligible.

```text
sourceTag = event.tags["source"]          // "10.138.157.93:57500"
deviceIp  = sourceTag.split(":")[0]       // "10.138.157.93"
device    = deviceCache.get(deviceIp)

if device != null:
  out.device_id   = (UInt32) device.device_id
  out.device_name = device.device_name    // "HAN-PE-01"
else:
  out.device_id   = 0                     // reserved: chưa đăng ký
  out.device_name = ""                    // empty string, NOT NULL
```

Cache refresh (side-input, mỗi 60s):

```sql
SELECT device_id, device_name, device_ip
  FROM mariadb.cat_device
 WHERE status = 1;
```
