# Telemetry Normalization Schema — MariaDB v4.3

> **Định dạng:** Markdown (kèm khối DDL `sql` chạy được).
> **Mục đích tài liệu:** Mô tả schema chuẩn hoá telemetry phía MariaDB cùng các quy ước vận hành, đồng bộ Kafka/ClickHouse, và Flink enrichment contract.

---

## Changelog

| Phiên bản | Thay đổi |
| --------- | -------- |
| **v4.3** | **[ADDED]** Bổ sung giá trị `"OpenConfig"` cho `vendor_code` ở các bảng rule (`tlm_path_aliases`, `tlm_metric_aliases`, `tlm_label_aliases`, `tlm_filter_rules`). `OpenConfig` **không phải vendor thiết bị** mà là **data model** vendor-neutral: rule áp cho mọi record có sensor path thuộc model OpenConfig (`original_path`/`match_path` bắt đầu bằng `openconfig-`), **bất kể vendor của thiết bị**. Thứ tự ưu tiên khi nhiều rule cùng match: vendor-specific → OpenConfig → All (xem Phụ lục B). Không đổi DDL kiểu dữ liệu (`VARCHAR(16)` đủ chứa). |
| **v4.2** | **[REMOVED]** Bỏ hoàn toàn bảng `tlm_revision_counter`. **[REMOVED]** Bỏ cột `revision` (cùng index `idx_*_revision`) trong cả 4 bảng `tlm_path_aliases`, `tlm_metric_aliases`, `tlm_label_aliases`, `tlm_filter_rules`. **[CHANGED]** Cơ chế version/ordering chuyển sang dùng `updated_at` (DATETIME(6)) — vốn đã `ON UPDATE CURRENT_TIMESTAMP(6)` nên tăng đơn điệu theo mỗi lần CRUD. Flink dùng `updated_at` để bỏ qua message stale; sync job dùng `updated_at` làm version cho ReplacingMergeTree bên ClickHouse. `pushed_at` giữ nguyên vai trò cờ "đã mirror sang ClickHouse". |
| v4.1 | **[NO DDL CHANGE]** Schema MariaDB không đổi; `cat_device` đã có `device_name`. **[DOCUMENTED]** Appendix E — Flink enrichment contract: Flink đọc `cat_device.device_name` và ghi vào `ClickHouse.tlm_metrics.device_name` (LowCardinality field mới trong ClickHouse v4.1). MariaDB chỉ là nguồn đọc. |
| v4.0 | **[REMOVED]** `tlm_flink_rules` (denormalized projection). **[ADDED]** `tlm_filter_rules` (nguồn duy nhất cho Filter rule), `tlm_revision_counter` (monotonic per-table). **[MODIFIED]** Thêm `revision`, `pushed_at` vào `tlm_path_aliases`/`tlm_metric_aliases`/`tlm_label_aliases`; thêm `priority` cho metric/label alias và filter. |

---

## Triết lý thiết kế

1. **Mỗi loại rule là một bảng nguồn-duy-nhất riêng:**
   - `tlm_path_aliases` → topic `rules.path_rename`
   - `tlm_metric_aliases` → topic `rules.field_rename`
   - `tlm_label_aliases` → topic `rules.label_rename`
   - `tlm_filter_rules` → topic `rules.filter`

   Bốn bảng độc lập, mỗi bảng → một compacted Kafka topic.

2. **KHÔNG có bảng denormalized trung gian** — publisher JOIN tại lúc publish (ví dụ: `tlm_metric_aliases` LEFT JOIN `tlm_path_aliases` để lấy `alias_path` làm `match_path` trong `FieldRenameRule` event).

3. **Versioning bằng `updated_at` (v4.2):** Mỗi bảng dùng cột `updated_at` (DATETIME(6), `ON UPDATE CURRENT_TIMESTAMP(6)`) làm mốc version. Mỗi lần CRUD đẩy `updated_at` tiến lên đơn điệu, đủ để Flink bỏ qua message stale và để ClickHouse `ReplacingMergeTree` dedup. Không còn counter riêng — giảm một điểm ghi chung gây contention và đơn giản hoá transaction CRUD.

   > **Lưu ý độ phân giải:** `DATETIME(6)` có độ phân giải micro-giây. Trên cùng một row, mỗi UPDATE đều refresh `updated_at` nên giá trị luôn tiến. Trường hợp hiếm hai write cực gần nhau trong cùng micro-giây, đặt `updated_at = GREATEST(updated_at + INTERVAL 1 MICROSECOND, CURRENT_TIMESTAMP(6))` trong transaction để bảo đảm đơn điệu tuyệt đối.

4. **`pushed_at` là cờ "sync job đã đẩy sang ClickHouse mirror":**
   - `NULL` = chưa sync, sync job sẽ pick ở vòng polling kế.
   - `NOT NULL` = đã INSERT thành công vào `ipms.<table>` bên ClickHouse.

   KHÔNG dùng để track Flink đã apply rule chưa (đó là observability concern tách biệt — dùng Kafka consumer lag hoặc Flink metrics).

5. **Soft-delete:** `status=0`, KHÔNG physical DELETE vì ClickHouse vẫn reference `path_id`/`alias_metric` của các row đã có dữ liệu.

6. **App layer chỉ ghi MariaDB; publisher relay tự đồng bộ Kafka.** App KHÔNG bao giờ gọi Kafka trực tiếp khi CRUD.

---

## (B.1) `tlm_path_aliases`

```sql
CREATE TABLE IF NOT EXISTS tlm_path_aliases (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT
        COMMENT 'PK. = ClickHouse.path_id. id=0 RESERVED cho "không có rule khớp".',
    vendor_code     VARCHAR(16) NOT NULL
        COMMENT '"Cisco"|"Juniper"|"Nokia"|"OpenConfig"|"All". Validate ở app layer.
                 v4.3: "OpenConfig" = rule cho path thuộc data model OpenConfig (original_path
                 bắt đầu bằng "openconfig-"), áp mọi vendor thiết bị. Ưu tiên: vendor-specific
                 > OpenConfig > All (Phụ lục B).',
    original_path   VARCHAR(512) NOT NULL
        COMMENT 'Sensor path thô. KHÔNG XOÁ sau khi đã có ClickHouse data tham chiếu.',
    alias_path      VARCHAR(512) NOT NULL
        COMMENT 'Path chuẩn Flink dùng (sau PathRename). KHÔNG SỬA sau khi Active; nếu cần đổi: Deprecated + tạo mới + cascade reset pushed_at của children.',
    status          TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '1=Active, 0=Deprecated.',

    -- v4.0: sync với ClickHouse mirror (v4.2: bỏ revision, dùng updated_at làm version)
    pushed_at       DATETIME(6) NULL
        COMMENT 'Sync job set sau khi INSERT thành công vào ipms.tlm_path_aliases (ClickHouse). NULL = chưa sync, sync job pick ở vòng kế.',

    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'v4.2: đóng vai trò version đơn điệu. Flink dùng để bỏ qua message stale; ClickHouse dùng làm version cho ReplacingMergeTree.',

    PRIMARY KEY (id),
    UNIQUE KEY uq_path_alias_source (vendor_code, original_path),
    UNIQUE KEY uq_path_alias_target_active (vendor_code, alias_path, status),
    KEY idx_tlm_path_aliases_status (status),
    KEY idx_tlm_path_aliases_updated_at (updated_at),
    KEY idx_tlm_path_aliases_pushed_at (pushed_at),
    CONSTRAINT chk_path_status CHECK (status IN (0, 1))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
```

---

## (B.2) `tlm_metric_aliases`

```sql
CREATE TABLE IF NOT EXISTS tlm_metric_aliases (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    vendor_code     VARCHAR(16) NOT NULL
        COMMENT '"Cisco"|"Juniper"|"Nokia"|"OpenConfig"|"All". Validate ở app layer.',
    path_alias_id   INT UNSIGNED NULL
        COMMENT 'NULL = any-path scope (áp cho mọi path). NOT NULL = chỉ áp cho path đó. Logic-layer FK → tlm_path_aliases.id.',
    original_name   VARCHAR(256) NOT NULL
        COMMENT 'Tên metric thô từ router (= match_metric trong FieldRename event).',
    alias_metric    VARCHAR(128) NOT NULL
        COMMENT 'Tên metric chuẩn Flink ghi vào ClickHouse.metric_name.',

    -- Value type / transform
    value_type_override TINYINT UNSIGNED NULL
        COMMENT 'NULL = giữ kiểu thô. 1=number, 2=string, 3=bool.',
    source_unit     VARCHAR(32) NULL
        COMMENT 'Đơn vị thô (UCUM). NULL nếu value_type∈{string,bool}.',
    target_unit     VARCHAR(32) NULL
        COMMENT 'Đơn vị sau chuẩn hoá (UCUM). Copy xuống ClickHouse.unit.',
    transform_kind  TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '0=identity, 1=linear(scale*x+offset), 2=expression(Java biến x), 3=enum_mapping(JSON).',
    scale_factor    DECIMAL(20,10) NOT NULL DEFAULT 1,
    offset_value    DECIMAL(20,10) NOT NULL DEFAULT 0,
    transform_expression VARCHAR(1024) NULL
        COMMENT 'NOT NULL khi transform_kind=2, NULL otherwise.',
    enum_mapping    JSON NULL
        COMMENT 'NOT NULL khi transform_kind=3, NULL otherwise.',

    -- v4.0: priority cho conflict resolution path-scoped vs any-path
    priority        SMALLINT UNSIGNED NOT NULL DEFAULT 100
        COMMENT 'Cao hơn thắng khi nhiều rule match. Convention: path-scoped=100, any-path=50.',

    status          TINYINT UNSIGNED NOT NULL DEFAULT 1,

    -- v4.0: sync với ClickHouse mirror (v4.2: bỏ revision, dùng updated_at làm version)
    pushed_at       DATETIME(6) NULL
        COMMENT 'Sync job set sau khi INSERT thành công vào ClickHouse mirror tương ứng. NULL = chưa sync.',

    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'v4.2: version đơn điệu cho Flink stale-rejection & ClickHouse ReplacingMergeTree.',

    path_scope_key  INT UNSIGNED GENERATED ALWAYS AS (IFNULL(path_alias_id, 0)) VIRTUAL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_metric_alias_source (vendor_code, path_scope_key, original_name),
    UNIQUE KEY uq_metric_alias_target (vendor_code, path_scope_key, alias_metric),
    KEY idx_tlm_metric_aliases_path_alias_id (path_alias_id),
    KEY idx_tlm_metric_aliases_status (status),
    KEY idx_tlm_metric_aliases_updated_at (updated_at),
    KEY idx_tlm_metric_aliases_pushed_at (pushed_at),

    CONSTRAINT chk_ma_value_type_override CHECK (value_type_override IS NULL OR value_type_override IN (1,2,3)),
    CONSTRAINT chk_ma_transform_kind      CHECK (transform_kind IN (0,1,2,3)),
    CONSTRAINT chk_ma_status              CHECK (status IN (0,1)),
    CONSTRAINT chk_ma_expr_required       CHECK (
        (transform_kind=2 AND transform_expression IS NOT NULL)
        OR (transform_kind<>2 AND transform_expression IS NULL)),
    CONSTRAINT chk_ma_enum_required       CHECK (
        (transform_kind=3 AND enum_mapping IS NOT NULL)
        OR (transform_kind<>3 AND enum_mapping IS NULL)),
    CONSTRAINT chk_ma_unit_type           CHECK (
        value_type_override IS NULL OR value_type_override=1
        OR (value_type_override IN (2,3)
            AND source_unit IS NULL AND target_unit IS NULL
            AND scale_factor=1 AND offset_value=0
            AND transform_kind IN (0,3)))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
```

---

## (B.3) `tlm_label_aliases`

```sql
CREATE TABLE IF NOT EXISTS tlm_label_aliases (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    vendor_code     VARCHAR(16) NOT NULL
        COMMENT '"Cisco"|"Juniper"|"Nokia"|"OpenConfig"|"All".',
    path_alias_id   INT UNSIGNED NULL
        COMMENT 'NULL = any-path. NOT NULL = path-scoped. Logic-layer FK → tlm_path_aliases.id.',

    -- Key rename
    original_key    VARCHAR(128) NOT NULL,
    alias_key       VARCHAR(128) NOT NULL,

    -- Value normalize dispatcher
    lv_kind         TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '0=identity, 1=regex(lv_pattern+lv_replace), 2=enum_mapping(lv_mapping).',
    lv_pattern      VARCHAR(512) NULL
        COMMENT 'NOT NULL khi lv_kind=1, NULL otherwise.',
    lv_replace      VARCHAR(512) NULL
        COMMENT 'NOT NULL khi lv_kind=1, NULL otherwise.',
    lv_mapping      JSON NULL
        COMMENT 'NOT NULL khi lv_kind=2, NULL otherwise.',

    -- v4.0: priority
    priority        SMALLINT UNSIGNED NOT NULL DEFAULT 100,

    status          TINYINT UNSIGNED NOT NULL DEFAULT 1,

    -- v4.0: sync với ClickHouse mirror (v4.2: bỏ revision, dùng updated_at làm version)
    pushed_at       DATETIME(6) NULL
        COMMENT 'Sync job set sau khi INSERT thành công vào ClickHouse mirror tương ứng. NULL = chưa sync.',

    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'v4.2: version đơn điệu cho Flink stale-rejection & ClickHouse ReplacingMergeTree.',

    path_scope_key  INT UNSIGNED GENERATED ALWAYS AS (IFNULL(path_alias_id, 0)) VIRTUAL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_label_alias_source (vendor_code, path_scope_key, original_key),
    UNIQUE KEY uq_label_alias_target (vendor_code, path_scope_key, alias_key),
    KEY idx_tlm_label_aliases_path_alias_id (path_alias_id),
    KEY idx_tlm_label_aliases_status (status),
    KEY idx_tlm_label_aliases_updated_at (updated_at),
    KEY idx_tlm_label_aliases_pushed_at (pushed_at),

    CONSTRAINT chk_la_lv_kind        CHECK (lv_kind IN (0,1,2)),
    CONSTRAINT chk_la_status         CHECK (status IN (0,1)),
    CONSTRAINT chk_la_regex_required CHECK (
        (lv_kind=1 AND lv_pattern IS NOT NULL AND lv_replace IS NOT NULL)
        OR (lv_kind<>1 AND lv_pattern IS NULL AND lv_replace IS NULL)),
    CONSTRAINT chk_la_enum_required  CHECK (
        (lv_kind=2 AND lv_mapping IS NOT NULL)
        OR (lv_kind<>2 AND lv_mapping IS NULL))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Label key rename + value normalize. v4.0: trực tiếp publish lên Kafka, không qua tlm_flink_rules.';
```

---

## (B.4) `tlm_filter_rules`

Quy tắc lọc bỏ metric (drop) trước các bước rename. Mỗi row = một quy tắc lọc cho một vendor + path scope + optional metric.

```sql
CREATE TABLE IF NOT EXISTS tlm_filter_rules (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    vendor_code     VARCHAR(16) NOT NULL
        COMMENT '"Cisco"|"Juniper"|"Nokia"|"OpenConfig"|"All".',
    match_path      VARCHAR(512) NOT NULL DEFAULT ''
        COMMENT 'Path scope sau PathRename. Rỗng = áp cho mọi path.',
    match_metric    VARCHAR(256) NULL
        COMMENT 'Metric name để lọc. NULL = lọc theo path (mọi metric trong path đó).',
    filter_expression VARCHAR(1024) NOT NULL DEFAULT ''
        COMMENT 'Biểu thức predicate. Rỗng = unconditional (luôn TRUE). Cú pháp do Flink tự định nghĩa (vd: "value > 1000", "tag.host LIKE %prod%").',
    filter_action   TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '0=EXCLUDE_IF_MATCH (drop record khi predicate TRUE — noise filter), 1=INCLUDE_IF_MATCH (chỉ giữ record khi predicate TRUE — selective sample vd CPU>80).',

    priority        SMALLINT UNSIGNED NOT NULL DEFAULT 100,
    status          TINYINT UNSIGNED NOT NULL DEFAULT 1,

    -- v4.0: sync với ClickHouse mirror (v4.2: bỏ revision, dùng updated_at làm version)
    pushed_at       DATETIME(6) NULL
        COMMENT 'Sync job set sau khi INSERT thành công vào ClickHouse mirror tương ứng. NULL = chưa sync.',

    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'v4.2: version đơn điệu cho Flink stale-rejection & ClickHouse ReplacingMergeTree.',

    PRIMARY KEY (id),
    KEY idx_tlm_filter_rules_match (vendor_code, match_path, match_metric),
    KEY idx_tlm_filter_rules_status (status),
    KEY idx_tlm_filter_rules_updated_at (updated_at),
    KEY idx_tlm_filter_rules_pushed_at (pushed_at),

    CONSTRAINT chk_fr_status CHECK (status IN (0,1)),
    CONSTRAINT chk_fr_action CHECK (filter_action IN (0,1))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Filter rules. Flink apply ĐẦU TIÊN trong pipeline — drop hoặc keep record theo filter_action.';
```

---

## Phụ lục A — Bảng tra cứu giá trị TINYINT

- **value_type** (`ClickHouse.value_type`, `tlm_metric_aliases.value_type_override`): `1=number, 2=string, 3=bool`
- **transform_kind** (`tlm_metric_aliases`): `0=identity, 1=linear(scale*x+offset), 2=expression(Java x), 3=enum_mapping(JSON)`
- **lv_kind** (`tlm_label_aliases`): `0=identity, 1=regex(lv_pattern+lv_replace), 2=enum_mapping(lv_mapping JSON)`
- **status** (mọi bảng rule): `0=Inactive/Deprecated, 1=Active`

---

## Phụ lục B — Quy ước priority

`tlm_metric_aliases` / `tlm_label_aliases`:

- `path_alias_id IS NOT NULL` → `priority=100` (path-specific, ưu tiên cao)
- `path_alias_id IS NULL` → `priority=50` (any-path, fallback)
- `vendor_code='OpenConfig'` → `priority=30` (model-fallback — v4.3)
- `vendor_code='All'` → `priority=10` (vendor-fallback)

`tlm_filter_rules`: do operator quyết định case-by-case.

Flink: khi nhiều rule match cùng một record, chọn rule có `priority` CAO NHẤT.

**Ngữ nghĩa `vendor_code='OpenConfig'` (v4.3):** `OpenConfig` không phải vendor thiết bị mà là **data model** vendor-neutral. Rule với `vendor_code='OpenConfig'` chỉ được xét khi sensor path thuộc model OpenConfig (`original_path`/`match_path` bắt đầu bằng `openconfig-`), và match **bất kể vendor của thiết bị** (cùng một path `openconfig-interfaces/...` có thể đến từ router Cisco lẫn Juniper). Thứ tự ưu tiên khi nhiều rule cùng match một record: **vendor-specific → OpenConfig → All**.

Riêng `tlm_path_aliases` (không có cột `priority`): unique `(vendor_code, original_path)` cho phép cùng một `original_path` có rule ở nhiều `vendor_code`; Flink resolve theo thứ tự cố định **vendor của thiết bị → OpenConfig → All**, lấy rule đầu tiên match.

---

## Phụ lục C — Topology Kafka topics

4 compacted topics (`cleanup.policy=compact`), key = string id:

| Topic | Key | Value |
| ----- | --- | ----- |
| `rules.path_rename` | `path:<id>` | `PathRenameRule` JSON |
| `rules.field_rename` | `field:<id>` | `FieldRenameRule` JSON |
| `rules.label_rename` | `label:<id>` | `LabelRenameRule` JSON |
| `rules.filter` | `filter:<id>` | `FilterRule` JSON |

Flink: 4 `KafkaSource` → union với tagged envelope `RuleEvent` → broadcast.

---

## Phụ lục D — Mapping bảng MariaDB → Kafka event POJO

> **v4.2:** Trường `version` của mỗi event lấy từ `updated_at` (epoch micro-giây hoặc ISO-8601), thay cho `revision` ở các phiên bản trước. Flink dùng nó để bỏ qua message stale.

```text
tlm_path_aliases → PathRenameRule {
  id, version (= updated_at), status,
  vendorCode = vendor_code,
  originalPath = original_path,
  aliasPath = alias_path
}

tlm_metric_aliases → FieldRenameRule {
  id, version (= updated_at), status,
  vendorCode = vendor_code,
  matchPath = COALESCE(JOIN tlm_path_aliases.alias_path, ''),  -- LEFT JOIN
  matchMetric = original_name,
  aliasMetric = alias_metric,
  transformKind, scaleFactor, offsetValue, transformExpression, enumMapping,
  sourceUnit, targetUnit, valueTypeOverride,
  priority
}

tlm_label_aliases → LabelRenameRule {
  id, version (= updated_at), status,
  vendorCode = vendor_code,
  matchPath = COALESCE(JOIN tlm_path_aliases.alias_path, ''),
  originalKey = original_key,
  aliasKey = alias_key,
  lvKind, lvPattern, lvReplace, lvMapping,
  priority
}

tlm_filter_rules → FilterRule {
  id, version (= updated_at), status,
  vendorCode = vendor_code,
  matchPath = match_path,
  matchMetric = match_metric,
  filterExpression = filter_expression,
  priority
}
```

---

## Phụ lục E — Flink enrichment contract: device_id + device_name (v4.1)

**Bối cảnh:** ClickHouse v4.1 thêm cột `device_name` (`LowCardinality(String)`) vào `ipms.tlm_metrics` để tránh JOIN `cat_device` tốn kém trên mọi query. MariaDB không đổi DDL; `cat_device.device_name` là nguồn dữ liệu.

Flink đọc từ MariaDB `cat_device` để lấy `device_name`, thực hiện lookup tại ingest time và ghi vào ClickHouse. KHÔNG có bảng MariaDB mới.

**Query Flink dùng để build cache** (chạy lúc startup + refresh mỗi 60s):

```sql
SELECT device_id, device_name, device_ip
  FROM cat_device
 WHERE status = 1;
```

→ Load vào `HashMap<String, DeviceInfo>` với key = `device_ip`, `DeviceInfo { int device_id, String device_name }`.

**Lưu ý quan trọng — `cat_device.device_id` là INT (có dấu):** Khi ghi vào ClickHouse (`UInt32`), cast `(UInt32) Math.max(0, device_id)`. `device_id` âm không xảy ra trong thực tế nhưng nên guard.

**Hành vi khi cache miss** (router chưa đăng ký trong `cat_device`):

- `out.device_id = 0` (UInt32, reserved value)
- `out.device_name = ""` (empty string, NOT NULL trong ClickHouse)

**Retry / stale cache:** Nếu router mới đăng ký, Flink pick up trong vòng refresh kế (≤60s). Các event trong 60s đó ghi `device_id=0`, `device_name=""` — chấp nhận được. KHÔNG cần replay; NOC có thể query bằng `device_id` sau khi cache update.

---

## Phụ lục F — Hướng dẫn migration v4.1 → v4.2

Áp dụng cho database đã chạy v4.0/v4.1 (đã có `revision` và `tlm_revision_counter`):

```sql
-- 1) Bỏ index revision rồi bỏ cột revision trên 4 bảng rule.
ALTER TABLE tlm_path_aliases   DROP INDEX idx_tlm_path_aliases_revision,   DROP COLUMN revision;
ALTER TABLE tlm_metric_aliases DROP INDEX idx_tlm_metric_aliases_revision, DROP COLUMN revision;
ALTER TABLE tlm_label_aliases  DROP INDEX idx_tlm_label_aliases_revision,  DROP COLUMN revision;
ALTER TABLE tlm_filter_rules   DROP INDEX idx_tlm_filter_rules_revision,   DROP COLUMN revision;

-- 2) Thêm index trên updated_at (nếu chưa có) để hỗ trợ sync job ORDER BY updated_at.
ALTER TABLE tlm_path_aliases   ADD KEY idx_tlm_path_aliases_updated_at   (updated_at);
ALTER TABLE tlm_metric_aliases ADD KEY idx_tlm_metric_aliases_updated_at (updated_at);
ALTER TABLE tlm_label_aliases  ADD KEY idx_tlm_label_aliases_updated_at  (updated_at);
ALTER TABLE tlm_filter_rules   ADD KEY idx_tlm_filter_rules_updated_at   (updated_at);

-- 3) Bỏ hoàn toàn bảng counter.
DROP TABLE IF EXISTS tlm_revision_counter;
```

> **Trước khi migrate, đồng bộ phía ClickHouse:** đổi `ReplacingMergeTree(revision)` của các bảng mirror sang `ReplacingMergeTree(updated_at)` (xem tài liệu ClickHouse v4.2). Và cập nhật app/sync job: bỏ bước `UPDATE tlm_revision_counter`, dùng `updated_at` làm version.
