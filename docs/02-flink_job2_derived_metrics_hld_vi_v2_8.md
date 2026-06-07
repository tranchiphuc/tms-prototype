# Thiết kế Flink Job 2 — Derived Metrics Pipeline (HLD)

**Phiên bản:** v2.8
**Loại tài liệu:** High-Level Design
**Ngôn ngữ xử lý:** Apache Flink (Java/Scala — dev tự quyết định)
**Phạm vi:** Pipeline tính toán derived metrics từ `processed_metrics` (Kafka) → `ipms.tlm_metrics` (ClickHouse) **+ Kafka `derived_metrics`** (cho Streaming Alert Engine — Job 3)

> **Mục đích tài liệu:** Mô tả **ý định thiết kế** và **các ràng buộc quan trọng** cho Flink Job 2. Dev có toàn quyền chọn thư viện, cấu trúc class, và chi tiết implementation — miễn đáp ứng yêu cầu đầu ra và các ràng buộc mô tả trong tài liệu này.

---

## Changelog

| Phiên bản | Thay đổi |
|-----------|----------|
| **v2.8** | **[REMOVED]** Bỏ cột `pushed_at` khỏi `tlm_derived_rules` — bảng này không có ClickHouse mirror; Flink Job 2 load rule trực tiếp từ MariaDB, versioning chỉ cần `updated_at`. Bỏ index `idx_derived_rules_pushed_at`. Bỏ bước "reset `pushed_at=NULL`" khỏi transaction write (§7.0). Bỏ field `pushed_at` khỏi response body (§7.2, §7.3). Bỏ endpoint `GET /derived-rules/refresh-status` (§7.6) — thay bằng ghi chú observability qua Flink metrics / Pipeline Monitor. **[ADDED]** Giới hạn `scope_device_ids` tối đa **20 phần tử** — validate tại API (§7.4), trả `422` nếu vượt. Lý do: field phục vụ scope "nhóm nhỏ cụ thể" (≤ 1–2 POP); nếu cần áp trên 20 thiết bị thì để `NULL`. **[UPDATED]** §3.2 comment DDL `scope_device_ids`. **[UPDATED]** comment `updated_at` bỏ đề cập sync job (không còn nghĩa). |
| **v2.7** | **[FIXED]** Sửa tham chiếu chéo tên tài liệu Job 3: `04-alert_engine_streaming_job3_hld` → **`03-alert_engine_streaming_job3_hld`** (2 chỗ: ghi chú changelog v2.5 và bảng quan hệ thành phần §2.2) — khớp tên file thực tế trong bộ tài liệu. **Không đổi bất kỳ nội dung thiết kế nào** (schema, pipeline, API, NFR giữ nguyên v2.6). |
| **v2.6** | **[FIXED]** Dọn nốt các tham chiếu **`input_metrics[].path_alias_id`** còn sót lại từ mô hình array-of-object (đã bỏ ở v2.4): comment trong DDL §3.2 và đoạn cascade §7.5 — nay nhất quán với `input_metrics` = array of string + `scope_path_alias_id` cấp rule. **[FIXED]** Mô hình **Aggregated (`derive_kind=1`)**: bổ sung `rule_id` vào key (trước đây key `(device_id, metric_name, path_id, labels_fingerprint)` thiếu `rule_id` ⇒ hai aggregated rule cùng metric sẽ đụng state/cửa sổ). Làm rõ cách xử lý **nhiều aggregated rule trên cùng metric với `window_seconds` khác nhau** dưới mô hình rule động/broadcast (§3.5, §5.2 — thêm §5.2.1). **[FIXED]** Sửa `rule.requiredInputs` → `rule.inputMetrics` trong pseudo-code §5.1. **[CLARIFIED]** Input của Computed phải là number (`valueType=1`) — bỏ qua event string/bool (§5.1). Không đổi DDL `tlm_derived_rules`, mô hình derive, hay hai-sink. |
| **v2.5** | **[ADDED]** Sink **Kafka `derived_metrics`** (song song với ClickHouse sink) — **BẮT BUỘC** để Streaming Alert Engine (Job 3, tài liệu `03-alert_engine_streaming_job3_hld`) alert được trên derived metric. Lý do: Job 3 thay thế poll engine cũ và **không đọc ClickHouse**, nên derived phải có mặt trên Kafka. **Topic riêng** `derived_metrics`, KHÔNG ghi vào `processed_metrics` (sẽ gây Job 2 tự consume lại output → vòng lặp). Message format = **`MetricEvent`** (giống `processed_metrics`), `path_id=0`, `raw_path="derived"`. Cập nhật §1.2, §2.1, §2.2, §4.1, §6.3 (từ "tùy chọn" → bắt buộc, kèm schema), §8 (lỗi sink Kafka), §9 (NFR delivery). Không đổi mô hình derive, DDL `tlm_derived_rules`, hay logic tính toán. |
| v2.4 | **[CHANGED]** `input_metrics` đổi từ JSON array of object `{alias_metric, path_alias_id}` về **JSON array of string** (`alias_metric`). Lý do: `scope_path_alias_id` (cấp rule) đã đảm nhiệm việc khử nhập nhằng path — `path_alias_id` per-input là dư thừa. Match trên stream: `event.metric_name == alias_metric` **và** thoả scope của rule (`scope_path_alias_id` NULL = mọi path). Trường hợp cùng `alias_metric` mang nghĩa khác nhau ở nhiều path được xử lý bằng `scope_path_alias_id` + **naming convention** (alias_metric không trùng nghĩa giữa các path) + API cảnh báo khi tạo rule scope-NULL với alias_metric tồn tại ở ≥2 path (§7.4, §7.8). Nhất quán với mô hình `alias_metric` + `path_alias_id` cấp rule của Alert Engine. **[ADDED]** Đưa `assembly_window_seconds` vào DDL `tlm_derived_rules` (trước đó chỉ có ở POJO/API — thiếu sót so với ý định thiết kế §5.1). **[REMOVED]** Bỏ cột `revision` + index + đăng ký `tlm_revision_counter` (bảng counter đã bị loại bỏ từ MariaDB v4.2); versioning chuyển sang `updated_at` như các bảng rule khác — cập nhật §7.0/§7.2/§7.3/§7.5/§7.6, POJO. **[FIXED]** Sửa các tham chiếu `derive_kind ∈ {0,1,2,3}` còn sót (chỉ còn `{0,1,2}` từ v2.3). |
| v2.3 | **[REMOVED]** Bỏ hẳn Composite (interval join). Nhu cầu "kết hợp nhiều metric rồi tính" do **Computed (`derive_kind=0`)** phục vụ trọn vẹn (1..n input, đúng 1 output, có `static_constants` fallback, cửa sổ gom `assembly_window_seconds`). **[CHANGED]** Dồn lại mã `derive_kind` còn **3 giá trị liên tục: `0`=computed, `1`=aggregated, `2`=delta** (Delta đổi từ `3` → `2`). Cập nhật schema MariaDB (bỏ `join_window_seconds`, bỏ CHECK composite, đổi CHECK kind thành `IN (0,1,2)`), POJO, API, ví dụ JSON, bảng lỗi, thuật ngữ. **[CLARIFIED §5.1 Computed]** tách bạch `assembly_window_seconds` (cửa sổ chờ gom đủ input) khỏi state TTL (lưới an toàn); chuẩn hoá bảng exception khi thiếu input; last-write-wins cho input trùng; `static_constants` không cấu hình + metric không về ⇒ thiếu dữ liệu ⇒ không emit. |
| v2.1 | **[EXPANDED]** Section 7 "REST API quản lý rule" — chi tiết hoá: quy ước chung (auth, transaction + `revision` + `pushed_at`, mã trạng thái, `If-Match`), bảng endpoint đầy đủ (CRUD + list + preview + metric-aliases lookup + refresh-status), list/lọc/phân trang, ví dụ tạo cho cả 4 `derive_kind`, PUT/PATCH/DELETE (soft-delete) + cascade theo `scope_path_alias_id`, validation đầy đủ theo CHECK constraint của bảng, định dạng lỗi. Không thay đổi schema, pipeline hay mô hình derive. |
| v2.0 | **[ADDED]** Delta — loại derived metric tính delta giữa sample hiện tại và sample trước (ví dụ: counter bytes → rate Mbps). *(Lưu ý: v2.0 đặt mã `derive_kind=3`; từ v2.3 Delta mang mã `derive_kind=2` sau khi bỏ Composite.)* **[CHANGED]** `input_metrics` trong `tlm_derived_rules` đổi từ JSON array of string sang JSON array of object `{alias_metric, path_alias_id}` để định danh metric không mơ hồ khi cùng `alias_metric` tồn tại ở nhiều path scope. Cặp này khớp trực tiếp với `(metric_name, path_id)` mang sẵn trên stream `processed_metrics` — không cần cache hay lookup phụ. Cập nhật schema MariaDB, POJO, API, và ví dụ liên quan. |
| v1.0 | Phiên bản khởi tạo. |

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Ngữ cảnh hệ thống](#2-ngữ-cảnh-hệ-thống)
3. [Dữ liệu tham chiếu — Derived Rules](#3-dữ-liệu-tham-chiếu--derived-rules)
4. [Kiến trúc pipeline](#4-kiến-trúc-pipeline)
5. [Các mô hình derive](#5-các-mô-hình-derive)
6. [Schema đầu ra](#6-schema-đầu-ra)
7. [REST API quản lý rule](#7-rest-api-quản-lý-rule)
8. [Xử lý lỗi](#8-xử-lý-lỗi)
9. [Yêu cầu phi chức năng](#9-yêu-cầu-phi-chức-năng)
10. [Thuật ngữ](#10-thuật-ngữ)

---

## 1. Tổng quan

### 1.1 Mục tiêu

Flink Job 2 cho phép operator tạo **derived metrics** — các metric mới được tính toán từ các metric thô đã có — mà không cần thay đổi cấu hình router hay pipeline thu thập dữ liệu (Flink Job 1).

Các loại derived metric được hỗ trợ:

| Loại | derive_kind | Mô tả | Ví dụ |
|------|-------------|-------|-------|
| Computed | `0` | Tính theo công thức trên nhiều metric cùng `(device_id, labels)`; **chờ gom đủ input** trong một cửa sổ rồi mới eval | `if_in_utilization_pct = if_in_octets * 8.0 / if_speed * 100` |
| Aggregated | `1` | Tổng hợp theo cửa sổ thời gian (tumbling window) | `avg_cpu_5min = avg(cpu_utilization_5min) over 5 phút` |
| Delta | `2` | Tính delta giữa sample hiện tại và sample trước trên cùng key, chia cho khoảng thời gian giữa hai sample | `if_in_bps = (if_in_octets[t] − if_in_octets[t−1]) * 8 / Δt` |

> **Lưu ý:** Computed là mô hình cho mọi phép tính "gom đủ input rồi eval", kể cả khi có **đúng 2 input** đến lệch thời gian — cửa sổ gom `assembly_window_seconds` xử lý độ lệch này (xem §5.1). Phiên bản trước có loại Composite (`derive_kind=2`, interval join) nhưng đã **bị loại bỏ từ v2.3**; Delta nay mang mã `derive_kind=2`.

### 1.2 Nguyên tắc thiết kế

- **Tách biệt khỏi Job 1:** Job 2 là một Flink job hoàn toàn độc lập. Job 1 (raw pipeline) không bao giờ bị ảnh hưởng bởi lỗi hay tải của Job 2.
- **Cùng bảng lưu trữ:** Derived metric rows được ghi vào cùng bảng `ipms.tlm_metrics` với raw metrics, phân biệt qua `metric_name` và `raw_path = "derived"`.
- **Hai sink song song (v2.5):** mỗi derived row được phát ra **đồng thời** (a) **ClickHouse** `ipms.tlm_metrics` (lưu trữ/audit/dashboard) và (b) **Kafka `derived_metrics`** (cho Streaming Alert Engine — Job 3). Topic Kafka là **riêng**, KHÔNG dùng `processed_metrics` (vì đó là input của chính Job 2 → sẽ tạo vòng lặp tự-consume). Xem §6.3.
- **Rule động:** Operator tạo/sửa rule qua API tại runtime. Flink Job 2 tự động nhận rule mới mà không cần restart.
- **Fallback an toàn:** Khi không đủ input để tính toán (metric tham chiếu chưa đến, thiếu value, counter reset), bỏ qua việc emit derived record thay vì crash. Không bao giờ emit giá trị sai lệch. Một input chỉ được coi là "có sẵn" khi (a) metric thực về trong cửa sổ gom, **hoặc** (b) có `static_constants` cấu hình cho nó. Input không thoả cả hai ⇒ **thiếu dữ liệu** ⇒ không emit (xem §5.1).
- **Định danh metric bằng `alias_metric` + scope (v2.4):** `input_metrics` là danh sách `alias_metric` (string) — khớp trực tiếp `MetricEvent.metric_name` trên stream, không cần resolve qua bảng phụ. Việc khử nhập nhằng path (cùng tên metric ở nhiều path) do `scope_path_alias_id` cấp rule đảm nhiệm: scope NOT NULL ⇒ chỉ nhận event có `path_id` đúng bằng scope; scope NULL ⇒ mọi path (dựa vào naming convention — xem §3.1).

---

## 2. Ngữ cảnh hệ thống

### 2.1 Vị trí trong kiến trúc tổng thể

```
[Flink Job 1]
    │
    ├──► ClickHouse: ipms.tlm_metrics     ← raw metric rows
    └──► Kafka: processed_metrics          ← fan-out downstream
                    │
                    ▼
            [Flink Job 2]  ◄── MariaDB: tlm_derived_rules (refresh định kỳ)
                    │
                    ├──► ClickHouse: ipms.tlm_metrics  ← derived metric rows (raw_path = "derived")
                    └──► Kafka: derived_metrics         ← (v2.5) cho Streaming Alert Engine (Job 3)
                                    │
                                    ▼
                          [Flink Job 3: Streaming Alert Engine]
                          (consume processed_metrics + derived_metrics)
```

> **(v2.5) Vì sao topic riêng `derived_metrics`:** `processed_metrics` là **input** của Job 2. Nếu ghi derived vào đó, Job 2 sẽ consume lại chính output của mình (vòng lặp / derived-of-derived). Topic riêng phá vòng lặp và để Job 3 union hai nguồn (raw + derived) một cách rõ ràng.

> Khác với cách định danh bằng `id`, Flink Job 2 **không** cần đọc thêm `tlm_metric_aliases` tại runtime: mọi thông tin cần để match metric đầu vào — `alias_metric` (so với `metric_name` của event) và `scope_path_alias_id` (so với `path_id` của event) — đã nằm trong rule. Việc validate `alias_metric` tồn tại trong `tlm_metric_aliases` chỉ diễn ra ở tầng API (Spring Boot) lúc tạo rule.

### 2.2 Quan hệ với các thành phần khác

| Thành phần | Quan hệ |
|------------|---------|
| Kafka `processed_metrics` | **Input** — Job 2 consume, Job 1 produce |
| MariaDB `tlm_derived_rules` | **Reference data** — Job 2 đọc rule, refresh định kỳ |
| MariaDB `tlm_metric_aliases` | **Validation only (API layer)** — Spring Boot validate `alias_metric` tồn tại (status=1) khi tạo rule; cảnh báo nếu alias_metric tồn tại ở ≥2 path mà rule không đặt `scope_path_alias_id` (§7.4). Flink **không** đọc tại runtime. |
| ClickHouse `ipms.tlm_metrics` | **Output** — Job 2 ghi derived rows, Job 1 cũng ghi vào đây |
| Kafka `derived_metrics` (v2.5) | **Output** — derived rows cho Streaming Alert Engine (Job 3); topic riêng, format `MetricEvent` (§6.3) |
| Spring Boot API | **Rule manager** — Operator CRUD rule qua API → MariaDB |
| Streaming Alert Engine (Job 3) | **Downstream consumer** — consume `derived_metrics` (Kafka) để alert real-time. Thay cho poll engine cũ (vốn đọc `ipms.tlm_metrics`). Xem `03-alert_engine_streaming_job3_hld`. |

---

## 3. Dữ liệu tham chiếu — Derived Rules

### 3.1 Định danh metric đầu vào — `alias_metric` (string) + `scope_path_alias_id` (v2.4)

Lịch sử thiết kế: v1.0 dùng array of string; v2.0 đổi sang array of object `{alias_metric, path_alias_id}` để phân biệt trường hợp cùng tên metric ở nhiều path; **v2.4 quay về array of string** sau khi nhận thấy `path_alias_id` per-input là **dư thừa** với `scope_path_alias_id` cấp rule.

Hai tình huống cần phân biệt (không đổi):

- **Cùng metric logic, nhiều vendor:** `if_in_octets` của Cisco / Juniper / Nokia là **cùng một metric logic**, được Job 1 chuẩn hoá về cùng tên. Trên stream và trong ClickHouse, tất cả đều là `metric_name = "if_in_octets"`. Match bằng string là **đúng** — đây chính là mục đích của lớp normalization.
- **Khác metric logic, trùng tên, khác path:** ví dụ `alias_metric = "errors"` dưới path `xr_interfaces` (lỗi interface) và `"errors"` dưới path `xr_optics` (lỗi quang) là **hai metric khác nhau**. Match bằng string đơn thuần sẽ gộp nhầm.

**Giải pháp v2.4:** `input_metrics` = JSON array of string (`alias_metric`); việc khử nhập nhằng path do **`scope_path_alias_id`** đảm nhiệm:

```
event match rule khi:
    event.metric_name ∈ rule.input_metrics
    AND (rule.scope_path_alias_id IS NULL
         OR event.path_id == rule.scope_path_alias_id)
```

- `scope_path_alias_id` NOT NULL → toàn rule khoá vào một path; tình huống "trùng tên khác path" được phân biệt rõ ràng (vd rule cho `errors` quang đặt scope = path `xr_optics`).
- `scope_path_alias_id` NULL → match mọi path có `metric_name` đúng — giữ hành vi gộp đa-vendor của tình huống thứ nhất. Tình huống thứ hai khi đó dựa vào **naming convention**: `alias_metric` không được trùng nghĩa giữa các path (vd đặt `if_in_errors` / `optics_errors` thay vì cùng tên `errors`). API hỗ trợ enforce mềm: khi tạo rule scope-NULL mà `alias_metric` đang tồn tại ở ≥ 2 path khác nhau trong `tlm_metric_aliases`, API trả `warnings` khuyến nghị đặt scope (§7.4, §7.8).

**Đánh đổi chấp nhận:** một rule không thể trộn input từ **hai path khác nhau có chủ đích** với scope NOT NULL — trường hợp đó phải để scope NULL và dựa vào naming convention. Chấp nhận được vì input vốn đã key theo cùng `(device_id, labels)`. Đổi lại, mô hình nhất quán với Alert Engine (`alias_metric` + `path_alias_id` cấp rule) và rule định nghĩa gọn hơn đáng kể.

> **Vì sao không dùng `tlm_metric_aliases.id`?** `MetricEvent` trên `processed_metrics` mang `metric_name` và `path_id`, **không** mang `metric_alias_id` (cũng không mang `vendor_code`). Nếu rule lưu `id`, Flink phải reverse-map `id → tên` để match stream, gánh thêm cache và endpoint lookup. Lưu string trực tiếp đơn giản hơn.

### 3.2 Bảng MariaDB: `tlm_derived_rules` (v2.4)

So với v2.0, các thay đổi DDL ở v2.3:

- Bỏ hẳn cột `join_window_seconds` (chỉ phục vụ Composite).
- `derive_kind` còn 3 giá trị: `0`=computed, `1`=aggregated, `2`=delta. CHECK đổi thành `IN (0,1,2)`; bỏ `chk_dr_composite_params`.
- Tham số Delta (`delta_scale_factor`, `delta_reset_threshold`) nay gắn với `derive_kind=2`.

Thay đổi DDL ở v2.4:

- `input_metrics` đổi về **JSON array of string** (`alias_metric`) — xem §3.1.
- **Bổ sung cột `assembly_window_seconds`** (cửa sổ gom của Computed — §5.1; trước đó có ở POJO/API nhưng thiếu trong DDL).
- **Bỏ cột `revision`**, index `idx_derived_rules_revision`, và đăng ký `tlm_revision_counter` (bảng counter đã bị loại bỏ từ MariaDB v4.2). Versioning dùng `updated_at` (DATETIME(6), `ON UPDATE CURRENT_TIMESTAMP(6)`) như mọi bảng rule khác; thêm index `idx_derived_rules_updated_at`.

> **Migration từ v2.0/v2.2:** ở các bản đó `derive_kind=2` là Composite và `derive_kind=3` là Delta. Khi nâng lên v2.3 cần migration script: (1) chuyển mọi rule Composite (`derive_kind=2` cũ) sang Computed (`derive_kind=0`, bỏ `join_window_seconds`, thêm `assembly_window_seconds`); (2) remap rule Delta `derive_kind=3` → `derive_kind=2`; (3) DROP cột `join_window_seconds`; (4) thay các CHECK constraint. Trong triển khai mới (greenfield), áp thẳng DDL dưới đây.

```sql
CREATE TABLE IF NOT EXISTS tlm_derived_rules (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    rule_name       VARCHAR(128) NOT NULL
        COMMENT 'Tên rule hiển thị trên UI.',
    output_metric   VARCHAR(128) NOT NULL
        COMMENT 'Tên metric output. UNIQUE trong toàn bộ active rules và không được trùng
                 với alias_metric nào trong tlm_metric_aliases (validate ở app layer).',
    output_unit     VARCHAR(32) NULL
        COMMENT 'Đơn vị UCUM của output metric.',
    derive_kind     TINYINT UNSIGNED NOT NULL
        COMMENT '0=computed, 1=aggregated, 2=delta.',

    -- ── Input metrics ─────────────────────────────────────────────────
    -- v2.4: JSON array of string (alias_metric).
    --   alias_metric khớp trực tiếp MetricEvent.metric_name trên stream.
    --   Khử nhập nhằng path do scope_path_alias_id cấp rule đảm nhiệm (xem §3.1).
    -- Ví dụ: ["if_in_octets", "if_speed"]
    input_metrics   JSON NOT NULL
        COMMENT 'v2.4: JSON array of string (alias_metric). Các phần tử đôi một khác nhau.
                 App validate alias_metric tồn tại trong tlm_metric_aliases (status=1);
                 cảnh báo nếu tồn tại ở >=2 path mà scope_path_alias_id IS NULL.',

    -- derive_kind=0: biểu thức tính toán
    -- Biến trong expression là alias_metric (string) của từng input.
    -- Ví dụ: "if_in_octets * 8.0 / if_speed * 100"
    expression      VARCHAR(1024) NULL
        COMMENT 'Biểu thức tính toán. Biến = alias_metric của input. Bắt buộc khi derive_kind=0. NULL khi derive_kind IN (1, 2).',

    -- v2.4: cửa sổ gom đủ input của Computed (per-rule). Xem §5.1 —
    -- KHÁC với state TTL (lưới an toàn, cấu hình ở Flink, không lưu trong rule).
    assembly_window_seconds INT UNSIGNED NULL
        COMMENT 'Cửa sổ chờ gom đủ input (giây), tính từ firstEventTimeMs. Chỉ dùng cho derive_kind=0.
                 NULL = dùng mặc định hệ thống (~1.5 × router sample interval ≈ 90s). Phải > 0 nếu khai báo.',

    -- derive_kind=1: tham số window
    window_seconds  INT UNSIGNED NULL
        COMMENT 'Kích thước tumbling window (giây). Bắt buộc khi derive_kind=1.',
    agg_function    TINYINT UNSIGNED NULL
        COMMENT '0=avg, 1=max, 2=min, 3=sum, 4=rate(delta/window). Bắt buộc khi derive_kind=1.',

    -- ── derive_kind=2 (Delta) — tham số riêng ─────────────────────────
    -- Công thức: output = (current_value - prev_value) * delta_scale_factor / Δt_seconds
    -- Ví dụ: bytes → Mbps  →  delta_scale_factor = 8.0 / 1_000_000, output_unit = "Mbps"
    --        packets → pps →  delta_scale_factor = 1.0,             output_unit = "pps"
    delta_scale_factor DECIMAL(20,10) NOT NULL DEFAULT 1
        COMMENT 'Hệ số nhân áp lên (current - prev) / Δt. Dùng để đổi đơn vị.
                 Ví dụ: bytes/s → Mbps thì delta_scale_factor = 8.0/1000000 = 0.000008.',
    delta_reset_threshold BIGINT UNSIGNED NULL
        COMMENT 'Nếu NOT NULL: khi current_value < prev_value VÀ current_value < delta_reset_threshold,
                 coi đây là counter reset (wrap-around) và bỏ qua sample đó (không emit).
                 NULL = không xử lý counter reset (dùng cho gauge metric không phải counter).',

    -- Giá trị hằng số thay thế khi một input metric chưa có trên stream.
    -- Key là alias_metric (string), value là hằng số (number).
    -- Ví dụ: {"if_speed": 10000000000}
    static_constants JSON NULL
        COMMENT 'Map<alias_metric, Double>. Giá trị hằng thay thế khi input metric không về. Áp dụng cho derive_kind=0.',

    -- Giới hạn phạm vi áp dụng của TOÀN rule (NULL = áp cho tất cả).
    -- v2.4+: input_metrics là array of string (alias_metric) — KHÔNG còn path_alias_id per-input.
    --   scope_path_alias_id đảm nhiệm CẢ HAI vai trò:
    --     (1) giới hạn rule chỉ chạy cho path đó, VÀ
    --     (2) khử nhập nhằng path khi cùng alias_metric tồn tại ở nhiều path (xem §3.1).
    scope_device_ids    JSON NULL
        COMMENT 'JSON array of INT (device_id). NULL = áp cho mọi thiết bị.
                 v2.8: tối đa 20 phần tử — validate tại API (§7.4); vượt quá trả 422.',
    scope_path_alias_id INT UNSIGNED NULL
        COMMENT 'logic-FK → tlm_path_aliases.id. NULL = áp cho mọi path.',

    priority        SMALLINT UNSIGNED NOT NULL DEFAULT 100,
    status          TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '1=Active, 0=Deprecated.',
    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'v2.4 (theo MariaDB v4.2): version đơn điệu — Flink dùng để bỏ qua snapshot stale.
                 v2.8: không còn pushed_at; đây là trường version duy nhất của bảng này.',

    PRIMARY KEY (id),
    UNIQUE KEY uq_derived_output (output_metric, status),
    KEY idx_derived_rules_status     (status),
    KEY idx_derived_rules_updated_at (updated_at),

    -- v2.3: derive_kind còn 0,1,2 (bỏ composite)
    CONSTRAINT chk_dr_kind   CHECK (derive_kind IN (0, 1, 2)),
    CONSTRAINT chk_dr_agg_fn CHECK (agg_function IS NULL OR agg_function IN (0,1,2,3,4)),
    CONSTRAINT chk_dr_status CHECK (status IN (0, 1)),

    -- Ràng buộc tham số theo derive_kind:
    --   derive_kind=0 (computed): expression NOT NULL, window params NULL
    --   derive_kind=1 (aggregated): window_seconds + agg_function NOT NULL, expression NULL
    --   derive_kind=2 (delta): expression NULL, window params NULL; delta_scale_factor có DEFAULT
    CONSTRAINT chk_dr_computed_params   CHECK (derive_kind <> 0 OR (expression IS NOT NULL AND window_seconds IS NULL AND agg_function IS NULL)),
    CONSTRAINT chk_dr_aggregated_params CHECK (derive_kind <> 1 OR (window_seconds IS NOT NULL AND agg_function IS NOT NULL AND expression IS NULL)),
    CONSTRAINT chk_dr_delta_params      CHECK (derive_kind <> 2 OR (expression IS NULL AND window_seconds IS NULL AND agg_function IS NULL)),
    -- v2.4: assembly_window chỉ có nghĩa với computed; nếu khai báo phải > 0
    CONSTRAINT chk_dr_assembly_params   CHECK (assembly_window_seconds IS NULL
                                               OR (derive_kind = 0 AND assembly_window_seconds > 0))

) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Derived metric rules. v2.8: bỏ pushed_at (không có ClickHouse mirror); scope_device_ids tối đa 20; derive_kind 0=computed,1=aggregated,2=delta; input_metrics là array of string (alias_metric); versioning bằng updated_at.';
```

### 3.3 Cơ chế match metric đầu vào

Mỗi `MetricEvent` trên stream mang sẵn `metricName` (= `alias_metric`) và `pathId`. Logic match (v2.4):

```
matchesInput(event, rule):
    if event.metricName not in rule.inputMetrics:   // so sánh string
        return false
    if rule.scopePathAliasId == null:
        return true                                 // mọi path
    return event.pathId == rule.scopePathAliasId    // khoá theo scope của rule
```

(`inScope(rule, event)` vẫn kiểm thêm `scope_device_ids` như cũ; kiểm tra path nêu trên có thể gộp chung vào `inScope` trong implementation.)

Không cần cache, không cần JOIN, không cần đọc `tlm_metric_aliases` tại runtime.

> **Ràng buộc đặt tên trong một computed rule:** biến trong `expression` là `alias_metric` (string), nên các phần tử trong `input_metrics` phải **đôi một khác nhau** — với array of string điều này đồng nghĩa array không chứa phần tử trùng (API validate). Với delta/aggregated (1 input) không phát sinh vấn đề.

### 3.4 Cơ chế load rule vào Flink

```
Startup:
  → Load toàn bộ active rules từ tlm_derived_rules
  → lưu vào Broadcast State: Map<Integer, DerivedRule>  (key = rule.id)

Định kỳ (interval cấu hình, đề xuất 30–60s):
  → query MariaDB WHERE status=1
  → atomic replace toàn bộ snapshot trong Broadcast State
  → các metric event đang xử lý tự động dùng snapshot mới ở vòng tiếp theo
```

Khi rule bị xoá (status=0), snapshot mới không chứa rule đó → Flink tự động ngừng emit derived metric tương ứng mà không cần thêm logic.

### 3.5 Mapping `derive_kind` → Flink operator

| derive_kind | Operator Flink | Keyed by | Ghi chú |
|-------------|----------------|----------|---------|
| `0` (computed) | Keyed `KeyedBroadcastProcessFunction` + keyed state buffer + event-time timer | `(device_id, rule_id, labels_fingerprint)` | Gom đủ input_metrics trong cửa sổ gom → eval expression → emit |
| `1` (aggregated) | `TumblingEventTimeWindows` + `AggregateFunction` **trên key có `rule_id`** (hoặc `KeyedBroadcastProcessFunction` tự windowing — §5.2.1) | `(rule_id, device_id, metric_name, path_id, labels_fingerprint)` | Emit tại window close. **Phải có `rule_id` trong key** để hai aggregated rule cùng metric không đụng state/cửa sổ. |
| `2` (delta) | Keyed `KeyedBroadcastProcessFunction` + keyed state (prev sample) | `(device_id, rule_id, labels_fingerprint)` | Emit mỗi sample mới sau khi đã có ít nhất 1 sample trước |

---

## 4. Kiến trúc pipeline

### 4.1 Luồng dữ liệu tổng quan

```
[Kafka: processed_metrics]
        │
        ▼
[Source: KafkaSource<MetricEvent>]
  Deserialize JSON → MetricEvent POJO  (mang metric_name + path_id)
        │
        ▼
[Rule Broadcast + Rule Dispatcher]
  - Broadcast State: Map<rule_id, DerivedRule>
  - Với mỗi MetricEvent: tìm rule có input descriptor matchesInput(event, ref)
  - Route sang đúng operator theo derive_kind
        │
   ┌────┼────────────────┐
   ▼    ▼                ▼
[Computed] [Aggregated] [Delta]
   │           │            │
   └───────────┴────────────┘
                       │
                       ▼
             [Derived Row Builder]
  Tạo MetricEvent với:
    raw_path        = "derived"
    raw_metric_name = "derived:<rule_id>"
    metric_name     = rule.outputMetric
    unit            = rule.outputUnit
    value_type      = 1 (luôn là number)
    value_number    = kết quả tính toán
    path_id         = 0
    labels          = kế thừa từ source event(s)
                       │
            ┌──────────┴───────────┐
            ▼                       ▼
   [ClickHouse Sink]        [Kafka Sink] (v2.5)
   → ipms.tlm_metrics       → derived_metrics  (cho Job 3)
```

> **(v2.5)** Hai sink nhận **cùng một** `MetricEvent` đã dựng. ClickHouse cho lưu trữ/dashboard; Kafka cho alert real-time. Hai sink độc lập (một bên lỗi không chặn bên kia — §8). `MetricEvent` đẩy ra `derived_metrics` dùng **đúng schema** Job 3 mong đợi (§6.3) nên Job 3 deserialize chung một POJO với `processed_metrics`.

### 4.2 Cấu trúc POJO chính

```java
// MetricEvent: message trên Kafka processed_metrics (đọc vào từ Job 1)
class MetricEvent {
    long      eventTimeMs;     // event_time (millisecond)
    long      receiveTimeMs;   // receive_time
    int       deviceId;
    String    deviceName;
    int       pathId;          // dùng để kiểm scope_path_alias_id của rule
    String    rawPath;
    String    rawMetricName;
    String    metricName;      // = alias_metric; dùng để match input_metrics (string)
    int       valueType;       // 1=number, 2=string, 3=bool
    double    valueNumber;
    String    valueString;
    boolean   valueBool;
    String    unit;
    Map<String, String> labels;
}

// DerivedRule: snapshot load từ MariaDB (v2.4)
class DerivedRule {
    int      id;
    String   ruleName;
    String   outputMetric;
    String   outputUnit;
    int      deriveKind;              // 0=computed, 1=aggregated, 2=delta

    // v2.4: list of alias_metric (string) — khớp MetricEvent.metricName;
    // path scope do scopePathAliasId quyết định
    List<String> inputMetrics;

    String   expression;              // cho deriveKind=0; null cho 1,2
    Integer  windowSeconds;           // cho deriveKind=1
    Integer  aggFunction;             // 0=avg,1=max,2=min,3=sum,4=rate
    Integer  assemblyWindowSeconds;   // cho deriveKind=0; null = dùng mặc định hệ thống

    // derive_kind=2 (Delta):
    double   deltaScaleFactor;        // default=1.0
    Long     deltaResetThreshold;     // null = no reset detection

    Map<String, Double> staticConstants;  // key = alias_metric string (chỉ deriveKind=0)
    List<Integer> scopeDeviceIds;    // null = all
    Integer  scopePathAliasId;       // null = mọi path; NOT NULL = khoá rule + input matching vào path này (§3.1)
    int      priority;
}

// DeltaState: keyed state cho derive_kind=2 (Delta)
class DeltaState {
    double prevValue;     // giá trị sample trước
    long   prevTimeMs;    // event_time của sample trước (millisecond)
    // TTL: cấu hình theo StateTtlConfig, đề xuất = 3 × router_sample_interval
}
```

---

## 5. Các mô hình derive

### 5.1 Computed (derive_kind = 0)

#### Mô tả

Tính một giá trị mới từ một hoặc nhiều metric đầu vào có **cùng** `(device_id, labels)`. Các input có thể đến từ nhiều Kafka message khác nhau và **lệch nhau về thời gian**. Engine gom chúng vào một buffer theo key, **chờ đủ bộ trong một cửa sổ thời gian** (`assembly_window_seconds`), rồi mới eval expression. Đây là mô hình cho mọi phép tính "gom đủ input rồi tính", kể cả khi có đúng 2 input đến lệch thời gian.

#### Ví dụ rule

```json
{
  "rule_name":       "Inbound interface utilization",
  "output_metric":   "if_in_utilization_pct",
  "output_unit":     "%",
  "derive_kind":     0,
  "input_metrics":   ["if_in_octets", "if_speed"],
  "expression":      "if_in_octets * 8.0 / if_speed * 100",
  "static_constants": { "if_speed": 10000000000 },
  "assembly_window_seconds": 90,
  "scope_path_alias_id": 5
}
```

Biến trong expression là `alias_metric` của input (`if_in_octets`, `if_speed`) — đọc trực tiếp, không cần resolve.

#### Hai khái niệm thời gian khác nhau — KHÔNG được nhầm

Đây là điểm dễ hiểu sai nhất của Computed. Có **hai** mốc thời gian độc lập:

| Khái niệm | Ý nghĩa | Giá trị đề xuất | Vai trò |
| --- | --- | --- | --- |
| **`assembly_window_seconds`** (cửa sổ gom) | Tính từ `firstEventTimeMs` — thời điểm metric **đầu tiên** của một chu kỳ rơi vào buffer. Trong khoảng này engine chờ các input còn lại về. | ~1.5 × router_sample_interval ≈ **90s** | **Quyết định "chờ bao lâu".** Đủ bộ trong window → eval ngay. Hết window mà chưa đủ → xử lý exception (bảng dưới). |
| **State TTL** | Tuổi thọ tối đa của keyed state trước khi Flink tự xoá. | **> assembly_window**, vd **120s** | **Chỉ là lưới an toàn** dọn buffer mồ côi (vd rule bị xoá, key không bao giờ đủ bộ). KHÔNG dùng để quyết định đủ bộ. |

> **Vì sao tách bạch:** nếu để TTL per-entry đóng cả hai vai, entry đến sớm sẽ hết hạn trước entry đến muộn, nên "đủ bộ hay chưa" lệ thuộc thứ tự đến của input — rất khó suy luận và dễ mất dữ liệu hợp lệ. Vì vậy thời gian chờ đủ bộ được đo từ **một mốc duy nhất** (`firstEventTimeMs`) áp cho **cả batch**, không phải theo từng entry. TTL chỉ là tầng dọn rác đặt rộng hơn để không giữ state vô hạn.

`assembly_window_seconds` là tham số **per-rule** (cho phép rule trộn input lệch chu kỳ đặt window rộng hơn); nếu rule không khai báo, dùng mặc định hệ thống (≈ 1.5 × sample interval).

#### static_constants — ngữ nghĩa "có sẵn"

Một input được coi là **đã có** khi thoả MỘT trong hai:

1. Metric thực của nó về trong cửa sổ gom, **hoặc**
2. Có `static_constants[alias_metric]` cấu hình (hằng số dùng làm giá trị thay thế).

Nếu một input **không** có hằng số cấu hình **và** metric thực không về trong cửa sổ ⇒ đó là **thiếu dữ liệu** ⇒ **không emit** (không có chuyện "điền 0" hay "bỏ biến"). `static_constants` là **cách duy nhất** để một input vắng mặt vẫn cho phép tính — và chỉ khi operator chủ động cấu hình.

#### Cơ chế hoạt động

```
Keyed by: (device_id, rule_id, labels_fingerprint)

State: Map<String, Double>  → { alias_metric → value } các metric thực đã nhận
       long firstEventTimeMs → event_time của record ĐẦU TIÊN vào batch
       TTL: > assembly_window (mặc định 120s)  ← chỉ là lưới an toàn

Cửa sổ gom: đóng tại firstEventTimeMs + assembly_window_seconds (event-time)
```

Pseudo-code operator:

```
class ComputedOperator extends KeyedBroadcastProcessFunction:

    processElement(event, broadcastCtx, out):
        rules = broadcastCtx.getBroadcastState(RULE_STATE)
            .filter(r => r.deriveKind == 0 && matchesInput(event, r))

        for each rule in rules:
            if not inScope(rule, event): continue

            matchedAlias = event.metricName          // 3.3: không trùng tên trong rule
            if event.valueType != 1:                 // input của computed phải là number
                continue                             // bỏ qua string/bool
            value = event.valueNumber
            if value is NaN:                         // giá trị bẩn → coi như chưa đến
                continue

            stateKey = (event.deviceId, rule.id, fingerprintLabels(event.labels))
            buffer = getKeyedState(stateKey)

            if buffer.isEmpty():
                buffer.setFirstEventTime(event.eventTimeMs)
                // đăng ký event-time timer đóng cửa sổ gom
                ctx.timerService().registerEventTimeTimer(
                    event.eventTimeMs + rule.assemblyWindowSeconds * 1000)

            buffer.put(matchedAlias, value)          // last-write-wins nếu trùng metric

            // resolveInputs: hợp nhất buffer + static_constants
            resolved = resolveInputs(rule, buffer)
            if resolved.isComplete():                // đủ MỌI input (thực hoặc hằng)
                emitIfFinite(rule, event, buffer, resolved, out)
                clear(stateKey)                      // huỷ luôn timer còn treo

    onTimer(timestamp, ctx, out):                    // cửa sổ gom đóng mà CHƯA đủ bộ
        rule, stateKey = lookup(ctx)
        buffer = getKeyedState(stateKey)
        missing = rule.inputMetrics
                    - buffer.keys()
                    - rule.staticConstants.keys()
        // thiếu input động không có hằng thay thế → KHÔNG emit
        log.debug("incomplete derived rule={}, device={}, labels={}, missing={}",
                  rule.id, deviceId, labels, missing)
        metric.inc("derived_incomplete_total", {rule_id: rule.id})
        clear(stateKey)

    emitIfFinite(rule, event, buffer, resolved, out):
        result = eval(rule.expression, resolved)
        if result is NaN or Infinity:
            log.warn("non-finite result rule={}, skip", rule.id)
            return                                   // không emit
        // event_time = event_time của record ĐẦU TIÊN vào batch (xem §6.1)
        out.collect(buildDerivedEvent(event, rule, result, buffer.getFirstEventTime()))

    processBroadcastElement(ruleUpdate, ctx, out):
        ctx.getBroadcastState(RULE_STATE).put(ruleUpdate.id, ruleUpdate)
```

> Dùng **event-time timer** (không phải processing-time) để cửa sổ gom nhất quán với watermark và chịu được late/replay khi job restart từ checkpoint.

#### Xử lý exception khi metric không về đủ

Nguyên tắc xuyên suốt (§1.2): **thiếu input thì im lặng bỏ qua emit, không bao giờ emit giá trị sai.**

| Tình huống | Hành vi |
| --- | --- |
| Đủ bộ trong cửa sổ gom | Eval, emit, clear buffer (huỷ timer). Đường đi chuẩn. |
| Một phần input thiếu nhưng **có `static_constants`** | Hằng số được coi như đã có ngay từ đầu; chỉ cần các input *động* còn lại về đủ là tính. |
| Hết cửa sổ gom, thiếu input động **không** có hằng thay thế | **Không emit.** Clear buffer. Log **DEBUG** kèm `rule_id, device_id, labels, missing`. Metric thưa là bình thường, không phải lỗi. |
| Thiếu kéo dài nhiều chu kỳ | Vẫn không emit. Tăng counter `derived_incomplete_total{rule_id}` để soi rule cấu hình sai (vd khai báo input không bao giờ tồn tại). Tín hiệu vận hành, không phải alert nội dung. |
| Input về nhưng `value` null / NaN / sai kiểu | Không nạp vào buffer (coi như chưa đến). Log WARN nếu lặp lại. |
| Eval ra NaN/Infinity dù đủ bộ (vd chia 0) | Không emit, clear buffer, log WARN. |
| Input trùng (cùng `alias_metric` về 2 lần trước khi đủ bộ) | **Last-write-wins**: giữ giá trị mới nhất. Nhất quán ngữ nghĩa "giá trị mới nhất mỗi metric". |
| Input đến **sau** khi đã eval & clear (late) | Mở batch mới cho chu kỳ kế (đặt lại `firstEventTimeMs` + timer mới). Không hồi tố kết quả đã emit. |
| State TTL hết giữa chừng (lưới an toàn) | Tương đương "hết cửa sổ thiếu input" → không emit, dọn state. |
| Job restart từ checkpoint | Buffer + timer phục hồi; cửa sổ gom tiếp tục theo event-time. |

**Lưu ý expression evaluation:** chạy trong sandbox (không I/O, không reflection, có timeout). Dev tự chọn thư viện (Spring SpEL, MVEL, Janino).

### 5.2 Aggregated (derive_kind = 1)

#### Mô tả

Tổng hợp giá trị của một metric duy nhất trong một tumbling window. Emit một giá trị tổng hợp tại thời điểm window đóng.

#### Ví dụ rule

```json
{
  "rule_name":      "Average CPU over 5 minutes",
  "output_metric":  "avg_cpu_5min",
  "output_unit":    "%",
  "derive_kind":    1,
  "input_metrics":  ["cpu_utilization_5min"],
  "window_seconds": 300,
  "agg_function":   0
}
```

Không đặt `scope_path_alias_id` (NULL) → gộp mọi sample CPU bất kể path scope.

#### `agg_function` mapping

| Giá trị | Tên | Logic |
|---------|-----|-------|
| `0` | avg | sum(values) / count |
| `1` | max | giá trị lớn nhất trong window |
| `2` | min | giá trị nhỏ nhất trong window |
| `3` | sum | tổng tất cả giá trị |
| `4` | rate | (last_value − first_value) / window_duration_seconds |

#### Pseudo-code operator

```
Stream<MetricEvent> aggregatedInput =
    sourceStream
        .filter(e => anyAggRuleMatches(e))   // dùng matchesInput
        // PHẢI key theo rule_id để mỗi aggregated rule có cửa sổ/state riêng
        // (một metric có thể bị nhiều aggregated rule tham chiếu với window_seconds khác nhau).
        .keyBy(e => (ruleId, e.deviceId, e.metricName, e.pathId, fingerprintLabels(e.labels)))
        .window(TumblingEventTimeWindows.of(Time.seconds(rule.windowSeconds)))
        .aggregate(new DerivedAggregateFunction(rule))

class DerivedAggregateFunction implements AggregateFunction:

    Accumulator: { sum: Double, count: Long, min: Double, max: Double,
                   firstValue: Double, lastValue: Double,
                   firstTime: Long,  lastTime: Long,
                   deviceId: Int, deviceName: String, labels: Map }

    add(event, acc):
        acc.sum   += event.valueNumber
        acc.count += 1
        acc.min    = min(acc.min, event.valueNumber)
        acc.max    = max(acc.max, event.valueNumber)
        if acc.count == 1: acc.firstValue = event.valueNumber; acc.firstTime = event.eventTimeMs
        acc.lastValue = event.valueNumber; acc.lastTime = event.eventTimeMs

    getResult(acc) → MetricEvent:
        result = switch(rule.aggFunction):
            case 0: acc.sum / acc.count
            case 1: acc.max
            case 2: acc.min
            case 3: acc.sum
            case 4: (acc.lastValue - acc.firstValue)
                    / ((acc.lastTime - acc.firstTime) / 1000.0)
        return buildDerivedEvent(acc, rule, result, windowCloseTimeMs)
```

**Watermark strategy:**

```
WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(
        max(30, rule.windowSeconds / 4)
    ))
    .withIdleness(Duration.ofSeconds(60))
```

`event_time` của derived record = thời điểm window đóng (window end timestamp).

#### 5.2.1 Aggregated với rule động — ràng buộc & hai cách hiện thực

`TumblingEventTimeWindows.of(Time.seconds(...))` nhận **một** kích thước window cố định **lúc dựng job graph** — nó **không** đọc được `window_seconds` từ broadcast state theo từng rule. Vì Job 2 cam kết "nhận rule mới không cần restart" (§1.2), aggregated cần một trong hai cách:

| Cách | Mô tả | Đánh đổi |
| --- | --- | --- |
| **(A) Manual windowing trong `KeyedBroadcastProcessFunction`** (khuyến nghị — nhất quán computed/delta) | Key `(rule_id, device_id, metric_name, path_id, labels_fingerprint)`. Tự gom vào accumulator theo `floor(eventTime / window_seconds)`, đăng ký **event-time timer** tại biên window để emit. `window_seconds` đọc từ rule trong broadcast → **rule mới/đổi window có hiệu lực ngay** ở chu kỳ refresh kế. | Tự quản lý accumulator + timer (nhiều code hơn), nhưng đồng nhất với computed/delta và **đúng** cam kết rule động. |
| **(B) Native `TumblingEventTimeWindows`** | Đơn giản, dùng API window sẵn có. | `window_seconds` cố định lúc build; **thêm rule aggregated có window mới ⇒ phải đổi job graph (redeploy)**. Chỉ chấp nhận nếu tập window là hữu hạn & ít đổi (vd chỉ 60s/300s/3600s, dựng sẵn một WindowedStream cho mỗi giá trị). |

> **Khuyến nghị:** dùng **(A)** để aggregated nhất quán với computed/delta (đều là `KeyedBroadcastProcessFunction` + event-time timer) và giữ đúng cam kết rule động. Nếu chọn **(B)**, tài liệu hoá rõ rằng aggregated rule với `window_seconds` mới cần redeploy, và `derive_kind=1` không thuộc nhóm "rule mới có hiệu lực ≤ 60s" như computed/delta.

### 5.3 Delta (derive_kind = 2)

#### Mô tả

Tính toán **tốc độ thay đổi (rate of change)** của một counter metric dựa trên sự khác biệt giữa sample hiện tại và sample trước trên cùng key `(device_id, labels)`. Đây là pattern chuẩn để chuyển monotonically-increasing counter (bytes, packets, errors) thành rate metric (bps, pps, eps).

Công thức:

```
output = (current_value − prev_value) * delta_scale_factor / Δt_seconds
```

Trong đó `Δt_seconds = (current_event_time_ms - prev_event_time_ms) / 1000.0`.

Ví dụ: `if_in_octets` (bytes, tăng dần) → `if_in_bps` (bits per second):

```
if_in_bps = (bytes_now - bytes_prev) * 8 / Δt_seconds
```

Router emit mỗi 60 giây nên `Δt_seconds ≈ 60`. Tuy nhiên Flink **không hard-code** interval này mà tính từ chính `event_time` của hai sample thực tế, đảm bảo chính xác khi có jitter hay late arrival.

#### Phân biệt với `agg_function=4` (rate) trong Aggregated

| Tiêu chí | Delta (derive_kind=2) | Aggregated rate (derive_kind=1, agg_function=4) |
|---|---|---|
| Đơn vị thời gian | Khoảng cách thực giữa 2 sample (Δt từ event_time) | Kích thước window cố định (`window_seconds`) |
| Độ trễ emit | ~1 sample interval (ngay khi sample kế đến) | `window_seconds` + watermark delay |
| Phù hợp cho | Monitoring real-time, alerting (latency thấp) | Trend analysis, capacity planning |
| Counter reset | Phát hiện được qua `delta_reset_threshold` | Không xử lý riêng |
| Input | Luôn 1 metric | Luôn 1 metric |

#### Ví dụ rule

```json
{
  "rule_name":             "Inbound interface throughput Mbps",
  "output_metric":         "if_in_mbps",
  "output_unit":           "Mbps",
  "derive_kind":           2,
  "input_metrics":         ["if_in_octets"],
  "scope_path_alias_id":   5,
  "delta_scale_factor":    0.000008,
  "delta_reset_threshold": 4294967295
}
```

`delta_scale_factor = 8 / 1_000_000 = 0.000008`: nhân 8 để đổi bytes → bits, chia 1_000_000 để bits/s → Mbps (`Δt` đã tính bằng giây ở mẫu số).

`delta_reset_threshold = 4294967295` (2^32 − 1): wrap-around point của 32-bit counter. Khi `current < prev` và `current < threshold`, coi là reset, bỏ qua sample.

#### Cơ chế hoạt động

```
Keyed by: (device_id, rule_id, labels_fingerprint)

State: DeltaState { prevValue: Double, prevTimeMs: Long }
       TTL: 3 × router_sample_interval (mặc định 180s)
       → nếu TTL hết (không có sample trong 3 chu kỳ), state bị xoá;
         sample kế tiếp sau đó coi như "sample đầu tiên" và không emit.
```

Pseudo-code operator:

```
class DeltaOperator extends KeyedBroadcastProcessFunction:

    processElement(event, broadcastCtx, out):
        rules = broadcastCtx.getBroadcastState(RULE_STATE)
            .filter(r => r.deriveKind == 2 && matchesInput(event, r))

        for each rule in rules:
            if not inScope(rule, event): continue

            stateKey = (event.deviceId, rule.id, fingerprintLabels(event.labels))
            state = getKeyedState(stateKey)  // DeltaState

            currentValue = event.valueNumber
            currentTimeMs = event.eventTimeMs

            if state.isEmpty():
                // Chưa có sample trước — lưu sample này, chờ sample kế
                state.set(DeltaState { prevValue=currentValue, prevTimeMs=currentTimeMs })
                continue  // không emit

            prevValue = state.prevValue
            prevTimeMs = state.prevTimeMs

            // ── Phát hiện counter reset ────────────────────────────────
            if currentValue < prevValue:
                if rule.deltaResetThreshold != null
                   && currentValue < rule.deltaResetThreshold:
                    log.warn("Counter reset detected rule={}, device={}, labels={}",
                             rule.id, event.deviceId, event.labels)
                    state.set(DeltaState { prevValue=currentValue, prevTimeMs=currentTimeMs })
                    continue  // không emit
                // deltaResetThreshold=null hoặc current >= threshold:
                // Không phải reset — gauge metric giảm; tính delta bình thường (âm hợp lệ).

            // ── Tính toán ──────────────────────────────────────────────
            deltaValue = currentValue - prevValue
            deltaTimeSeconds = (currentTimeMs - prevTimeMs) / 1000.0

            if deltaTimeSeconds <= 0:
                log.warn("Non-positive Δt rule={}, skipping", rule.id)
                state.set(DeltaState { prevValue=currentValue, prevTimeMs=currentTimeMs })
                continue

            result = deltaValue * rule.deltaScaleFactor / deltaTimeSeconds

            // ── Emit ───────────────────────────────────────────────────
            // event_time = sample hiện tại (điểm cuối của khoảng delta)
            out.collect(buildDerivedEvent(event, rule, result, currentTimeMs))

            state.set(DeltaState { prevValue=currentValue, prevTimeMs=currentTimeMs })

    processBroadcastElement(ruleUpdate, ctx, out):
        ctx.getBroadcastState(RULE_STATE).put(ruleUpdate.id, ruleUpdate)
```

#### Các trường hợp biên cần xử lý

| Tình huống | Hành vi |
|---|---|
| Sample đầu tiên (state trống) | Lưu state, không emit. Emit bắt đầu từ sample thứ hai. |
| `deltaTimeSeconds <= 0` (out-of-order, same timestamp) | Bỏ qua emit, cập nhật state với sample mới hơn. Log WARN. |
| Counter reset (`current < prev` và `current < reset_threshold`) | Bỏ qua sample, khởi động lại tracking từ current. Log WARN. |
| `delta_reset_threshold = null` và `current < prev` | Tính delta bình thường (kết quả âm — hợp lệ với gauge). |
| State TTL hết (không có sample trong 3 chu kỳ) | Flink tự expire state. Sample kế coi như "đầu tiên", không emit. |
| Job restart từ checkpoint | State phục hồi; delta tiếp tục bình thường từ sample cuối trước restart. |

---

## 6. Schema đầu ra

### 6.1 Derived metric row trong `ipms.tlm_metrics`

| Field | Giá trị cho derived row | Giải thích |
|-------|------------------------|-----------|
| `raw_path` | `"derived"` | Giá trị cố định, không phải YANG path |
| `raw_metric_name` | `"derived:<rule_id>"` | Ví dụ: `"derived:17"` cho rule id=17 |
| `metric_name` | `rule.outputMetric` | Ví dụ: `"if_in_utilization_pct"`, `"if_in_mbps"` |
| `path_id` | `0` | Không có path alias rule nào match |
| `value_type` | `1` (number) | Derived metrics luôn là số |
| `value_number` | Kết quả tính toán | |
| `unit` | `rule.outputUnit` | Ví dụ: `"%"`, `"Mbps"`, `"pps"` |
| `labels` | Kế thừa từ source event | Giữ nguyên labels của metric đầu vào |
| `event_time` | Tuỳ theo derive_kind (xem bảng dưới) | |
| `receive_time` | Thời điểm Flink emit derived row | |
| `device_id` | Kế thừa từ source event | |
| `device_name` | Kế thừa từ source event | |

**`event_time` theo derive_kind:**

| derive_kind | event_time |
|-------------|-----------|
| 0 (computed) | `event_time` của record **đầu tiên** vào batch (`firstEventTimeMs`) — mốc bắt đầu cửa sổ gom |
| 1 (aggregated) | Thời điểm window đóng (window end) |
| 2 (delta) | `event_time` của sample hiện tại (điểm cuối của khoảng delta) |

> Derived row luôn lấy thời gian từ **một input đại diện cụ thể** (không nội suy/trung bình). `receive_time` luôn là lúc Flink emit, nên độ trễ xử lý = `receive_time − event_time`. Labels và `device_id`/`device_name` **kế thừa nguyên vẹn** từ source event; vì mọi input của một rule được key theo cùng `(device_id, labels_fingerprint)` nên labels của chúng giống nhau, không phát sinh mâu thuẫn.

### 6.2 Phân biệt derived metric trên UI

```sql
-- Tất cả derived metrics của một device
SELECT event_time, metric_name, value_number, unit, labels
  FROM ipms.tlm_metrics
 WHERE device_name = 'HAN-PE-01'
   AND raw_path    = 'derived'
   AND event_time >= now() - INTERVAL 1 HOUR
 ORDER BY event_time DESC;

-- Delta metrics cụ thể trên toàn fleet (throughput Mbps)
SELECT device_name,
       labels['if_name']      AS interface,
       avg(value_number)      AS avg_mbps,
       max(value_number)      AS peak_mbps
  FROM ipms.tlm_metrics
 WHERE metric_name = 'if_in_mbps'
   AND raw_path    = 'derived'
   AND event_time >= now() - INTERVAL 5 MINUTE
 GROUP BY device_name, interface
 ORDER BY peak_mbps DESC;

-- So sánh raw counter và derived rate cùng lúc
SELECT event_time, metric_name, value_number, unit,
       raw_path = 'derived' AS is_derived
  FROM ipms.tlm_metrics
 WHERE device_name = 'HAN-PE-01'
   AND metric_name IN ('if_in_octets', 'if_in_mbps')
   AND event_time >= now() - INTERVAL 1 HOUR
 ORDER BY event_time, metric_name;
```

### 6.3 Kafka output `derived_metrics` (v2.5 — BẮT BUỘC)

Streaming Alert Engine (Job 3) **không đọc ClickHouse** (poll engine cũ đã bị loại bỏ). Do đó Job 2 **phải** publish mỗi derived row ra Kafka topic **`derived_metrics`** để Job 3 alert real-time trên derived metric.

**Topic & lý do tách:**
- Topic **riêng** `derived_metrics`. **KHÔNG** ghi vào `processed_metrics` — đó là **input** của Job 2, ghi vào sẽ khiến Job 2 tự consume lại output của mình (vòng lặp / derived-of-derived). Job 3 union hai topic (`processed_metrics` cho raw, `derived_metrics` cho derived).

**Message format = `MetricEvent`** (giống `processed_metrics`, §4.2) — Job 3 deserialize chung một POJO:

| Field | Giá trị cho derived |
|-------|---------------------|
| `eventTimeMs` | theo `derive_kind` (§6.1: computed=firstEventTime, aggregated=window end, delta=sample hiện tại) |
| `receiveTimeMs` | lúc Job 2 emit |
| `deviceId` / `deviceName` | kế thừa source event |
| `pathId` | `0` (derived không có path alias) |
| `rawPath` | `"derived"` |
| `rawMetricName` | `"derived:<rule_id>"` |
| `metricName` | `rule.outputMetric` (= `alias_metric` mà alert rule tham chiếu, với `path_alias_id=0`) |
| `valueType` | `1` |
| `valueNumber` | kết quả tính |
| `unit` | `rule.outputUnit` |
| `labels` | kế thừa source event |

**Quy ước & ngữ nghĩa:**
- **Kafka key** đề xuất `(deviceId, metricName, labels_fingerprint)` để giữ thứ tự per-series (Job 3 cần thứ tự cho prev-sample/delta của chính derived metric nếu có pct/delta rule trên derived).
- **Delivery:** **at-least-once** (nhất quán với toàn pipeline). Trùng khi retry/replay được Job 3 + NOC PRO xử lý idempotent ở tầng alert; bản thân metric trùng trên `derived_metrics` được Job 3 khử như sample đảo/late (§5.3 của Job 3 HLD).
- **Khớp định danh alert:** alert rule định danh metric bằng `(alias_metric, path_alias_id)` và **derived metric dùng `path_alias_id=0`** (Alert Engine HLD). Vì derived row đặt `pathId=0` và `metricName=outputMetric`, Job 3 match `aliasMetric==metricName` và `pathAliasId∈{null,0}` là khớp — không cần ánh xạ phụ.

> **Tương thích ClickHouse:** việc thêm Kafka sink **không** đổi gì ở ClickHouse sink hay schema `tlm_metrics`. Dashboard/truy vấn derived (§6.2) giữ nguyên.

---

## 7. REST API quản lý rule

Spring Boot cung cấp CRUD API cho `tlm_derived_rules`. API là kênh duy nhất để operator quản lý derived rule tại runtime. Flink Job 2 **không** gọi API trực tiếp — nó load + refresh snapshot rule từ MariaDB định kỳ (Section 3.4). Vì vậy hợp đồng quan trọng nhất của API là: **mọi write phải tất định, validate đúng ràng buộc bảng, và làm `updated_at` tiến lên để chu kỳ refresh kế của Flink pick up thay đổi.**

### 7.0 Quy ước chung

- **Base path:** `/api/v1/telemetry`
- **Định dạng:** request/response JSON, `Content-Type: application/json; charset=utf-8`.
- **Auth & RBAC:** Bearer token; tối thiểu tách quyền read vs write. Mọi write ghi `created_by` từ token.
- **Transaction + versioning bằng `updated_at` (bắt buộc — theo MariaDB v4.2):** mỗi write (POST/PUT/PATCH/DELETE) chạy trong **một transaction**: INSERT/UPDATE row trong `tlm_derived_rules` — `updated_at` tự bump nhờ `ON UPDATE CURRENT_TIMESTAMP(6)`; trường hợp hiếm hai write cực gần nhau trong cùng micro-giây, đặt `updated_at = GREATEST(updated_at + INTERVAL 1 MICROSECOND, CURRENT_TIMESTAMP(6))` để bảo đảm đơn điệu tuyệt đối. *(v2.8: không còn bước reset `pushed_at=NULL` — bảng này không có ClickHouse mirror.)*
- **Soft-delete:** DELETE = set `status = 0` (Deprecated), **không** physical DELETE — giữ audit; `updated_at` vẫn tiến lên. (Phù hợp `UNIQUE KEY uq_derived_output (output_metric, status)`: cho phép tạo lại output_metric mới sau khi bản cũ đã Deprecated.)
- **Hiệu lực trong Flink:** sau khi write thành công, thay đổi có hiệu lực sau **một chu kỳ refresh** (Section 3.4; SLO ≤ 60s — Section 9). API không đẩy trực tiếp; phản hồi nên nêu rõ độ trễ này (xem §7.6).
- **Mã trạng thái:** `200` OK, `201` Created, `204` No Content (DELETE), `400` sai cú pháp, `401/403` auth, `404` không thấy, `409` xung đột unique (`output_metric` trùng) / `If-Match` (`updated_at`) mismatch, `422` vi phạm ràng buộc ngữ nghĩa (vd `derive_kind=1` thiếu `window_seconds`), `500` lỗi máy chủ.
- **Concurrency:** PUT/PATCH hỗ trợ `If-Match: <updated_at>` (giá trị lấy từ response trước đó); nếu `updated_at` hiện tại khác → `409` (chống ghi đè đồng thời).

### 7.1 Bảng endpoint

| Method & Path | Mục đích |
| --- | --- |
| `GET /derived-rules` | Liệt kê rule (lọc + phân trang) |
| `GET /derived-rules/{id}` | Chi tiết một rule |
| `POST /derived-rules` | Tạo rule (mọi `derive_kind`) |
| `PUT /derived-rules/{id}` | Cập nhật toàn bộ (full replace) |
| `PATCH /derived-rules/{id}` | Cập nhật một phần (vd chỉ `priority`, `status`) |
| `DELETE /derived-rules/{id}` | Soft-delete (`status=0`) |
| `POST /derived-rules/preview` | Dry-run: eval expression / tính delta với input mẫu, không lưu |
| `GET /metric-aliases` | Lookup `alias_metric` theo path — hỗ trợ UI quyết định `scope_path_alias_id` (§7.8) |

### 7.2 List, lọc, phân trang — `GET /derived-rules`

Query params: `status` (mặc định `1`), `derive_kind`, `output_metric` (khớp chính xác), `input_alias_metric` (lọc rule có input chứa alias_metric này), `scope_path_alias_id`, `q` (tìm theo `rule_name`), `page` (mặc định 1), `page_size` (mặc định 50, tối đa 200), `sort` (vd `-updated_at`, `-priority`).

Response:

```json
{
  "page": 1,
  "page_size": 50,
  "total": 87,
  "items": [
    {
      "id": 55,
      "rule_name": "Inbound interface throughput Mbps",
      "output_metric": "if_in_mbps",
      "output_unit": "Mbps",
      "derive_kind": 2,
      "input_metrics": ["if_in_octets"],
      "delta_scale_factor": 0.000008,
      "delta_reset_threshold": 4294967295,
      "scope_path_alias_id": 5,
      "priority": 100,
      "status": 1,
      "updated_at": "2026-06-03T08:00:00.000000+07:00"
    }
  ]
}
```

### 7.3 Tạo rule — `POST /derived-rules`

`input_metrics` nhận **array of string** (`alias_metric`). Path scope đặt ở `scope_path_alias_id` cấp rule (NULL = mọi path) — xem §3.1.

**Delta (`derive_kind=2`):**

```json
{
  "rule_name":             "Inbound interface throughput Mbps",
  "output_metric":         "if_in_mbps",
  "output_unit":           "Mbps",
  "derive_kind":           2,
  "input_metrics":         ["if_in_octets"],
  "scope_path_alias_id":   5,
  "delta_scale_factor":    0.000008,
  "delta_reset_threshold": 4294967295
}
```

**Computed (`derive_kind=0`):**

```json
{
  "rule_name":       "Inbound interface utilization",
  "output_metric":   "if_in_utilization_pct",
  "output_unit":     "%",
  "derive_kind":     0,
  "input_metrics":   ["if_in_octets", "if_speed"],
  "expression":      "if_in_octets * 8.0 / if_speed * 100",
  "static_constants": { "if_speed": 10000000000 },
  "assembly_window_seconds": 90,
  "scope_path_alias_id": 5
}
```

> `static_constants` cấu hình cho `if_speed` nên dù metric `if_speed` không về trên stream, rule vẫn tính được khi `if_in_octets` về. Nếu một input **không** có hằng số và metric không về trong `assembly_window_seconds` ⇒ coi là thiếu dữ liệu ⇒ không emit (§5.1).

**Aggregated (`derive_kind=1`):**

```json
{
  "rule_name":      "CPU trung bình 5 phút",
  "output_metric":  "avg_cpu_5min",
  "output_unit":    "%",
  "derive_kind":    1,
  "input_metrics":  ["cpu_utilization_5min"],
  "window_seconds": 300,
  "agg_function":   0
}
```

> Các loại còn lại đã trình bày ở §5. `derive_kind` chỉ nhận `0` (computed), `1` (aggregated), `2` (delta).

Response `201 Created`:

```json
{
  "id":         55,
  "status":     1,
  "created_at": "2026-06-03T08:00:00.000000+07:00",
  "updated_at": "2026-06-03T08:00:00.000000+07:00"
}
```

### 7.4 Validation bắt buộc tại API

Trả `400` (sai cú pháp) hoặc `422` (vi phạm ngữ nghĩa) với body lỗi có cấu trúc (xem §7.9):

**Chung cho mọi rule:**
- `output_metric` **unique** trong toàn bộ active `tlm_derived_rules` (theo `uq_derived_output`) **và** không trùng bất kỳ `alias_metric` nào trong `tlm_metric_aliases` (tránh đụng tên với raw metric). Trùng → `409`.
- `input_metrics` là array of string, các phần tử **đôi một khác nhau** (trùng → `422`). Mỗi `alias_metric` phải tồn tại trong `tlm_metric_aliases` `status=1` ở ≥ 1 row. Khi `scope_path_alias_id` NOT NULL: mỗi `alias_metric` phải tồn tại ở row path-scoped đúng path đó (hoặc row any-path tương thích) — nếu không sẽ không bao giờ match event.
- **Cảnh báo nhập nhằng (v2.4):** khi `scope_path_alias_id = null` mà một `alias_metric` đang tồn tại ở **≥ 2 path khác nhau** trong `tlm_metric_aliases`, API vẫn cho lưu nhưng trả kèm `warnings` khuyến nghị đặt `scope_path_alias_id` (hoặc rà soát naming convention) — xem §3.1, §7.8.
- `scope_path_alias_id` (nếu khác null) phải trỏ tới `tlm_path_aliases.id` Active. `scope_device_ids` (nếu khác null) là array `device_id` tồn tại trong inventory; **tối đa 20 phần tử** (v2.8) — vượt quá trả `422` với message `"scope_device_ids vượt giới hạn 20 thiết bị; để null để áp toàn bộ thiết bị"`.
- `derive_kind ∈ {0,1,2}`; `priority ≥ 0`.

**Theo `derive_kind` (ánh xạ các CHECK của bảng):**

| `derive_kind` | Bắt buộc | Phải vắng (NULL) | Ràng buộc input |
| --- | --- | --- | --- |
| `0` computed | `expression` | `window_seconds`, `agg_function` | `len(input_metrics) ≥ 1`; các `alias_metric` **đôi một khác nhau**. `assembly_window_seconds` tuỳ chọn (> 0; mặc định hệ thống nếu vắng) |
| `1` aggregated | `window_seconds`, `agg_function` | `expression` | `len(input_metrics) == 1` |
| `2` delta | — (`delta_scale_factor` có default) | `expression`, `window_seconds`, `agg_function` | `len(input_metrics) == 1` |

> **`derive_kind` hợp lệ:** chỉ `0`, `1`, `2`. `POST`/`PUT` với giá trị khác (vd `3`) trả `422`. Schema DB ràng buộc `CHECK derive_kind IN (0,1,2)`.

**Riêng expression (`derive_kind=0`):**
- Parse được; biến trong expression **khớp đúng** tập `alias_metric` của input (cộng key trong `static_constants` nếu có).
- Chạy trong sandbox: không I/O, không reflection, không chứa ký tự/cú pháp nguy hiểm. API nên thử parse + eval khô với giá trị giả để bắt lỗi sớm.
- **Lý do cấm trùng `alias_metric` trong cùng rule:** biến expression là `alias_metric` (string); hai input trùng tên (dù khác `path_alias_id`) tạo hai biến không phân biệt được (Section 3.3).

**Riêng delta (`derive_kind=2`):**
- `delta_scale_factor` > 0 (đề xuất); `delta_reset_threshold` null = metric là gauge (không xử lý wrap-around), NOT NULL = counter (phát hiện reset).

### 7.5 Cập nhật & xoá — `PUT` / `PATCH` / `DELETE`

- `PUT /derived-rules/{id}`: thay toàn bộ body (mọi field như khi tạo), validate lại đầy đủ. Tùy chọn `If-Match: <updated_at>` → mismatch trả `409`. `updated_at` tự bump.
- `PATCH /derived-rules/{id}`: chỉ field gửi lên. Ví dụ bật/tắt nhanh hoặc đổi ngưỡng:

```json
{ "status": 0 }
```

```json
{ "priority": 120, "delta_reset_threshold": 8589934591 }
```

  Lưu ý: đổi `output_metric` hoặc `input_metrics`/`derive_kind` qua PATCH vẫn phải chạy lại toàn bộ validation §7.4 (PATCH không bỏ qua ràng buộc).
- `DELETE /derived-rules/{id}`: soft-delete (`status=0`), trả `204`. Ở chu kỳ refresh kế, Flink loại rule khỏi snapshot → ngừng emit `output_metric` đó; delta/window state tự expire theo TTL (Section 8). Derived rows đã ghi trong ClickHouse **không** bị xoá (giữ lịch sử).

> **Cascade với `scope_path_alias_id`:** nếu một Path Alias bị Deprecate ở module Job 1, các derived rule có `scope_path_alias_id` trỏ tới path đó sẽ ngừng match (không còn event qua scope). API nên **cảnh báo** (không tự xoá) danh sách derived rule bị ảnh hưởng khi phát hiện path tham chiếu không còn Active, để operator chủ động xử lý.

### 7.6 Quan sát độ trễ hiệu lực rule (v2.8 — thay thế refresh-status)

> **v2.8:** Endpoint `GET /derived-rules/refresh-status` đã bị bỏ cùng với `pushed_at`. Không còn `rows_pending_push` để báo cáo.

Để biết rule mới đã được Flink Job 2 pick up chưa, dùng các kênh sau:

- **Pipeline Monitor (PL-01):** panel Flink Job 2 hiển thị `last_broadcast_refresh_at` — timestamp lần cuối Job 2 reload snapshot từ MariaDB. Nếu `last_broadcast_refresh_at > updated_at` của rule → rule đã có hiệu lực.
- **Kafka consumer lag (PL-02):** lag thấp trên `processed_metrics` cho thấy Job 2 đang xử lý bình thường.
- **Flink metrics:** `broadcast_state_update_count` per-job tăng sau mỗi refresh cycle.
- **API response field `effective_note`:** mọi write response nên kèm `"effective_note": "Rule sẽ được Job 2 áp dụng trong vòng ≤ 60s (chu kỳ refresh broadcast)."` để operator biết độ trễ.

### 7.7 Preview / dry-run — `POST /derived-rules/preview`

Eval expression hoặc tính delta trên server với input mẫu, trả kết quả **trước khi lưu** (không ghi MariaDB, không ảnh hưởng Flink). Nhận **định nghĩa rule chưa lưu** (hoặc `{ "id": <id> }` để thử rule đã có với input mẫu).

Request — computed:

```json
{
  "derive_kind":   0,
  "expression":    "if_in_octets * 8.0 / if_speed * 100",
  "sample_inputs": { "if_in_octets": 1250000000, "if_speed": 10000000000 }
}
```

Request — delta:

```json
{
  "derive_kind":            2,
  "delta_scale_factor":     0.000008,
  "delta_reset_threshold":  4294967295,
  "sample_inputs": {
    "prev_value":   125000000000,
    "prev_time_ms": 1748918400000,
    "curr_value":   125750000000,
    "curr_time_ms": 1748918460000
  }
}
```

Response:

```json
{ "result": 100.0, "unit": "Mbps", "warnings": [], "errors": [] }
```

> Preview phản ánh **một-rule, một-thời-điểm**; không mô phỏng windowing thực, watermark, hay tương tác `priority` giữa nhiều rule. Với delta, preview tính trên đúng một cặp prev/curr do người dùng cung cấp — không phản ánh phát hiện reset qua chuỗi sample thực.

### 7.8 Lookup metric alias (hỗ trợ UI) — `GET /metric-aliases`

Giúp operator **phát hiện `alias_metric` tồn tại ở nhiều path** để quyết định đặt `scope_path_alias_id` cho rule (hoặc rà soát naming convention). Đây chỉ là tiện ích UI — Flink không phụ thuộc (Section 2.1).

```
GET /api/v1/telemetry/metric-aliases?alias_metric=if_in_octets
```

Response:

```json
{
  "results": [
    { "alias_metric": "if_in_octets", "path_alias_id": 5, "alias_path": "xr_interfaces", "vendor_code": "Cisco" },
    { "alias_metric": "if_in_octets", "path_alias_id": 7, "alias_path": "junos_ifd",     "vendor_code": "Juniper" },
    { "alias_metric": "if_in_octets", "path_alias_id": 9, "alias_path": "nokia_port",    "vendor_code": "Nokia" }
  ]
}
```

Kết quả trên cho thấy `if_in_octets` tồn tại ở 3 path nhưng là **cùng một metric logic** (3 vendor) — trường hợp này để `scope_path_alias_id: null` là đúng (gộp đa-vendor). Ngược lại, nếu cùng tên nhưng **khác nghĩa** giữa các path, operator phải đặt `scope_path_alias_id` cụ thể cho rule (hoặc đổi tên alias để tránh trùng — naming convention §3.1).

### 7.9 Định dạng lỗi

```json
{
  "error": "validation_failed",
  "message": "derive_kind=1 yêu cầu window_seconds và agg_function.",
  "details": [
    { "field": "window_seconds", "reason": "bắt buộc khi derive_kind=1" },
    { "field": "agg_function",   "reason": "bắt buộc khi derive_kind=1" }
  ]
}
```

---

## 8. Xử lý lỗi

| Tình huống | Hành vi |
|-----------|---------|
| Expression eval exception / kết quả NaN/Infinity (div/0, timeout, null) | Log WARN. Bỏ qua emit. Xoá keyed state nếu là computed. Không crash job. |
| **Computed: hết `assembly_window_seconds` mà thiếu input động** (không có `static_constants` thay thế) | **Không emit. Clear buffer (huỷ timer). Log DEBUG kèm `rule_id, device_id, labels, missing`.** Bình thường khi metric thưa. |
| **Computed: thiếu kéo dài nhiều chu kỳ** | Vẫn không emit. Tăng counter `derived_incomplete_total{rule_id}` để soi rule cấu hình sai. |
| **Computed: input trùng trước khi đủ bộ** | Last-write-wins (giữ giá trị mới nhất). |
| Computed: state TTL hết (lưới an toàn, key không bao giờ đủ bộ) | State tự expire. Không emit. Tương đương hết cửa sổ gom. |
| Delta: sample đầu tiên (không có prev state) | Lưu state, không emit (khởi động bình thường). |
| Delta: counter reset phát hiện qua `delta_reset_threshold` | Bỏ qua sample, restart tracking. Log WARN với `rule_id, device_id, labels`. |
| Delta: `deltaTimeSeconds <= 0` | Bỏ qua emit, cập nhật state. Log WARN. Không crash. |
| `input_metrics` tham chiếu `alias_metric` không tồn tại | API validation chặn khi tạo/sửa rule. Flink chỉ đọc rule đã validated. |
| ClickHouse write fail | Retry với exponential backoff. Flink checkpoint để không mất window/delta result khi restart. |
| **Kafka `derived_metrics` write fail (v2.5)** | Retry/backoff trong Kafka sink. **Độc lập** với ClickHouse sink: lỗi Kafka KHÔNG chặn ghi ClickHouse và ngược lại. Backpressure từ sink lỗi sẽ tự làm chậm pipeline (cơ chế Flink) — chấp nhận. At-least-once: có thể trùng message khi retry; Job 3 xử lý như sample đảo/late. |
| Rule load fail (MariaDB không kết nối được) | Giữ snapshot rule cũ, log ERROR, retry ở cycle kế. Flink job tiếp tục với rule cũ. |
| Rule bị xoá (status=0) khi state đang hoạt động | State tự expire theo TTL. Rule không còn trong snapshot → không có emit mới. |
| `output_metric` trùng raw metric alias | API validation chặn. Không thể xảy ra ở Flink. |

---

## 9. Yêu cầu phi chức năng

| Yêu cầu | Giá trị |
|---------|---------|
| Độ trễ emit computed | ≤ router sample interval + processing latency ≈ 60–90s sau khi đủ input |
| Độ trễ emit aggregated | = window_seconds + watermark_delay ≤ window_seconds × 1.25 |
| **Độ trễ emit delta** | **≈ 1 router sample interval (~60s) — emit ngay khi sample kế đến** |
| Throughput input | Consume cùng rate với `processed_metrics` của Job 1 (~50M metrics/phút raw) |
| Rule refresh latency | ≤ 60s kể từ khi operator lưu rule |
| State size (computed buffer) | TTL 120s × cardinality(device × labels) — ~vài trăm MB managed state |
| **State size (delta)** | **TTL 180s × cardinality(device × metric × labels) — với 300 thiết bị, ~100 interface, ~5 delta rules ≈ thêm vài trăm MB managed state** |
| Availability | Flink checkpoint định kỳ (đề xuất mỗi 30s). Restart tự động từ checkpoint. |
| **Kafka `derived_metrics` (v2.5)** | Sink song song ClickHouse; **at-least-once**; key per-series (§6.3). Thêm Kafka producer không làm tăng đáng kể độ trễ emit derived (độ trễ vẫn bị chi phối bởi cửa sổ tính của từng `derive_kind` ở trên). Bắt buộc cho alert derived (Job 3). |
| Audit / observability | Log mỗi emit ở DEBUG. Log bỏ qua (eval fail, reset, state timeout) ở WARN. Flink metrics: counter per `output_metric`. |

---

## 10. Thuật ngữ

| Thuật ngữ | Định nghĩa |
|-----------|-----------|
| `derive_kind` | Loại derived metric: 0=computed, 1=aggregated, 2=delta |
| `output_metric` | `alias_metric` của derived metric — tên ghi vào `ipms.tlm_metrics.metric_name` |
| `input_metrics` | JSON array of string (`alias_metric`). Định danh metric đầu vào, khớp `metric_name` trên stream; path do `scope_path_alias_id` khử nhập nhằng (v2.4). |
| `matchesInput` | Hàm match một MetricEvent với một rule: `metricName ∈ input_metrics` và thoả `scope_path_alias_id` |
| `expression` | Biểu thức tính toán, biến là `alias_metric` của input |
| `static_constants` | Map<alias_metric, Double> — giá trị hằng thay thế cho một input. **Là cách duy nhất để input vắng mặt vẫn cho phép tính.** Input không có hằng và metric không về trong cửa sổ gom ⇒ thiếu dữ liệu ⇒ không emit. |
| `assembly_window_seconds` | (Computed) Cửa sổ chờ gom đủ input, tính từ `firstEventTimeMs`. Quyết định "chờ bao lâu". Mặc định ≈ 1.5 × sample interval (~90s). **Khác với state TTL.** |
| `firstEventTimeMs` | event_time của record đầu tiên vào batch computed; mốc bắt đầu cửa sổ gom và là `event_time` của derived row. |
| `window_seconds` | Kích thước tumbling window (giây) cho aggregated derive |
| `agg_function` | Hàm tổng hợp: avg, max, min, sum, rate |
| `delta_scale_factor` | Hệ số nhân áp lên `(current − prev) / Δt` để đổi đơn vị (vd: `8/1000000` để bytes/s → Mbps) |
| `delta_reset_threshold` | Ngưỡng phát hiện counter wrap-around. `current < prev` và `current < threshold` → reset, không emit. |
| `DeltaState` | Keyed state cho derive_kind=2 (Delta): lưu `prevValue` và `prevTimeMs` của sample trước |
| `Δt` | Khoảng thời gian giữa hai sample liên tiếp, tính từ `event_time` thực tế (không hard-code interval) |
| `scope_path_alias_id` | Giới hạn phạm vi áp dụng của TOÀN rule, đồng thời là cơ chế khử nhập nhằng path cho input matching (v2.4 — NULL = mọi path) |
| labels fingerprint | Chuỗi định danh duy nhất từ toàn bộ labels map, dùng làm key trong keyed state và window |
| state TTL | Thời gian tối đa keyed state tồn tại trước khi tự expire. **Lưới an toàn dọn buffer mồ côi, đặt > assembly_window — không quyết định "đủ bộ".** |
| `derived_incomplete_total` | Counter quan sát: số lần một computed rule hết cửa sổ gom mà chưa đủ input. Cao kéo dài ⇒ rule cấu hình sai. |
| sandbox | Môi trường thực thi expression bị giới hạn: không I/O, không reflection, có timeout |
| `raw_path = "derived"` | Giá trị cố định trong `ipms.tlm_metrics.raw_path` phân biệt derived row với raw row |
| `derived_metrics` (Kafka, v2.5) | Topic Kafka riêng chứa derived row (format `MetricEvent`) cho Streaming Alert Engine (Job 3). KHÔNG dùng `processed_metrics` để tránh Job 2 tự consume vòng lặp. |
