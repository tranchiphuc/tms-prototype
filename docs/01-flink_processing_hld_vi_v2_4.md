# Thiết kế xử lý Flink — Telemetry Pipeline (HLD)

**Phiên bản:** v2.4  
**Loại tài liệu:** High-Level Design  
**Ngôn ngữ xử lý:** Apache Flink (Java/Scala — dev tự quyết định)  
**Phạm vi:** Pipeline telemetry từ Kafka → Flink → ClickHouse / Kafka downstream

> **Mục đích tài liệu:** Mô tả **ý định thiết kế** và **các ràng buộc quan trọng**, không quy định chi tiết implementation. Dev có toàn quyền chọn thư viện, cấu trúc class, và thứ tự bên trong từng bước — miễn đáp ứng các yêu cầu đầu ra và SLO.

---

## Changelog

| Phiên bản | Thay đổi |
| --------- | -------- |
| v2.4 | **[ADDED]** Hỗ trợ giá trị `vendor_code = "OpenConfig"` (đồng bộ MariaDB v4.3). `OpenConfig` không phải vendor thiết bị mà là **data model** vendor-neutral: rule chỉ được xét khi sensor path thuộc model OpenConfig (bắt đầu bằng `openconfig-`) và match **bất kể vendor của thiết bị**. Thứ tự resolve khi nhiều rule cùng match: **vendor của thiết bị → OpenConfig → All** (với metric/label alias thực hiện qua quy ước `priority`: path-scoped=100, any-path=50, OpenConfig=30, All=10). Cập nhật Section 4.4 (ghi chú resolve), Section 8.3 (validation), Thuật ngữ. |
| v2.3 | **[CHANGED]** Bỏ cơ chế `revision` / `tlm_revision_counter` (đồng bộ với MariaDB v4.2 + ClickHouse v4.2 đã loại bỏ). Versioning/ordering chuyển sang dùng `updated_at` (DATETIME(6), `ON UPDATE CURRENT_TIMESTAMP(6)`): Flink dùng `updated_at` để bỏ qua message stale; sync job dùng `updated_at` làm version cho `ReplacingMergeTree` bên ClickHouse. Cập nhật Section 8 (transaction CRUD bỏ bước update counter; `If-Match` & conflict dùng `updated_at`; `refresh-status` trả `max_updated_at`). `pushed_at` giữ nguyên vai trò. Không thay đổi các giai đoạn xử lý pipeline (Section 4) hay schema `tlm_metrics`. |
| v2.2 | **[ADDED]** Section 8 "REST API quản lý" — đặc tả CRUD đầy đủ cho 4 nhóm rule (Path/Metric/Label Alias, Filter), endpoint chung, quy ước transaction + versioning (`updated_at` từ v2.3, trước đó là `revision`) + `pushed_at`, validation theo ràng buộc bảng, soft-delete & cascade, lọc/phân trang, preview/dry-run, refresh-status để quan sát độ trễ rule có hiệu lực trong Flink. Không thay đổi pipeline xử lý hay schema bảng. |
| v2.1 | **[ADDED]** Auto-derivation path & metric name cho fallthrough khi không có rule (Section 4.4.1): với các leaf đồng thời trong một notification (cùng timestamp), tách phần chung làm `raw_path`, phần riêng làm `metric_name` thay vì đổ full YANG path. Bổ sung lưu ý determinism. **[CLARIFIED]** Heuristic auto-detect đơn vị timestamp ns/ms (Section 2.1). **[ADDED]** Ràng buộc sink batching & idempotency ở throughput ~50M/phút (Section 7). |
| v2.0 | Bổ sung `device_name` vào Device Enrichment (Section 3.1, 4.2, 5.1). Làm rõ chính sách fallthrough khi không tìm thấy Path Alias hoặc Metric Alias — **không drop record** (Section 4.4, 4.6, 6). |
| v1.0 | Phiên bản khởi tạo. |

---

## Mục lục

1. [Tổng quan pipeline](#1-tổng-quan-pipeline)
2. [Cấu trúc message đầu vào (gNMIc → Kafka)](#2-cấu-trúc-message-đầu-vào-gnmic--kafka)
3. [Dữ liệu tham chiếu (Reference Data)](#3-dữ-liệu-tham-chiếu-reference-data)
4. [Các giai đoạn xử lý](#4-các-giai-đoạn-xử-lý)
5. [Schema đầu ra](#5-schema-đầu-ra)
6. [Xử lý lỗi](#6-xử-lý-lỗi)
7. [Yêu cầu phi chức năng](#7-yêu-cầu-phi-chức-năng)
8. [REST API quản lý](#8-rest-api-quản-lý)
9. [Thuật ngữ](#9-thuật-ngữ)

---

## 1. Tổng quan pipeline

### 1.1 Luồng dữ liệu

```
[300 Routers]
  Cisco / Juniper / Nokia
  gNMI dial-in
        │
        ▼
[gNMIc × 3 instances]
  Nhận telemetry, serialize sang JSON
        │
        ▼
[Kafka: telemetry.raw]
  ~10.000 metrics/router/60s
  → ~50 triệu metrics/phút (tổng hệ thống)
        │
        ▼
[Apache Flink]  ◄── Reference data: MariaDB (device, rules)
        │
        ├──► [ClickHouse: ipms.tlm_metrics]       ← metrics đã xử lý, dùng cho dashboard/alert
        ├──► [ClickHouse: ipms.tlm_metrics_raw]   ← payload thô, dùng để debug (TTL ngắn)
        └──► [Kafka: processed_metrics]            ← fan-out cho các consumer downstream (alert module, analytics)
```

### 1.2 Trách nhiệm của Flink

Flink chịu trách nhiệm:

1. **Parse** message gNMIc JSON từ Kafka thành các metric record chuẩn hoá.
2. **Enrich** mỗi record với thông tin device (`device_id`, `device_name`) tra theo IP của router.
3. **Áp dụng rules** được cấu hình động (path alias, label alias, metric alias, filter).
4. **Ghi đầu ra** vào ClickHouse và Kafka downstream.

Flink ở tài liệu này **không** chịu trách nhiệm: alerting, escalation, notification — các chức năng đó thuộc alert module (mô tả Flink sau) và NOC PRO.

---

## 2. Cấu trúc message đầu vào (gNMIc → Kafka)

### 2.1 Format chung

Mỗi Kafka message là một JSON có thể ở dạng **array** (nhiều notification) hoặc **object đơn** (một notification). Flink phải xử lý được cả hai.

Một notification object có cấu trúc:

```json
{
  "name": "<subscription_name>",
  "timestamp": <unix_nanoseconds>,
  "tags": {
    "source": "<router_ip>:<port>",
    "subscription-name": "<subscription_name>",
    "<tag_key>": "<tag_value>"
  },
  "values": {
    "<yang_path_leaf>": <value>
  }
}
```

**Các trường quan trọng:**

| Trường        | Ghi chú                                                                                                                                                                       |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tags.source` | IP:port của router. Cần tách lấy phần IP để tra device.                                                                                                                       |
| `timestamp`   | Đơn vị nanosecond. Cần convert sang millisecond khi lưu. Chú ý: magnitude của timestamp có thể không đồng nhất tùy nguồn — nên detect tự động thay vì hard-code `/1_000_000`. Heuristic đề xuất: phân loại theo số chữ số / khoảng giá trị của một thời điểm gần hiện tại — ~19 chữ số ⇒ ns (÷1e6 → ms), ~16 chữ số ⇒ µs (÷1e3), ~13 chữ số ⇒ đã là ms (giữ nguyên), ~10 chữ số ⇒ s (×1e3). Sau convert, sanity-check kết quả nằm trong khoảng hợp lý (vd: trong vòng ±N ngày so với `now`) trước khi dùng làm `event_time`. |
| `values`      | Map từ YANG path → value. Value có thể là number, string, hoặc bool tùy metric và vendor.                                                                                     |

### 2.2 Lưu ý về encoding gNMIc

gNMIc hỗ trợ hai encoding khi nhận data từ router (`json` và `proto`). **Kafka output vẫn là JSON text trong cả hai trường hợp**, nhưng cấu trúc bên trong khác:

- `encoding: json` — mỗi notification thường gom nhiều metric trong `values`.
- `encoding: proto` — mỗi notification thường chỉ có 1 metric trong `values`; value là native type (số, bool), không phải string.

Flink phải xử lý tương thích cả hai, vì hệ thống có thể chạy hỗn hợp.

---

## 3. Dữ liệu tham chiếu (Reference Data)

Flink cần load và refresh định kỳ hai loại dữ liệu từ MariaDB:

### 3.1 Device data

**Mục đích:** Tra `device_id` và `device_name` từ IP của router.

**Nguồn:** Bảng `cat_device`.

**Query Flink sử dụng để build cache (chạy lúc startup + refresh định kỳ):**

```sql
SELECT device_id, device_name, device_ip
  FROM cat_device
 WHERE status = 1;
```

**Dữ liệu load vào cache:** `HashMap<String, DeviceInfo>` với key = `device_ip`.  
`DeviceInfo` tối thiểu gồm: `device_id` (Int32), `device_name` (String).

**Cơ chế refresh:** Định kỳ theo interval cấu hình (không hard-code). Khi load mới, thay thế toàn bộ snapshot cũ (atomic replace).

**Cache miss:** Nếu IP không tìm thấy trong cache, dùng `device_id = 0` và `device_name = ""` (empty string) — tiếp tục xử lý, **không drop record**.

> **Ghi chú cho dev:**
> - `cat_device.device_id` là `INT` có dấu; khi ghi vào ClickHouse (`UInt32`) cần cast: `(UInt32) Math.max(0, device_id)`.
> - Nếu router mới đăng ký, Flink pick up trong vòng refresh kế (≤ interval cấu hình). Các event trong khoảng đó ghi `device_id=0`, `device_name=""` — chấp nhận được.
> - Chọn cơ chế phân phối snapshot tới các Flink operator phù hợp với kiến trúc job (Broadcast State, side input, shared cache, v.v.).

### 3.2 Rule data

Flink cần bốn nhóm rule sau, load từ MariaDB và refresh định kỳ:

| Nhóm         | Bảng tham chiếu        | Mục đích                                                                    |
| ------------ | ---------------------- | --------------------------------------------------------------------------- |
| Path Alias   | `tlm_path_aliases`     | Map YANG container path → alias ngắn gọn (`path_id`, `alias_path`)          |
| Metric Alias | `tlm_metric_aliases`   | Map YANG leaf path → tên metric nghiệp vụ (`alias_metric`), transform value |
| Label Alias  | `tlm_label_aliases`    | Đổi tên / chuẩn hoá tag key và value                                        |
| Filter       | `tlm_filter_rules`     | Drop metric record theo điều kiện do operator cấu hình                      |

**Cơ chế refresh:** Tương tự device — định kỳ, atomic replace toàn bộ snapshot.

**Priority:** Khi nhiều rule cùng match một record, rule có `priority` cao hơn (số lớn hơn) được áp dụng. Dev quyết định cách resolve tie-break.

> **Quan trọng:** Rule được quản lý động qua UI/API bởi operator. Interval refresh phải đủ ngắn để thay đổi rule có hiệu lực trong vòng vài phút. Không cần restart Flink job khi rule thay đổi.

---

## 4. Các giai đoạn xử lý

Dưới đây là các **giai đoạn logic** cần thực hiện. Thứ tự thực tế, cách gom hay tách thành Flink operator, và cách implement nội bộ là quyết định của dev.

### 4.1 Parse

**Đầu vào:** Raw JSON string từ Kafka.  
**Đầu ra:** Một hoặc nhiều metric record, mỗi record đại diện cho một cặp (YANG path, value).

Yêu cầu:

- Xử lý được cả dạng array và object đơn.
- Mỗi entry trong `values` map tạo ra một metric record riêng biệt để xử lý downstream.
- Record tối thiểu phải mang: router IP (từ tag `source`), timestamp đã convert, YANG path, raw value, các tag còn lại.
- **Giữ thông tin nhóm theo notification:** các leaf cùng một `values` map (cùng `timestamp` và `tags.source`) là các metric **đồng thời**. Parser cần giữ liên kết nhóm này (vd: gắn một `notification_id`/batch key tạm cho các record sinh ra từ cùng notification) để bước Path Alias có thể tính phần chung khi auto-derive (xem Section 4.4.1).

### 4.2 Device Enrichment

**Mục đích:** Gắn `device_id` và `device_name` vào mỗi record dựa trên router IP.

**Tra cứu:** Dùng IP (tách từ `tags.source`, bỏ phần `:port`) để lookup device snapshot.

**Kết quả ghi vào record:**

| Trường        | Khi tìm thấy trong cache          | Khi cache miss (router chưa đăng ký) |
| ------------- | --------------------------------- | ------------------------------------ |
| `device_id`   | `(UInt32) device.device_id`       | `0` (reserved value)                 |
| `device_name` | `device.device_name` (vd: `"HAN-PE-01"`) | `""` (empty string)           |

Cache miss **không** làm drop record — xử lý tiếp tục bình thường.

### 4.3 Lưu Raw Payload

**Mục đích:** Ghi lại payload thô phục vụ debug và truy vết.  
**Đích:** ClickHouse `ipms.tlm_metrics_raw`.  
**TTL:** 7 ngày (cấu hình ở ClickHouse, không phải ở Flink).

> Raw save nên lưu payload gần như nguyên bản (trước hoặc sau parse tùy thuận tiện), không phụ thuộc vào kết quả các bước xử lý sau.

### 4.4 Áp dụng Path Alias

**Mục đích:** Xác định nhóm metric (path group) từ YANG path.

Logic tra cứu:

1. Dùng YANG path (hoặc phần container chung) để tìm rule match trong Path Alias.
2. Nếu có rule match: gán `path_id` và `alias_path` vào record. **Rename theo rule — KHÔNG auto-derive.**
3. **Nếu không có rule match → `path_id = 0`. Áp dụng auto-derivation (Section 4.4.1) để tách `raw_path` và `metric_name` một cách có cấu trúc thay vì đổ nguyên full YANG path. Record tiếp tục được xử lý — không drop.**

`alias_path` (khi có rule) hoặc `raw_path` auto-derived (khi không rule) sau bước này được dùng làm key để tra Metric Alias và Label Alias ở các bước tiếp theo.

> **Resolve theo `vendor_code` (v2.4):** khi tra rule (áp cho cả 4 nhóm: Path/Metric/Label Alias, Filter), một record được xét với các rule có `vendor_code` thuộc: **(a)** vendor của thiết bị (từ Device Enrichment), **(b)** `"OpenConfig"` — chỉ khi sensor path thuộc data model OpenConfig (bắt đầu bằng `openconfig-`), **(c)** `"All"`. Khi nhiều rule cùng match, thứ tự ưu tiên: **vendor-specific → OpenConfig → All**. Với Metric/Label Alias, thứ tự này thể hiện qua quy ước `priority` (path-scoped=100, any-path=50, OpenConfig=30, All=10 — MariaDB v4.3 Phụ lục B); với Path Alias (không có `priority`), Flink resolve theo thứ tự cố định trên, lấy rule đầu tiên match.

> **Lý do không drop khi miss:** Path alias chưa được cấu hình là trường hợp bình thường trong quá trình onboard subscription mới. Drop silently gây mất dữ liệu mà operator không nhận biết được. Record vẫn có giá trị với `raw_path` và `raw_metric_name` để operator tra cứu và bổ sung rule sau.

#### 4.4.1 Auto-derivation path & metric name (khi không có rule)

**Bối cảnh:** Một notification gNMIc thường chứa nhiều leaf trong cùng `values` map — tức nhiều metric **đồng thời** ở cùng một `timestamp` và cùng `tags.source`. Khi các leaf này **chưa có Path/Metric Alias rule**, nếu chỉ đổ full YANG path vào `metric_name` thì dữ liệu rất khó query và mọi leaf nằm phẳng không có khái niệm "nhóm". Để dữ liệu chưa-cấu-hình vẫn dùng được, Flink tự suy ra cấu trúc path/metric từ **phần chung** và **phần riêng** của các leaf đồng thời.

> Auto-derivation **chỉ** áp dụng cho các leaf **không** match Path Alias rule. Leaf có rule luôn đi theo rule (Section 4.4 bước 2). Hai cơ chế độc lập, có thể cùng tồn tại trong một notification.

**Thuật toán** (áp cho tập các leaf chưa-có-rule trong **cùng một notification**):

1. Thu thập tập leaf path chưa-có-rule của cùng notification (dùng nhóm giữ từ Section 4.1).
2. Tách mỗi path thành các **segment** theo `/`. **Coi predicate khoá `[...]` là một phần của segment, KHÔNG tách bên trong dấu ngoặc** (vd `interface[name=Gi0/0/0]` là một segment, không bị `/` bên trong cắt nhỏ).
3. Tính **longest common prefix (LCP) theo segment** trên toàn tập.
4. `LCP` → `raw_path` (auto). Phần segment còn lại của mỗi leaf (nối lại bằng `/`) → `metric_name` (auto).
5. Vì chưa có Field Rename rule, đây cũng là giá trị `metric_name` cuối (xem Section 4.6). `raw_metric_name` **vẫn lưu full leaf path** để debug.

**Ví dụ** (1 notification, 2 leaf chưa có rule):

```
leaf 1: A/B/C/D/E
leaf 2: A/B/C/G/H

→ LCP (theo segment) = A/B/C

leaf 1 → raw_path = "A/B/C",  metric_name = "D/E",  raw_metric_name = "A/B/C/D/E"
leaf 2 → raw_path = "A/B/C",  metric_name = "G/H",  raw_metric_name = "A/B/C/G/H"
```

**Các trường hợp biên:**

| Tình huống | Hành vi |
| ---------- | ------- |
| Chỉ 1 leaf chưa-có-rule trong notification | Không có sibling để so. Quy ước: `raw_path` = toàn bộ path trừ segment cuối; `metric_name` = segment cuối (suy biến của LCP một phần tử = parent container). |
| Không có common prefix (root khác nhau) | `raw_path = ""`, `metric_name` = full leaf path. Giữ hành vi như v2.0. |
| LCP = toàn bộ path của một leaf (leaf này là prefix của leaf khác) | Rút LCP lại tối đa `N−1` segment để `metric_name` không bao giờ rỗng. |
| Trong notification có cả leaf-có-rule và leaf-không-rule | Chỉ nhóm các leaf **không-rule** để tính LCP; leaf có rule đi theo rule riêng. |

**Lưu ý quan trọng — tính ổn định (determinism):**

`raw_path` auto phụ thuộc vào **tập leaf đồng thời** trong notification. Nếu tập leaf của một subscription thay đổi giữa các sample (vd leaf xuất hiện/biến mất), `raw_path` auto của cùng một metric có thể khác nhau giữa các lần lấy mẫu → gây phân mảnh khi query.

- Trong thực tế, gNMIc gửi tập leaf **ổn định** theo subscription/container ở mỗi sample, nên auto-path thường ổn định.
- Auto-derivation chỉ là **best-effort cho dữ liệu chưa cấu hình rule**. **Khuyến nghị:** với metric quan trọng, operator tạo Path Alias rule để có `path_id` ổn định và xác định (one-time onboarding). Khi đã có rule → hoàn toàn deterministic.
- **Tùy chọn cứng hoá:** nếu message gNMIc còn giữ `prefix`/subscription path, ưu tiên dùng nó làm phần chung thay vì LCP theo co-arrival để ổn định tuyệt đối, độc lập với tập leaf đồng thời.

### 4.5 Áp dụng Label Alias

**Mục đích:** Chuẩn hoá tag key và value.

Logic:

- Tra Label Alias theo `(vendor, path_id)` hoặc `alias_path`.
- Đổi tên tag key theo rule (nếu có).
- Transform tag value theo `lv_kind` (identity, regex, enum_mapping).
- Tags không có rule → giữ nguyên (default allow, không drop tag).

### 4.6 Áp dụng Metric Alias + Transform

**Mục đích:** Đổi tên metric và transform giá trị sang đơn vị chuẩn.

Logic cho mỗi metric trong record:

1. Tra Metric Alias theo `(vendor, alias_path, original_yang_path)`.
2. Nếu match: gán `alias_metric` (tên nghiệp vụ), transform value theo `transform_kind` và `scale_factor`/`offset_value`.
3. Xác định `value_type` (number / string / bool).
4. **Nếu không match: `metric_name` = giá trị auto-derived (phần riêng sau LCP — xem Section 4.4.1) nếu path không có rule; hoặc phần leaf sau `alias_path` nếu path có rule nhưng metric chưa có rule. Không transform value, `unit = ""`. `raw_metric_name` vẫn lưu full YANG leaf path. Record tiếp tục được ghi — không drop.**

> **Ghi chú quan hệ với 4.4.1:** Field Rename (metric alias) chỉ ghi đè `metric_name` khi có rule khớp. Khi không có rule, `metric_name` lấy từ auto-derivation ở bước Path Alias chứ không phải full leaf path. `raw_metric_name` luôn giữ full leaf path để truy vết và để operator soạn rule sau.

> **Lý do không drop khi miss:** Metric alias chưa được định nghĩa nghĩa là operator chưa kịp tạo rule, không phải metric vô giá trị. Dữ liệu vẫn cần được lưu với tên thô để operator có thể query, phân tích, và bổ sung alias rule sau đó. Drop gây mất dữ liệu không thể phục hồi vì `tlm_metrics_raw` có TTL 7 ngày.

### 4.7 Filter

**Mục đích:** Drop metric record theo rule do **operator chủ động cấu hình**.

Logic:

- Tra Filter Rule theo `(vendor, alias_path, alias_metric)` (sau khi đã alias).
- Nếu rule match và điều kiện thoả → drop record, không ghi downstream.
- Nếu không có rule match → giữ record (default allow).

> **Phân biệt với Section 4.4 và 4.6:** Drop ở đây là **hành vi chủ động** do operator tạo rule trong `tlm_filter_rules` — không phải fallback khi thiếu alias. Hai cơ chế hoàn toàn độc lập.

### 4.8 Ghi đầu ra

Sau khi qua tất cả các bước, record được ghi ra:

1. **ClickHouse `ipms.tlm_metrics`** — bảng metrics chính, dùng cho dashboard và alert.
2. **Kafka `processed_metrics`** — fan-out cho alert module và các consumer downstream khác.

> Dev quyết định cơ chế sink (batch size, flush interval, retry policy) phù hợp với latency SLO.

---

## 5. Schema đầu ra

### 5.1 ClickHouse: `ipms.tlm_metrics`

| Cột               | Kiểu                | Mô tả                                                                                           |
| ----------------- | ------------------- | ----------------------------------------------------------------------------------------------- |
| `event_time`      | DateTime64(3)       | Thời điểm router lấy mẫu (từ `timestamp` đã convert)                                            |
| `receive_time`    | DateTime64(3)       | Thời điểm Flink xử lý                                                                           |
| `device_id`       | UInt32              | ID thiết bị. `0` nếu router chưa đăng ký trong `cat_device`.                                   |
| `device_name`     | LowCardinality(String) | Tên thiết bị, denormalized từ `cat_device.device_name` tại ingest time. `""` nếu `device_id=0`. Dùng trực tiếp trong WHERE/GROUP BY, không cần JOIN `cat_device`. |
| `path_id`         | UInt32              | ID của path alias. `0` nếu không có rule match trong `tlm_path_aliases`.                        |
| `raw_path`        | String              | Phần "path" trước PathRename. Có rule: container thô đã match rule. Không rule: LCP auto-derived của các leaf đồng thời (Section 4.4.1). Luôn lưu. |
| `raw_metric_name` | String              | YANG leaf path **đầy đủ** trước FieldRename. Luôn lưu (kể cả khi auto-derive) để debug & soạn rule.       |
| `metric_name`     | LowCardinality(String) | Tên metric dùng để query. Có Metric Alias rule → `alias_metric`. Không rule → phần riêng auto-derived sau LCP (Section 4.4.1), KHÔNG phải full leaf path. |
| `value_type`      | UInt8               | `1`=number, `2`=string, `3`=bool.                                                               |
| `value_number`    | Float64             | Giá trị số (sau transform). Đọc khi `value_type=1`.                                             |
| `value_string`    | String              | Giá trị string. Đọc khi `value_type=2`.                                                         |
| `value_bool`      | Bool                | Giá trị bool. Đọc khi `value_type=3`.                                                           |
| `unit`            | LowCardinality(String) | Đơn vị sau transform (từ Metric Alias rule). `""` nếu không có rule hoặc `value_type∈{2,3}`. |
| `labels`          | Map(LowCardinality(String), String) | Tags sau Label Alias. Keys đã là tên chuẩn.                               |

### 5.2 ClickHouse: `ipms.tlm_metrics_raw`

Lưu payload thô phục vụ debug. Schema do dev quyết định, tối thiểu cần có:

- Timestamp nhận từ Kafka.
- Raw payload (JSON string).
- TTL 7 ngày.

### 5.3 Kafka: `processed_metrics`

Format message do dev quyết định, cần đủ thông tin để alert module consume:

- `device_id`, `device_name`, `metric_name` (`alias_metric` hoặc `raw_metric_name`), `value_number`, `event_time`, `labels`.
- Nên dùng JSON hoặc Avro với schema rõ ràng.

---

## 6. Xử lý lỗi

| Tình huống               | Hành vi                                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------------------- |
| JSON parse fail          | Log warning, không crash job. Cân nhắc ghi vào Dead Letter Queue (Kafka topic riêng) để phân tích sau. |
| Device cache miss        | `device_id = 0`, `device_name = ""`. Tiếp tục xử lý, **không drop**.                                   |
| Path Alias không match   | `path_id = 0`. Auto-derive `raw_path` (LCP) + `metric_name` (phần riêng) theo Section 4.4.1. Tiếp tục xử lý, **không drop**.            |
| Metric Alias không match | `metric_name` = giá trị auto-derived (không phải full leaf path), không transform, `unit = ""`. `raw_metric_name` vẫn lưu full leaf. Tiếp tục xử lý, **không drop**.         |
| Filter rule match        | Drop record theo **ý định của operator** (khác với miss alias ở trên — đây là drop chủ động).           |
| ClickHouse write fail    | Retry với backoff. Không mất data — cân nhắc buffer hoặc checkpoint.                                   |
| Rule load fail (MariaDB) | Giữ snapshot cũ, log error, retry ở cycle sau.                                                          |

> **Nguyên tắc chung:** Chỉ drop record khi có Filter Rule chủ động của operator. Mọi tình huống miss alias hay cache miss đều là **fallthrough** — record vẫn được ghi với giá trị mặc định để bảo toàn dữ liệu.

> Ngoài các ràng buộc trên, dev tự quyết định chi tiết error handling (DLQ format, retry count, alerting on error rate, v.v.).

---

## 7. Yêu cầu phi chức năng

| Yêu cầu                                        | Giá trị                                                                |
| ---------------------------------------------- | ---------------------------------------------------------------------- |
| Throughput                                     | ~50 triệu metrics/phút (300 router × 10.000 metrics × 1/60s) ≈ **833K rows/giây** |
| End-to-end latency (Kafka in → ClickHouse out) | Cần đủ nhanh để alert module đáp ứng SLO 5–30 giây                     |
| ClickHouse sink batching                       | Ở ~833K rows/s, **bắt buộc** batch insert lớn (đề xuất 100K–500K rows hoặc flush mỗi 1–5s, tuỳ điều kiện đến trước) + nhiều parallel sink subtask. KHÔNG insert từng row. Cân nhắc async insert ở phía ClickHouse. |
| Idempotency / delivery                         | Pipeline ở mức **at-least-once**; khi retry sau lỗi sink có thể sinh row trùng. `tlm_metrics` là `MergeTree` thuần (không tự dedup). Chấp nhận trùng nhỏ, **hoặc** chuyển sang khử trùng bằng business key + thời gian (vd `ReplacingMergeTree`/dedup ở query) nếu yêu cầu chính xác tuyệt đối. Dev xác nhận trade-off. |
| Rule refresh latency                           | Thay đổi rule có hiệu lực trong vòng vài phút, không cần restart job   |
| Raw data TTL                                   | 7 ngày                                                                 |
| Audit log (nếu có)                             | 90 ngày                                                                |
| Availability                                   | Flink job phải tự recover khi gặp lỗi transient (checkpoint/savepoint) |

---

## 8. REST API quản lý

API là kênh duy nhất để operator quản lý **reference data** mà Flink tiêu thụ. Flink **không** gọi API trực tiếp — nó load + refresh snapshot từ MariaDB định kỳ (Section 3). Vì vậy hợp đồng quan trọng nhất của API là: **mọi write phải tất định, validate đúng ràng buộc bảng, và đẩy `updated_at` tiến lên để chu kỳ refresh kế của Flink pick up thay đổi.**

Phạm vi API gồm 4 nhóm rule trong Section 3.2 cộng tra cứu device (read-only ở module này). Không bao gồm alerting/derivation (thuộc module khác).

### 8.0 Quy ước chung

- **Base path:** `/api/v1/flink`
- **Định dạng:** request/response JSON, `Content-Type: application/json; charset=utf-8`.
- **Auth & RBAC:** Bearer token; tối thiểu tách quyền read vs write. Mọi write ghi `created_by` từ token.
- **Transaction + versioning (bắt buộc):** mỗi write (POST/PUT/PATCH/DELETE) chạy trong **một transaction**:
  1. INSERT/UPDATE row trong bảng tương ứng — cột `updated_at` tự cập nhật (`ON UPDATE CURRENT_TIMESTAMP(6)`) thành mốc version mới, tăng đơn điệu;
  2. đặt `pushed_at = NULL` (đánh dấu "chưa mirror sang ClickHouse" — sync job sẽ pick ở vòng kế).

  > **v2.3:** Không còn bảng `tlm_revision_counter` hay cột `revision`. `updated_at` (độ phân giải micro-giây) đóng vai trò version. Trường hợp hiếm hai write cùng micro-giây trên một row, dùng `updated_at = GREATEST(updated_at + INTERVAL 1 MICROSECOND, CURRENT_TIMESTAMP(6))` để bảo đảm đơn điệu tuyệt đối.
- **Soft-delete:** DELETE = set `status = 0` (Deprecated), **không** physical DELETE — vì ClickHouse vẫn tham chiếu `path_id`/alias cũ. `updated_at` vẫn tự tiến + reset `pushed_at`.
- **Hiệu lực trong Flink:** sau khi write thành công, thay đổi có hiệu lực sau **một chu kỳ refresh** của Flink (Section 3.2, "vài phút", cấu hình). API không đẩy trực tiếp — phản hồi nên nêu rõ độ trễ này (xem §8.8).
- **Mã trạng thái:** `200` OK, `201` Created, `204` No Content (DELETE), `400` sai cú pháp, `401/403` auth, `404` không thấy, `409` xung đột unique / `updated_at` mismatch, `422` vi phạm ràng buộc ngữ nghĩa (vd `transform_kind=2` thiếu `transform_expression`), `500` lỗi máy chủ.
- **Concurrency:** PUT/PATCH hỗ trợ `If-Unmodified-Since: <updated_at>` (hoặc `If-Match: <updated_at>` dạng opaque token); nếu `updated_at` hiện tại khác → `409` (chống ghi đè đồng thời).

### 8.1 Bảng endpoint

Mỗi nhóm rule có bộ CRUD đồng nhất. Ký hiệu `{group}` ∈ `path-aliases | metric-aliases | label-aliases | filter-rules`.

| Method & Path | Mục đích |
| --- | --- |
| `GET /flink/{group}` | Liệt kê rule (lọc + phân trang) |
| `GET /flink/{group}/{id}` | Chi tiết một rule |
| `POST /flink/{group}` | Tạo rule |
| `PUT /flink/{group}/{id}` | Cập nhật toàn bộ (full replace) |
| `PATCH /flink/{group}/{id}` | Cập nhật một phần (vd chỉ `priority`, `status`) |
| `DELETE /flink/{group}/{id}` | Soft-delete (`status=0`) |
| `POST /flink/{group}/preview` | Dry-run: áp thử rule lên mẫu metric thực gần nhất (không lưu) |
| `GET /flink/devices` | Liệt kê device (read-only; tra `device_id`/`device_name` theo IP) |
| `GET /flink/refresh-status` | Quan sát `updated_at` mới nhất per-bảng vs `pushed_at`/độ trễ Flink (§8.8) |

> Device (`cat_device`) ở module này chỉ **đọc** để hỗ trợ soạn rule và kiểm tra enrichment; vòng đời device được quản lý ở module inventory riêng.

### 8.2 List, lọc, phân trang (`GET /flink/{group}`)

Query params chung: `status` (mặc định `1`), `vendor_code`, `q` (tìm chuỗi trên field tên/path tuỳ nhóm), `page` (mặc định 1), `page_size` (mặc định 50, tối đa 200), `sort` (vd `-updated_at`, `-priority`).

Params riêng theo nhóm: `path_alias_id` (metric/label alias), `match_path` & `match_metric` (filter).

Response (ví dụ `path-aliases`):

```json
{
  "page": 1,
  "page_size": 50,
  "total": 212,
  "items": [
    {
      "id": 5,
      "vendor_code": "Cisco",
      "original_path": "Cisco-IOS-XR-wd-oper:watchdog/nodes/node/memory-state",
      "alias_path": "xr_watchdog_memory",
      "status": 1,
      "pushed_at": "2026-06-03T08:00:00+07:00",
      "updated_at": "2026-06-03T07:59:40+07:00"
    }
  ]
}
```

### 8.3 Path Alias — `POST /flink/path-aliases`

```json
{
  "vendor_code": "Cisco",
  "original_path": "Cisco-IOS-XR-wd-oper:watchdog/nodes/node/memory-state",
  "alias_path": "xr_watchdog_memory"
}
```

Validation:
- `vendor_code ∈ {Cisco, Juniper, Nokia, OpenConfig, All}` (app-layer — v2.4). Khuyến nghị: khi `vendor_code = "OpenConfig"`, API cảnh báo nếu `original_path` **không** bắt đầu bằng `openconfig-` (rule sẽ không bao giờ match).
- Unique `(vendor_code, original_path)` và `(vendor_code, alias_path, status)` — trùng → `409`.
- `alias_path` **không được sửa** sau khi Active (ràng buộc bảng): muốn đổi → Deprecate row cũ + tạo row mới + cascade reset `pushed_at` các metric/label alias con (xem §8.7).

### 8.4 Metric Alias — `POST /flink/metric-aliases`

```json
{
  "vendor_code": "Cisco",
  "path_alias_id": 5,
  "original_name": "free-application-memory",
  "alias_metric": "node_free_memory",
  "value_type_override": 1,
  "source_unit": "By",
  "target_unit": "MBy",
  "transform_kind": 1,
  "scale_factor": 0.00000095367432,
  "offset_value": 0,
  "priority": 100
}
```

Validation (theo CHECK của bảng):
- `path_alias_id`: `null` = any-path (`priority` quy ước 50); NOT NULL phải trỏ tới `tlm_path_aliases.id` Active (logic-layer FK) → `path-scoped` (`priority` quy ước 100).
- Unique theo `path_scope_key = IFNULL(path_alias_id,0)`: `(vendor_code, path_scope_key, original_name)` và `(vendor_code, path_scope_key, alias_metric)`.
- `transform_kind`: `0=identity, 1=linear, 2=expression, 3=enum_mapping`. `=2` ⇒ `transform_expression` bắt buộc (và chỉ khi đó); `=3` ⇒ `enum_mapping` bắt buộc (và chỉ khi đó).
- `value_type_override ∈ {null,1,2,3}`. Nếu là `2|3` (string/bool) ⇒ `source_unit`/`target_unit` phải null, `scale_factor=1`, `offset_value=0`, `transform_kind ∈ {0,3}` — vi phạm trả `422`.

### 8.5 Label Alias — `POST /flink/label-aliases`

```json
{
  "vendor_code": "Cisco",
  "path_alias_id": 5,
  "original_key": "interface-name",
  "alias_key": "if_name",
  "lv_kind": 0,
  "priority": 100
}
```

Validation:
- Unique theo `path_scope_key`: `(vendor_code, path_scope_key, original_key)` và `(vendor_code, path_scope_key, alias_key)`.
- `lv_kind`: `0=identity, 1=regex, 2=enum_mapping`. `=1` ⇒ `lv_pattern` + `lv_replace` bắt buộc (và chỉ khi đó); `=2` ⇒ `lv_mapping` bắt buộc (và chỉ khi đó). Ví dụ regex:

```json
{
  "vendor_code": "All",
  "path_alias_id": null,
  "original_key": "intf",
  "alias_key": "if_name",
  "lv_kind": 1,
  "lv_pattern": "^Gi(\\d.*)$",
  "lv_replace": "GigabitEthernet$1",
  "priority": 50
}
```

### 8.6 Filter Rule — `POST /flink/filter-rules`

```json
{
  "vendor_code": "All",
  "match_path": "xr_watchdog_memory",
  "match_metric": null,
  "filter_expression": "value < 1000",
  "filter_action": 0,
  "priority": 100
}
```

Validation:
- `filter_action`: `0=EXCLUDE_IF_MATCH` (drop khi predicate TRUE — noise filter), `1=INCLUDE_IF_MATCH` (chỉ giữ khi predicate TRUE — selective sample).
- `match_path` rỗng = mọi path; `match_metric` null = mọi metric trong path đó.
- `filter_expression` rỗng = unconditional (luôn TRUE). Cú pháp predicate do Flink định nghĩa — API **không** đánh giá biểu thức, chỉ kiểm độ dài/cú pháp cơ bản; khuyến nghị validate sâu qua `preview` (§8.9).

> **Lưu ý vận hành:** filter là **drop chủ động** của operator (Section 4.7). Một rule sai (vd `EXCLUDE` quá rộng) sẽ âm thầm loại bỏ metric. Luôn `preview` trước khi tạo và xem lại `priority` để tránh rule rộng đè rule hẹp.

### 8.7 Cập nhật & xoá (cascade)

- `PUT`/`PATCH`: `updated_at` tự tiến (version mới), reset `pushed_at=NULL`. `PATCH` chỉ field gửi lên (vd bật/tắt nhanh `{ "status": 0 }`, hoặc đổi `{ "priority": 120 }`).
- `DELETE` (soft): `status=0`. Trả `204`.
- **Cascade khi đổi/deprecate Path Alias:** vì metric/label alias tham chiếu `path_alias_id`, khi một Path Alias bị Deprecate, API phải **reset `pushed_at=NULL` cho các alias con** (để sync job đẩy lại trạng thái) và **cảnh báo** danh sách rule con bị ảnh hưởng trong response. Chặn (`409`) hoặc yêu cầu `?force=true` nếu còn metric/label alias Active trỏ tới path đó, tránh để con mồ côi.

### 8.8 `GET /flink/refresh-status` — quan sát độ trễ hiệu lực

Vì Flink chỉ pick up rule ở chu kỳ refresh, operator cần biết thay đổi "đã sống" chưa. Endpoint trả `updated_at` mới nhất per-bảng và dấu thời gian mirror:

```json
{
  "tables": [
    {
      "table_name": "tlm_metric_aliases",
      "max_updated_at": "2026-06-03T08:00:01.482931+07:00",
      "rows_pending_push": 3,
      "oldest_pending_pushed_at": null,
      "last_push_completed_at": "2026-06-03T08:00:05+07:00"
    }
  ],
  "note": "Flink refresh interval ~ vài phút (cấu hình). pushed_at=NULL nghĩa là chưa mirror sang ClickHouse."
}
```

`rows_pending_push` = số row `pushed_at IS NULL` (chưa được sync job mirror). Giá trị cao kéo dài ⇒ sync job có vấn đề.

### 8.9 Preview / dry-run — `POST /flink/{group}/preview`

Nhận **định nghĩa rule chưa lưu** (hoặc `{ "id": <id> }`) cùng tham số `sample_window` (mặc định: vài phút gần nhất) và bộ lọc tuỳ chọn (`vendor_code`, `device_id`). Engine lấy mẫu metric thực gần nhất từ ClickHouse và áp **chỉ** rule đang preview, **không** ghi gì, **không** ảnh hưởng pipeline.

Response (ví dụ `metric-aliases`):

```json
{
  "sampled_records": 540,
  "matched_records": 312,
  "samples": [
    {
      "raw_metric_name": "...:watchdog/.../free-application-memory",
      "before": { "metric_name": "free-application-memory", "value_number": 1572864, "unit": "" },
      "after":  { "metric_name": "node_free_memory", "value_number": 1.5, "unit": "MBy" }
    }
  ]
}
```

Với `filter-rules`, preview trả số record **sẽ bị drop/giữ** theo `filter_action` — giúp tránh rule loại nhầm diện rộng. Preview phản ánh tác động **một-rule, một-thời-điểm**; không mô phỏng tương tác `priority` giữa nhiều rule (nêu rõ để operator không kỳ vọng sai).

### 8.10 Định dạng lỗi

```json
{
  "error": "validation_failed",
  "message": "transform_kind=2 yêu cầu transform_expression.",
  "details": [
    { "field": "transform_expression", "reason": "bắt buộc khi transform_kind=2" }
  ]
}
```

---

## 9. Thuật ngữ

| Thuật ngữ           | Định nghĩa                                                                      |
| ------------------- | ------------------------------------------------------------------------------- |
| `alias_path`        | Tên ngắn gọn đặt cho một YANG container path (ví dụ: `xr_watchdog_memory`)      |
| `alias_metric`      | Tên nghiệp vụ đặt cho một YANG leaf metric (ví dụ: `node_free_memory`)          |
| `path_id`           | ID số của alias_path trong hệ thống. `0` = không có rule match.                |
| `device_name`       | Tên thiết bị denormalized từ `cat_device` tại ingest time. `""` khi cache miss. |
| `vendor_code`       | Mã phạm vi vendor của rule: `"Cisco"`/`"Juniper"`/`"Nokia"` (vendor thiết bị), `"OpenConfig"` (data model vendor-neutral — match theo path `openconfig-*`, bất kể vendor thiết bị), `"All"` (mọi vendor). |
| `transform_kind`    | Loại transform áp dụng lên value: linear (scale/offset), enum mapping, v.v.     |
| `lv_kind`           | Loại transform áp dụng lên label value: identity, regex, enum_mapping.          |
| `raw_path`          | YANG path ở dạng thô trước khi alias                                            |
| `raw_metric_name`   | YANG leaf metric name ở dạng thô trước khi alias                                |
| `processed_metrics` | Kafka topic downstream nhận metric đã xử lý, dùng cho alert module và analytics |
| fallthrough         | Hành vi tiếp tục xử lý với giá trị mặc định khi không tìm thấy rule match, thay vì drop record |
| auto-derivation     | Cơ chế tự suy ra `raw_path` + `metric_name` khi không có Path/Metric Alias rule, dựa trên phần chung/riêng của các leaf đồng thời trong cùng notification (Section 4.4.1) |
| LCP (longest common prefix) | Tiền tố segment dài nhất chung cho tập leaf path đồng thời; dùng làm `raw_path` auto-derived |
| notification (gNMIc)| Một object trong message Kafka, chứa một `timestamp`, một `tags.source`, và một `values` map gồm các leaf đồng thời |
