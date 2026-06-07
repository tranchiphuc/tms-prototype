# Hướng Dẫn Sử Dụng Cơ Sở Dữ Liệu — NetStream (TMS) Schema v4.0.1

**Tài liệu kèm theo:** `netstream_schema_v4_0.dbml`
**Phiên bản:** 1.0 | Tháng 6 năm 2026
**Đối tượng đọc:** Backend developer, DBA, kiến trúc sư hệ thống

---

## 1. Tổng quan

File `netstream_schema_v4_0.dbml` mô tả toàn bộ cơ sở dữ liệu của hệ thống Telemetry Management Software (TMS), gồm **59 bảng** trải trên **2 hệ quản trị CSDL**:

- **MariaDB (37 bảng):** dữ liệu vận hành/giao dịch — cấu hình, quy tắc, danh mục, người dùng.
- **ClickHouse (22 bảng):** dữ liệu telemetry và phân tích — metrics, utilization, lịch sử cảnh báo, các bảng mirror.

Mọi bảng đều mang tiền tố `tlm_` để nhận diện thuộc dự án này và tránh xung đột với các bảng catalog dùng chung (ví dụ `cat_device`, `cat_interface`).

Một nguyên tắc xuyên suốt: **không có foreign key cấp database giữa hai hệ CSDL**, và phần lớn quan hệ cross-schema được thực thi ở **tầng ứng dụng** (logic-FK). File DBML khai báo `Ref` chỉ cho các quan hệ nội bộ MariaDB để hỗ trợ vẽ sơ đồ; các quan hệ cross-DB được ghi chú ở cuối file.

---

## 2. Cách mở và xem sơ đồ

File ở định dạng DBML (Database Markup Language) của dbdiagram.io. Để xem sơ đồ trực quan:

1. Mở trình duyệt, vào `https://dbdiagram.io`.
2. Tạo diagram mới, chọn **Import → DBML**.
3. Dán toàn bộ nội dung file `netstream_schema_v4_0.dbml`.
4. Sơ đồ ERD sẽ được render, các bảng nhóm theo `TableGroup` và quan hệ vẽ bằng đường nối.

Lưu ý: DBML là ngôn ngữ mô tả, không phải DDL chạy trực tiếp. Kiểu dữ liệu trong file (ví dụ `DateTime64_3`, `LowCardinality_String`, `AggState_Float32`) là **bí danh dễ đọc** cho các kiểu ClickHouse thật. Xem mục 8 để biết cách ánh xạ sang DDL thật.

---

## 3. Bản đồ kiến trúc dữ liệu

Luồng dữ liệu đi qua các bảng theo thứ tự sau:

Router phát telemetry qua gNMI Dial-In, được cụm gNMIc (3 instance, cluster mode) thu thập và đẩy vào Kafka. Flink Job #1 đọc Kafka, áp dụng 4 nhóm quy tắc xử lý (đọc từ các bảng mirror trong ClickHouse), rồi ghi metric đã chuẩn hóa vào `tlm_metrics`. Resource Utilization Service đọc `tlm_metrics` mỗi `T_poll` giây, tính utilization và ghi vào 5 bảng `tlm_*_utilization` — từ đó các Materialized View 5-phút và theo giờ tự động tổng hợp. Alert Engine đọc các MV 5-phút mỗi `T_cycle` giây, đánh giá quy tắc, ghi lịch sử vào `tlm_alert_history` và phát cảnh báo qua Kafka đến NOC PRO.

Cấu hình toàn hệ thống (profile, alias, binding, rule, settings) nằm trong MariaDB. Một sync job định kỳ mirror các bảng quy tắc Flink từ MariaDB sang ClickHouse để phục vụ JOIN trên dashboard.

---

## 4. Hướng dẫn theo từng nhóm chức năng (domain)

### 4.1 Hạ tầng gNMIc Cluster

Ba bảng: `tlm_gnmic_cluster_config`, `tlm_gnmic_instances`, `tlm_consul_config`.

Điểm cần nhớ: trong cluster mode, **operator không gán router thủ công vào instance**. Cụm gNMIc tự bầu leader (qua Consul) và tự phân phối target. Bảng `tlm_gnmic_instances` vì thế chủ yếu là bảng **theo dõi trạng thái** — các cột `is_leader`, `locked_targets`, `last_seen_at` được TMS cập nhật bằng cách poll REST API của cụm (`/api/v1/cluster`), không phải do người dùng nhập.

`tlm_gnmic_cluster_config` và `tlm_consul_config` đều là bảng singleton (chỉ một dòng, `id=0`). Mẫu thao tác là upsert vào dòng `id=0`. `tlm_consul_config.enabled` phải bằng 1 để cluster hoạt động — không có Consul, cụm không thể bầu leader.

### 4.2 Router

Bảng `tlm_routers` đã được đơn giản hóa: bỏ cột `oper_mode` (chỉ còn gNMI Dial-In) và `agent_instance_id` (cluster tự phân phối). `cat_device_id` là tham chiếu chéo sang catalog thiết bị bên ngoài — đây là nguồn sự thật cho hostname, IP, vendor; các cột `short_name`, `ip_address`, `vendor` chỉ là bản cache, làm mới khi thêm router hoặc refresh thủ công. Cột `gnmi_port` cho phép override cổng gNMI riêng cho router dùng cổng phi tiêu chuẩn; để NULL thì dùng cổng mặc định ở `tlm_gnmic_cluster_config.gnmi_port`.

### 4.3 Profile và Liên kết Profile

Bốn bảng định nghĩa "thu thập dữ liệu gì": `tlm_profiles` (nhóm subscription), `tlm_yang_paths` (thư viện đường dẫn cảm biến), `tlm_profile_sensors` (liên kết profile với path kèm interval và subscription mode), `tlm_profile_associations` (gán profile cho router).

Lưu ý quan trọng về `tlm_profile_associations`: cột `cluster_status` (giá trị Pending/Applied) thay cho `push_status` của thiết kế cũ. Pending nghĩa là cấu hình cluster đã cập nhật nhưng cluster leader chưa xác nhận đã phân phối subscription — điều này điều khiển huy hiệu "Đang Chờ Cluster Áp Dụng" trên UI. `interval_sec` tối thiểu là 10 giây (ràng buộc gNMI SAMPLE mode), khác với mức 2 giây của Telegraf trước đây.

### 4.4 Pipeline quy tắc Flink

Đây là phần thay đổi lớn nhất so với thiết kế cũ. Thay vì một bảng `flink_rules` denormalized, thiết kế mới dùng **4 bảng nguồn độc lập**, mỗi bảng tương ứng một loại quy tắc và một Kafka topic:

`tlm_path_aliases` đổi tên đường dẫn cảm biến thô sang đường dẫn chuẩn. `tlm_metric_aliases` đổi tên và transform giá trị metric (hỗ trợ linear, expression, enum mapping). `tlm_label_aliases` chuẩn hóa tên và giá trị tag (giải quyết vấn đề mỗi vendor đặt tên tag khác nhau). `tlm_filter_rules` lọc bỏ hoặc giữ lại metric trước các bước đổi tên.

Cơ chế đồng bộ dựa trên hai cột: `revision` (lấy từ `tlm_revision_counter`, tăng đơn điệu trong cùng transaction CRUD — Flink dùng để bỏ qua message cũ) và `pushed_at` (NULL nghĩa là sync job chưa đẩy sang ClickHouse mirror). Quy ước `priority`: path-scoped=100, any-path=50, vendor=All=10 — khi nhiều quy tắc khớp cùng một record, quy tắc priority cao thắng.

Một điểm tinh tế: `id=0` của `tlm_path_aliases` được dành riêng cho ý nghĩa "không có quy tắc nào khớp". Vì vậy soft-delete (đặt `status=0`) chứ không xóa vật lý — ClickHouse vẫn tham chiếu `path_id` của các quy tắc đã có dữ liệu.

### 4.5 Resource Utilization Binding

Thiết kế **hai tầng**. Tầng 1 là `tlm_util_binding` chứa metadata chung cho mọi loại resource (metric_alias, direction, util_group, vendor). Tầng 2 là năm bảng config riêng cho từng loại: `tlm_util_if_config`, `tlm_util_cpu_config`, `tlm_util_mem_config`, `tlm_util_queue_config`, `tlm_util_packet_error_config`.

Khái niệm `util_group` rất quan trọng: nó gom các metric liên quan để Service biết cần chờ đủ metric trước khi tính. Ví dụ group `interface_traffic` gom `if_in_octets` (direction=ingress) và `if_out_octets` (direction=egress) — Service chờ cả hai rồi mới tính ra một record utilization.

Mỗi loại resource có đặc thù riêng: CPU luôn có capacity=100% (không cần cấu hình thêm); Memory hỗ trợ 3 chế độ capacity (percent_gauge / metric_total / hardcode_bytes); Packet Error dùng tall format (mỗi metric là một dòng riêng) và chỉ cần một binding (không bắt buộc cặp ingress/egress).

Bảng `tlm_util_resource_config` lưu cấu hình N consecutive buckets cho mỗi loại resource — đây là cầu nối với Alert Engine, quyết định số bucket 5-phút liên tiếp phải vi phạm thì cảnh báo mới fire.

### 4.6 Alert Engine

Năm bảng MariaDB cộng một bảng ClickHouse. `tlm_alert_rule` là header quy tắc; `tlm_alert_rule_condition` chứa các điều kiện dạng phẳng (mỗi dòng một điều kiện, không có cấu trúc cây). Một quy tắc basic có một điều kiện; quy tắc composite có 2–10 điều kiện kết hợp bằng AND hoặc OR.

Ba loại điều kiện được phân biệt bằng `compare_type`: Type A (threshold cố định), Type B (so sánh % thay đổi với cùng thời điểm lịch sử), Type C (so với AVG/MAX của khoảng thời gian dài). Type B dùng `history_offset_sec` và Type C dùng `agg_func`/`agg_window_sec` — cả hai chỉ nhận giá trị từ whitelist để Alert Engine tối ưu việc fetch snapshot.

`tlm_alert_rule_audit` ghi lịch sử CRUD lên quy tắc (CREATE/UPDATE/DISABLE/ENABLE/DELETE) với snapshot before/after dạng JSON. `tlm_active_alerts` theo dõi cảnh báo đang sống để hỗ trợ acknowledge — lưu ý đây không phải nguồn sự thật về lịch sử (lịch sử nằm ở ClickHouse `tlm_alert_history`), mà chỉ là bảng tiện ích cho thao tác ACK của NOC.

`tlm_alert_history` (ClickHouse) ghi mọi kết quả đánh giá khớp, kể cả bị suppress — cột `is_first_in_window` phân biệt lần fire đầu (đã phát ra Kafka) với lần bị suppress (chỉ ghi audit). Cột `condition_snapshot` lưu JSON quy tắc tại thời điểm fire, cho phép tái dựng quy tắc kể cả khi sau đó operator chỉnh sửa.

### 4.7 Người dùng và Audit

`tlm_users` và `tlm_audit_log`. Bảng audit là append-only — không bao giờ chạy UPDATE hay DELETE. Người dùng bị vô hiệu hóa (`is_active=0`) không đăng nhập được nhưng giữ lại bản ghi để bảo toàn tính toàn vẹn của lịch sử audit.

### 4.8 Settings

Sáu bảng. Bốn bảng singleton (`tlm_credentials`, `tlm_kafka_config`, `tlm_clickhouse_config`, `tlm_grafana_config`) dùng mẫu một-dòng `id=0`. `tlm_grafana_dashboard_map` ánh xạ từng loại resource sang dashboard Grafana cho tính năng deep-link. `tlm_service_config` lưu các tham số runtime: `T_poll`, `T_cycle`, dedup window mặc định, và các giá trị TTL.

Đáng chú ý: `tlm_credentials` đã bỏ toàn bộ trường Dial-Out (vì không còn chế độ Dial-Out); các thiết lập TLS chuyển sang `tlm_gnmic_cluster_config`. `tlm_kafka_config.encoding_format` nay hỗ trợ cả `proto` lẫn `json` (gNMIc hỗ trợ proto native).

### 4.9 Thư viện YANG

Ba bảng: `tlm_yang_version_bundles`, `tlm_yang_models`, `tlm_yang_nodes`. Bảng nodes tự tham chiếu (`parent_id`) để dựng cây YANG trong UI. Cột `gnmi_xpath` là giá trị mà nút "Copy Path" sao chép vào clipboard. Xóa bundle sẽ cascade xuống tất cả models và nodes của nó.

---

## 5. Các mẫu truy vấn thường gặp

### 5.1 Truy vấn metric theo router (không cần JOIN)

Nhờ cột `device_name` đã được denormalize vào `tlm_metrics`, truy vấn phổ biến không cần JOIN sang catalog:

```sql
SELECT event_time, metric_name, value_number, labels
  FROM ipms.tlm_metrics
 WHERE device_name = 'HAN-PE-01'
   AND metric_name = 'if_in_octets'
   AND event_time >= now() - INTERVAL 1 HOUR
 ORDER BY event_time DESC
 LIMIT 1000;
```

### 5.2 Đọc Materialized View (phải dùng Merge function)

Các bảng MV lưu trạng thái tổng hợp (AggregateFunction), nên phải đọc bằng hàm `*Merge()` kèm GROUP BY:

```sql
SELECT bucket_5min,
       avgMerge(util_in_pct_avg)  AS avg_in,
       maxMerge(util_in_pct_max)  AS peak_in
  FROM ipms.tlm_if_util_5min
 WHERE device_id = 10
   AND if_name = 'GE0/0/0'
   AND bucket_5min >= now() - INTERVAL 1 HOUR
 GROUP BY bucket_5min
 ORDER BY bucket_5min;
```

### 5.3 Đọc bảng mirror (ReplacingMergeTree)

Bảng mirror có thể chứa nhiều bản của cùng một `id` cho tới khi merge nền chạy. Dùng `argMax(..., revision)` để lấy bản mới nhất:

```sql
SELECT id,
       argMax(alias_path, revision)  AS alias_path,
       argMax(status,     revision)  AS status
  FROM ipms.tlm_path_aliases_mirror
 GROUP BY id
HAVING status = 1;
```

### 5.4 Top 10 interface tải cao nhất hiện tại

```sql
SELECT device_name, if_name,
       maxMerge(util_max_pct_max) AS peak
  FROM ipms.tlm_if_util_5min
 WHERE bucket_5min = ( SELECT max(bucket_5min) FROM ipms.tlm_if_util_5min )
 GROUP BY device_name, if_name
 ORDER BY peak DESC
 LIMIT 10;
```

### 5.5 Lịch sử cảnh báo của một thiết bị

```sql
SELECT fired_at, rule_name, severity, triggered_value, threshold_value, state
  FROM ipms.tlm_alert_history
 WHERE device_name = 'HAN-PE-01'
   AND fired_at >= now() - INTERVAL 24 HOUR
   AND is_first_in_window = 1
 ORDER BY fired_at DESC;
```

---

## 6. Quy ước thiết kế cần tuân thủ khi phát triển

Khi viết code tương tác với các bảng này, có một số quy ước bắt buộc.

Về bảng quy tắc Flink: mỗi lần CRUD phải tăng `tlm_revision_counter` trong **cùng transaction** với INSERT/UPDATE. App chỉ ghi MariaDB và không bao giờ gọi Kafka trực tiếp — publisher relay tự đồng bộ. Soft-delete bằng `status=0`, không xóa vật lý các quy tắc đã có dữ liệu ClickHouse tham chiếu.

Về bảng singleton: luôn dùng mẫu upsert `id=0`, không bao giờ INSERT dòng mới.

Về audit: với cả `tlm_audit_log` lẫn `tlm_alert_rule_audit`, ghi audit row trong cùng transaction với thao tác nghiệp vụ để đảm bảo atomicity. Không bao giờ UPDATE/DELETE `tlm_audit_log`.

Về cross-DB integrity: không có FK cấp database giữa MariaDB và ClickHouse. Mọi ràng buộc như `tlm_metrics.path_id → tlm_path_aliases.id` phải được app kiểm soát. Khi xóa một router hay quy tắc, cân nhắc dữ liệu ClickHouse đã tham chiếu nó.

Về kiểu device_id: `cat_device.device_id` là Int32 (có dấu) trong khi `tlm_metrics.device_id` là UInt32. Khi JOIN phải cast `toInt32(m.device_id) = d.device_id`. Khi Flink ghi, guard giá trị âm bằng `Math.max(0, device_id)`.

---

## 7. Mở rộng thêm loại resource mới

Thiết kế utilization cho phép thêm loại resource mới (ví dụ optical power, nhiệt độ) mà không phá vỡ schema hiện có. Quy trình: tạo bảng `tlm_util_<type>_config` mới ở MariaDB; seed một dòng vào `tlm_util_resource_config` với resource_type mới; tạo bảng `tlm_<type>_utilization` cùng hai MV (5-phút và theo giờ) ở ClickHouse; thêm giá trị resource_type mới vào `tlm_util_binding` (chỉ là VARCHAR, không cần ALTER TABLE); và bổ sung phương thức tính trong Resource Utilization Service. Không cần restart Flink Job #1, không cần đổi schema `tlm_metrics`.

---

## 8. Ánh xạ kiểu dữ liệu DBML → DDL thật

Vì DBML không có sẵn các kiểu ClickHouse, file dùng bí danh dễ đọc. Bảng ánh xạ:

| Bí danh trong DBML | Kiểu ClickHouse thật |
| :----------------- | :------------------- |
| `DateTime64_3` | `DateTime64(3, 'Asia/Ho_Chi_Minh')` |
| `LowCardinality_String` | `LowCardinality(String)` |
| `Map_String` | `Map(LowCardinality(String), String)` |
| `AggState_Float32` | `AggregateFunction(avg/max/min, Float32)` |
| `AggState_Float64` | `AggregateFunction(avg/max/min, Float64)` |
| `AggState_UInt64` | `AggregateFunction(count, UInt64)` |
| `UInt8/UInt16/UInt32/UInt64` | giữ nguyên trong ClickHouse |

Với MariaDB, các kiểu `INT`, `VARCHAR`, `DATETIME`, `TINYINT`, `BIGINT`, `DECIMAL`, `JSON`, `ENUM`, `TEXT` ánh xạ trực tiếp. Lưu ý thiết kế gốc (file SQL nguồn) dùng `TINYINT UNSIGNED + CHECK constraint` thay cho `ENUM` ở các bảng telemetry để dễ tiến hóa; file DBML hiển thị `ENUM` cho dễ đọc sơ đồ, nhưng khi sinh DDL thật nên theo quy ước TINYINT+CHECK của file SQL nguồn (`telemetry_mariadb_v4_1.sql`).

DDL thật và chính xác cho phần telemetry/normalization nằm ở hai file nguồn: `telemetry_mariadb_v4_1.sql` (MariaDB) và `telemetry_normalization_v4_1.sql` (ClickHouse). File DBML là bản hợp nhất phục vụ trực quan hóa và làm tài liệu — khi có khác biệt về chi tiết kiểu dữ liệu, **hai file SQL nguồn là chuẩn**.

---

## 9. Bảng tra cứu nhanh — 59 bảng theo domain

**MariaDB (37 bảng):**

Hạ tầng cluster: `tlm_gnmic_cluster_config`, `tlm_gnmic_instances`, `tlm_consul_config`. Router: `tlm_routers`. Profile: `tlm_profiles`, `tlm_yang_paths`, `tlm_profile_sensors`, `tlm_profile_associations`. Quy tắc Flink: `tlm_revision_counter`, `tlm_path_aliases`, `tlm_metric_aliases`, `tlm_label_aliases`, `tlm_filter_rules`. Utilization: `tlm_util_binding`, `tlm_util_if_config`, `tlm_util_cpu_config`, `tlm_util_mem_config`, `tlm_util_queue_config`, `tlm_util_packet_error_config`, `tlm_util_resource_config`. Alert: `tlm_alert_rule`, `tlm_alert_rule_condition`, `tlm_alert_rule_audit`, `tlm_notification_channels`, `tlm_alert_rule_channels`, `tlm_active_alerts`. Users/Audit: `tlm_users`, `tlm_audit_log`. Settings: `tlm_credentials`, `tlm_kafka_config`, `tlm_clickhouse_config`, `tlm_grafana_config`, `tlm_grafana_dashboard_map`, `tlm_service_config`. YANG: `tlm_yang_version_bundles`, `tlm_yang_models`, `tlm_yang_nodes`.

**ClickHouse (22 bảng):**

Telemetry: `tlm_metrics`, `tlm_metrics_raw`. Mirror: `tlm_path_aliases_mirror`, `tlm_metric_aliases_mirror`, `tlm_label_aliases_mirror`, `tlm_filter_rules_mirror`. Alert: `tlm_alert_history`. Utilization raw: `tlm_if_utilization`, `tlm_cpu_utilization`, `tlm_mem_utilization`, `tlm_queue_utilization`, `tlm_packet_error_utilization`. MV 5-phút: `tlm_if_util_5min`, `tlm_cpu_util_5min`, `tlm_mem_util_5min`, `tlm_queue_util_5min`, `tlm_packet_error_util_5min`. MV theo giờ: `tlm_if_util_hourly`, `tlm_cpu_util_hourly`, `tlm_mem_util_hourly`, `tlm_queue_util_hourly`, `tlm_packet_error_util_hourly`.
