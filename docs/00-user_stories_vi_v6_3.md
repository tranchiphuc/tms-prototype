# Hệ Thống Quản Lý Telemetry Mạng (TMS): User Stories & Tiêu Chí Chấp Nhận

**Phiên bản 6.3 | Tháng 6 năm 2026**

> **Mục đích bản này:** Đồng bộ user story với **3 file HLD hiện hành** đang được phát triển và sẽ làm MVP frontend (prototype) trước:
> - `01-flink_processing_hld_vi_v2_4.md` — **Flink Job 1** (raw pipeline: parse → enrich → 4 nhóm rule → ClickHouse + Kafka).
> - `02-flink_job2_derived_metrics_hld_vi_v2_7.md` — **Flink Job 2** (derived metrics: computed / aggregated / delta; **hai sink**: ClickHouse + Kafka `derived_metrics`).
> - `03-alert_engine_streaming_job3_hld_vi_v1_4.md` — **Alert Engine (Flink Job 3, streaming)** (đánh giá rule theo luồng trên Kafka, **không còn poll ClickHouse**: basic/composite rule, sustain + dedup, alert history).
>
> **Thay đổi lớn so với v5.1:**
> 1. **[REMOVED]** "Resource Utilization Service" (T_poll, 5-min bucket, utilization binding, materialized view) — **không còn là module riêng**. Mọi metric phái sinh (bandwidth %, rate, aggregate) nay do **Flink Job 2 — Derived Metrics** tạo và ghi thẳng vào `ipms.tlm_metrics` (`raw_path = "derived"`). Nhóm story Utilization Binding (UB-*) cũ được **thay** bằng nhóm **Derived Metrics (DM-*)**.
> 2. **[CHANGED]** Alert Engine: bỏ mô hình Type A/B/C + "N consecutive 5-min bucket" + Redis. Thay bằng mô hình HLD v1.1: **basic rule** với `condition_kind` (`0=threshold`, `1=pct_change_prev`, `2=no_data`, `3=abs_delta_prev`), **composite rule** AND/OR cùng `entity_keys`, **sustain_samples** (chống nhiễu) + **dedup** (mirror trong MariaDB `tlm_alert_state`), history trong `ipms.alert_history` (TTL 90 ngày). "previous" = đúng **1 sample liền trước**, không phải mốc lịch sử 1h/1d/7d.
> 3. **[CHANGED]** Flink Rule: gộp đủ **4 nhóm rule** của Job 1 (Path Alias, Metric Alias, Label Alias, Filter) vào một nhóm story **FR-*** thống nhất, kèm preview/dry-run, refresh-status, versioning bằng `updated_at`.
> **Thay đổi v6.1 (đồng bộ HLD mới):** (a) `vendor_code` thêm giá trị `OpenConfig` (data model vendor-neutral — MariaDB v4.3, Job 1 v2.4): cập nhật FR-01/FR-02. (b) Job 2 v2.4: `input_metrics` là **array of string** (`alias_metric`); path scope do `scope_path_alias_id` cấp rule đảm nhiệm; `assembly_window_seconds` đã vào DDL; versioning `tlm_derived_rules`/`tlm_alert_rules` bằng `updated_at` (bỏ `revision`): cập nhật nhóm DM-*. (c) Alert Engine: thêm trường severity (thứ tự cập nhật ở v6.2 — xem dưới).
>
> **Thay đổi v6.2 (đồng bộ HLD Job 2 v2.6 + Alert Engine Streaming Job 3 v1.3):**
> - **[CHANGED] Severity đảo thứ tự:** `0=info, 1=warning, 2=error, 3=critical` (trước v6.1: `2=critical, 3=error`) — để mã tăng dần đúng theo mức nghiêm trọng (`critical` nặng nhất). Cập nhật AL-01.
> - **[CHANGED] Alert Engine là streaming (Flink Job 3), KHÔNG còn poll ClickHouse.** Engine đánh giá rule **ngay trên luồng** Kafka `processed_metrics` (+ `derived_metrics`), SLO alert **≤ 5s** (đo từ lúc metric sẵn sàng trên `processed_metrics`). Spring Boot "Alert Rule Service" **thu hẹp vai trò**: chỉ còn quản lý rule (CRUD/validate), preview/dry-run, và đọc state cho dashboard. Cập nhật bối cảnh AL-*, AL-09 (cập nhật near-realtime thay vì "poll interval"), AL-13 (hiển thị **sức khoẻ Flink job** thay cho poll interval).
> - **[CHANGED] Job 2 có thêm Kafka sink `derived_metrics`** (song song ClickHouse) — **bắt buộc** để Job 3 alert được trên derived metric (vì Job 3 không đọc ClickHouse). Cập nhật bối cảnh DM-* và Tóm Tắt Kiến Trúc.
> - **[CHANGED] Cập nhật tham chiếu HLD:** Job 2 → `v2.6`, Alert Engine → `03-alert_engine_streaming_job3_hld_vi_v1_3.md`.
>
> **Thay đổi v6.3 (đồng bộ HLD Job 2 v2.7 + Alert Engine Streaming Job 3 v1.4):**
> - **[CHANGED] Cập nhật tham chiếu HLD:** Job 2 → `02-flink_job2_derived_metrics_hld_vi_v2_7.md`, Alert Engine → `03-alert_engine_streaming_job3_hld_vi_v1_4.md`.
> - **[NOTE] Job 2 v2.6 → v2.7 chỉ là sửa tài liệu** (đính chính tham chiếu chéo tên file Job 3 `04-…` → `03-…` ở 2 chỗ). HLD ghi rõ **KHÔNG đổi** schema / pipeline / API / NFR. ⇒ Nhóm story **DM-*** giữ nguyên nội dung, chỉ đổi số phiên bản tham chiếu.
> - **[CHANGED] Job 3 v1.3 → v1.4 — chốt ngữ nghĩa REFIRED cho `no_data`:** mặc định **không refire chu kỳ** — mỗi đợt im lặng chỉ FIRED **đúng một lần**, sau đó RESOLVED khi series báo lại (resolve-on-return) **hoặc** aging-RESOLVED khi hết aging-timeout; **`REFIRED` KHÔNG xuất hiện** cho `no_data` ở chế độ mặc định. Cập nhật **AL-04** và ghi chú ở **AL-11**.
> - **[ADDED][C] AL-15 (Could Have):** tuỳ chọn **nhắc lại định kỳ** cho `no_data` theo `dedup_seconds` — ánh xạ "tuỳ chọn mở rộng" §6.5 của HLD v1.4; **mặc định TẮT**.
> - **[CLARIFIED] DM-03:** làm rõ cho phép **nhiều aggregated rule trên cùng một input** với `window_seconds` khác nhau (state tách theo `rule_id` — Job 2 §5.2.1, đã có từ v2.6) — bổ sung khi rà soát hoàn chỉnh theo yêu cầu.
> - **[FIXED] Phụ Lục C — Story Count:** đính chính số liệu P/N của các nhóm cho khớp bảng story thực tế (bản v6.2 lệch ở FR/DM/AL) và thêm cột **C**.
> - **[NOTE] Không có thay đổi thiết kế nào khác** từ hai HLD: kiến trúc, hợp đồng I/O ra NOC PRO, schema (`tlm_alert_rules`/`tlm_alert_state`/`ipms.alert_history`, `tlm_derived_rules`), và SLO alert ≤ 5s đều **không đổi**. Các sửa đổi còn lại trong v1.3/v1.4 (`KeyedBroadcastProcessFunction`, no_data-child của composite trả `raw=false`, Phụ Lục A DDL tham chiếu) là **chi tiết hiện thực nội bộ** — không phát sinh user story mới.
>
> 4. **[ADDED]** Đánh dấu **MVP** cho mọi story thuộc 3 HLD. Các phần ngoài phạm vi 3 HLD (Profile, Liên Kết Profile, Phân Công Agent gNMIc cluster, Kiểm Kê Router, Grafana, Cài Đặt hạ tầng…) được **giữ lại nhưng hạ ưu tiên xuống "Phase Sau"** — không thuộc prototype lần này.

---

## Chú Giải Ký Hiệu

| Ký hiệu | Ý nghĩa |
| :------ | :------ |
| ⭐ **MVP** | Thuộc phạm vi prototype lần này (bám sát 1 trong 3 HLD) |
| 🕓 **Phase Sau** | Giữ lại để tham chiếu, **không** làm trong prototype lần này |
| ★ | Story mới ở bản v6.0 |
| ◆ | Story sửa đổi so với v5.1 |
| ✕ | Story đã xóa / thay thế |

**Độ ưu tiên:** P = Phải Có, N = Nên Có, C = Có Thể Có.

---

## Tóm Tắt Kiến Trúc (đồng bộ HLD)

- **Lớp nguồn:** 300 router (Cisco, Juniper, Nokia), một chế độ **gNMI Dial-In**, ~10.000 metrics/router/60s ≈ **50 triệu metrics/phút** toàn hệ thống.
- **Lớp thu thập:** 3 instance **gNMIc** (cluster mode), serialize sang JSON, đẩy vào Kafka.
- **Lớp hàng đợi:** Kafka — topic `telemetry.raw` (vào Job 1), `processed_metrics` (Job 1 → Job 2 + Job 3 + downstream), `derived_metrics` (Job 2 → Job 3, topic **riêng** để tránh Job 2 tự consume vòng lặp), `alerts` (Alert Engine Job 3 → NOC PRO).
- **Lớp xử lý:**
  - **Flink Job 1** — parse gNMIc JSON → device enrichment → 4 nhóm rule (Path/Metric/Label Alias, Filter) → ghi `ipms.tlm_metrics` + `ipms.tlm_metrics_raw` (TTL 7 ngày) + Kafka `processed_metrics`.
  - **Flink Job 2** — consume `processed_metrics`, tính **derived metrics** (computed / aggregated / delta), **hai sink song song**: ghi `ipms.tlm_metrics` với `raw_path = "derived"` **và** publish Kafka `derived_metrics` (cho Job 3).
  - **Flink Job 3 (Alert Engine, streaming)** — consume `processed_metrics` + `derived_metrics`, broadcast rule snapshot, đánh giá basic/composite theo luồng (prev-sample/sustain/dedup/no_data bằng keyed state), emit fire/refire/resolve sang **NOC PRO** qua Kafka `alerts` + audit `ipms.alert_history` + mirror `tlm_alert_state`.
- **Lớp lưu trữ:** **ClickHouse** (`ipms.tlm_metrics`, `ipms.tlm_metrics_raw`, `ipms.alert_history`); **MariaDB** (device catalog, rule tables: `tlm_path_aliases`, `tlm_metric_aliases`, `tlm_label_aliases`, `tlm_filter_rules`, `tlm_derived_rules`, `tlm_alert_rules`, `tlm_alert_state`).
- **Alert Engine (Flink Job 3, streaming):** đánh giá rule **ngay trên luồng** (không poll ClickHouse), SLO alert **≤ 5s** (đo từ lúc metric sẵn sàng trên `processed_metrics`); sustain + dedup; ghi history; emit fire/refire/resolve sang **NOC PRO** qua Kafka `alerts`. **Không** làm escalation / notification / silence (thuộc NOC PRO). **Spring Boot "Alert Rule Service"** chỉ còn quản lý rule (CRUD/validate), preview/dry-run, và đọc `tlm_alert_state` cho dashboard.
- **Versioning rule:** mọi rule table dùng `updated_at` (DATETIME(6), `ON UPDATE CURRENT_TIMESTAMP(6)`) làm version; `pushed_at = NULL` đánh dấu chưa mirror sang ClickHouse; Flink/Engine pick up thay đổi trong vòng một chu kỳ refresh (Job 1/2 vài phút; Job 3 ≤ vài chục giây).
- **Grafana:** phân tích sâu (trend dài ngày, capacity) — **Phase Sau**.

---

# PHẦN I — MVP (3 HLD) ⭐

Đây là phạm vi prototype frontend lần này: **Flink Job 1 (FR-*)**, **Flink Job 2 Derived Metrics (DM-*)**, **Alert Engine (AL-*)**, cùng các màn hình hỗ trợ trực tiếp cho 3 module (Pipeline Job View, Fallthrough Monitor, Metric Explorer tối thiểu).

---

## 1. Quản Lý Flink Rule — 4 Nhóm Rule (FR-*) ⭐

> **Bám HLD:** `01-flink_processing_hld_vi_v2_4.md` §3, §4, §8.
>
> **Bối cảnh:** Flink Job 1 áp 4 nhóm rule động trước khi ghi ClickHouse. Rule quản lý qua REST API (`/api/v1/flink`); Flink **không** gọi API mà load snapshot từ MariaDB và refresh định kỳ (vài phút). Mọi write đẩy `updated_at` tiến lên + reset `pushed_at=NULL`. **Nguyên tắc fallthrough:** miss Path/Metric/Label Alias hay cache miss device **không drop** record; chỉ **Filter Rule** mới drop chủ động.

| ID | User Story | Tiêu Chí Chấp Nhận | Ưu Tiên | Màn Hình |
| :-- | :-- | :-- | :-- | :-- |
| FR-01 ★⭐ | Là operator, tôi muốn xem tất cả **Path Alias** rule, để biết YANG container path nào đã được đặt alias ngắn gọn. | Bảng hiển thị: `vendor_code` (Cisco/Juniper/Nokia/OpenConfig/All), `original_path`, `alias_path`, `status` (Active/Deprecated), `updated_at`, `pushed_at`. Lọc theo vendor, status, `q` (tìm theo path). Phân trang (mặc định 50, tối đa 200), sort `-updated_at`. | P | Flink Rule |
| FR-02 ★⭐ | Là operator, tôi muốn tạo/sửa **Path Alias** rule, để map một YANG container path dài thành alias gọn (vd `xr_watchdog_memory`). | Form: `vendor_code`, `original_path`, `alias_path`. Validation: `vendor_code ∈ {Cisco,Juniper,Nokia,OpenConfig,All}` (`OpenConfig` = rule cho path data model OpenConfig, áp mọi vendor thiết bị; cảnh báo nếu `original_path` không bắt đầu bằng `openconfig-`); unique `(vendor_code, original_path)` và `(vendor_code, alias_path, status)` → trùng trả `409`. **`alias_path` không sửa được sau khi Active** — muốn đổi phải Deprecate row cũ + tạo row mới (cảnh báo cascade các metric/label alias con). Lưu xong báo rõ "có hiệu lực sau ~1 chu kỳ refresh Flink". | P | Flink Rule |
| FR-03 ★⭐ | Là operator, tôi muốn xem/tạo/sửa **Metric Alias** rule (đổi tên leaf + transform value), để chuẩn hóa metric nghiệp vụ và đổi đơn vị. | Form: `vendor_code`, `path_alias_id` (null = any-path; NOT NULL = path-scoped), `original_name`, `alias_metric`, `value_type_override ∈ {null,1,2,3}`, `source_unit`, `target_unit`, `transform_kind` (`0=identity,1=linear,2=expression,3=enum_mapping`), `scale_factor`, `offset_value`, `priority`. Validation theo CHECK: `transform_kind=2` ⇒ bắt buộc `transform_expression`; `=3` ⇒ bắt buộc `enum_mapping`; `value_type_override ∈ {2,3}` ⇒ unit phải null, `scale_factor=1`, `offset_value=0`, `transform_kind ∈ {0,3}` (vi phạm trả `422`). Unique theo `path_scope_key=IFNULL(path_alias_id,0)`. | P | Flink Rule |
| FR-04 ★⭐ | Là operator, tôi muốn xem/tạo/sửa **Label Alias** rule, để chuẩn hóa tag key/value từ nhiều vendor về chuẩn chung (vd Cisco `intf`, Juniper `ifd-name` → `if_name`). | Form: `vendor_code`, `path_alias_id`, `original_key`, `alias_key`, `lv_kind` (`0=identity,1=regex,2=enum_mapping`), kèm `lv_pattern`+`lv_replace` (khi regex) hoặc `lv_mapping` (khi enum), `priority`. Validation: regex ⇒ bắt buộc pattern+replace; enum ⇒ bắt buộc mapping. Preview realtime: nhập value mẫu → hiển thị value sau transform. Tag không có rule → giữ nguyên (default allow). | P | Flink Rule |
| FR-05 ★⭐ | Là operator, tôi muốn xem/tạo/sửa **Filter Rule** (drop chủ động), để loại metric không cần thiết trước khi ghi ClickHouse. | Form: `vendor_code`, `match_path` (rỗng = mọi path), `match_metric` (null = mọi metric trong path), `filter_expression` (rỗng = luôn TRUE), `filter_action` (`0=EXCLUDE_IF_MATCH` drop khi TRUE / `1=INCLUDE_IF_MATCH` chỉ giữ khi TRUE), `priority`. **Cảnh báo nổi bật:** "Metric bị drop KHÔNG ghi vào ClickHouse và KHÔNG phục hồi được." Khuyến nghị `preview` trước khi lưu; lưu ý `priority` để rule rộng không đè rule hẹp. | P | Flink Rule |
| FR-06 ★⭐ | Là operator, tôi muốn **preview / dry-run** một rule trên mẫu metric thực gần nhất, để xác nhận trước khi lưu mà không ảnh hưởng pipeline. | Nút "Preview" gọi `POST /flink/{group}/preview` với rule chưa lưu (hoặc `{id}`) + `sample_window` + lọc tùy chọn (`vendor_code`, `device_id`). Hiển thị `sampled_records`, `matched_records`, và danh sách `before/after` (metric alias) hoặc số record **sẽ bị drop/giữ** (filter). Ghi chú rõ: preview phản ánh **một-rule, một-thời-điểm** — không mô phỏng tương tác `priority` nhiều rule. | P | Flink Rule |
| FR-07 ★⭐ | Là operator, tôi muốn xem **trạng thái hiệu lực rule** (refresh-status), để biết thay đổi đã "sống" trong Flink/ClickHouse chưa. | Panel gọi `GET /flink/refresh-status`. Mỗi bảng rule hiển thị: `max_updated_at`, `rows_pending_push` (số row `pushed_at IS NULL`), `oldest_pending_pushed_at`, `last_push_completed_at`. Cảnh báo khi `rows_pending_push` cao kéo dài (sync job có vấn đề). Ghi chú: "Flink refresh ~ vài phút; `pushed_at=NULL` = chưa mirror sang ClickHouse." | P | Flink Rule |
| FR-08 ★⭐ | Là operator, tôi muốn **Fallthrough Monitor** — metric không khớp Path/Metric Alias, để phát hiện metric mới chưa cấu hình và bổ sung kịp thời. | Panel query `ipms.tlm_metrics` với `path_id = 0` (auto-derived path) hoặc `metric_name = raw_metric_name` trong 1 giờ qua. Hiển thị: `raw_path` (LCP auto-derived), `raw_metric_name` (full YANG leaf), vendor, device, số lần xuất hiện. Nút "Tạo Alias" mở sẵn form FR-02/FR-03 với dữ liệu điền trước. Ghi chú: "Fallthrough vẫn được lưu với tên thô — không mất dữ liệu." | N | Flink Rule |
| FR-09 ★⭐ | Là operator, tôi muốn soft-delete (Deprecate) một rule và thấy cảnh báo cascade, để không tạo alias con mồ côi khi Deprecate Path Alias. | `DELETE` set `status=0`, trả `204`; `updated_at` tự tiến + reset `pushed_at`. Khi Deprecate **Path Alias** còn metric/label alias Active trỏ tới: chặn `409` (hoặc yêu cầu `?force=true`) và liệt kê danh sách rule con bị ảnh hưởng; nếu force thì reset `pushed_at=NULL` cho các con. PATCH nhanh `{status:0}` hoặc `{priority:120}`. | P | Flink Rule |

---

## 2. Derived Metrics — Flink Job 2 (DM-*) ⭐

> **Bám HLD:** `02-flink_job2_derived_metrics_hld_vi_v2_7.md` (v2.7 = chỉ sửa tài liệu; thiết kế giữ nguyên so với v2.6).
>
> **Bối cảnh:** Job 2 tạo metric mới từ metric thô đã có mà **không** đổi cấu hình router. 3 loại: **computed** (`derive_kind=0`), **aggregated** (`derive_kind=1`), **delta** (`derive_kind=2`). (Composite/interval-join đã **bị loại bỏ** từ v2.3.) Mỗi input định danh bằng `alias_metric` (string, khớp trực tiếp `metric_name` trên stream); path scope do `scope_path_alias_id` cấp rule đảm nhiệm (Job 2 v2.4). Mỗi derived row được ghi **đồng thời ra hai sink** (v2.5): (a) ClickHouse `ipms.tlm_metrics` với `raw_path="derived"`, `value_type=1` (lưu trữ/dashboard) và (b) Kafka `derived_metrics` (topic **riêng** — bắt buộc để Alert Engine streaming Job 3 alert được trên derived metric, vì Job 3 không đọc ClickHouse). **Đây là phần thay thế cho "Resource Utilization Service" cũ.**

| ID | User Story | Tiêu Chí Chấp Nhận | Ưu Tiên | Màn Hình |
| :-- | :-- | :-- | :-- | :-- |
| DM-01 ★⭐ | Là operator, tôi muốn xem tất cả **derived rule** với loại và trạng thái, để nắm toàn bộ metric phái sinh đang được tính. | Bảng: `rule_name`, `output_metric`, `output_unit`, `derive_kind` (Computed/Aggregated/Delta), số input, `scope_path_alias_id`/`scope_device_ids` (phạm vi), `priority`, `status`, `updated_at`. Lọc theo `derive_kind`, status; tìm theo `output_metric`. | P | Derived Metrics |
| DM-02 ★⭐ | Là operator, tôi muốn tạo **Computed** rule (`derive_kind=0`), để tính một giá trị từ nhiều input cùng `(device_id, labels)` theo công thức. | Form: `output_metric` (unique toàn active, không trùng `alias_metric` nào trong `tlm_metric_aliases`), `output_unit`, `input_metrics[]` = list `alias_metric` (string, **đôi một khác nhau**), `expression` (biến = `alias_metric` của input), `static_constants` (Map<alias_metric,Double> thay cho input vắng mặt), `assembly_window_seconds` (mặc định ≈1.5× sample ≈ 90s; chỉ cho computed, phải > 0 nếu khai báo). Validation: expression NOT NULL; window params NULL. Ghi chú phân biệt **`assembly_window_seconds`** (cửa sổ chờ gom đủ input) với **state TTL** (lưới an toàn, đặt > assembly). | P | Derived Metrics |
| DM-03 ★⭐ | Là operator, tôi muốn tạo **Aggregated** rule (`derive_kind=1`), để tổng hợp metric theo tumbling window. | Form: `output_metric`, `output_unit`, đúng 1 input `alias_metric` (string), `window_seconds`, `agg_function` (`0=avg,1=max,2=min,3=sum,4=rate`). Validation: `window_seconds` + `agg_function` NOT NULL; `expression` NULL. Emit tại window close; độ trễ ≈ `window_seconds × 1.25`. Cho phép **nhiều aggregated rule trên cùng một input** với `window_seconds` khác nhau — state/cửa sổ tách biệt theo `rule_id` (Job 2 §5.2.1). | P | Derived Metrics |
| DM-04 ★⭐ | Là operator, tôi muốn tạo **Delta** rule (`derive_kind=2`), để tính rate giữa hai sample liên tiếp (vd counter bytes → Mbps). | Form: `output_metric`, `output_unit`, 1 input, `delta_scale_factor` (vd `8/1_000_000` cho bytes→Mbps), `delta_reset_threshold` (null = không xử lý counter reset). Công thức: `(current − prev) × scale / Δt`. Validation: expression + window params NULL. Ghi chú: sample đầu chỉ lưu state (không emit); phát hiện reset → bỏ sample, restart tracking; `Δt ≤ 0` → bỏ emit. Độ trễ ≈ 1 sample interval (~60s). | P | Derived Metrics |
| DM-05 ★⭐ | Là operator, tôi muốn **giới hạn phạm vi (scope)** của rule, để áp derived metric chỉ cho một số device/path. | Form con: `scope_device_ids` (JSON array device_id; null = mọi thiết bị), `scope_path_alias_id` (null = mọi path). Ghi rõ (v2.4): `scope_path_alias_id` vừa giới hạn **toàn rule** vừa là cơ chế **khử nhập nhằng path** cho input matching — đặt NOT NULL khi `alias_metric` trùng tên khác nghĩa giữa các path. | P | Derived Metrics |
| DM-06 ★⭐ | Là operator, tôi muốn **preview** kết quả tính derived trước khi lưu, để xác nhận công thức/đơn vị đúng. | Nút "Preview" gửi rule + `sample_inputs` (vd cặp prev/curr cho delta, hoặc map input cho computed). Hiển thị `result`, `unit`, `warnings`, `errors`. Ghi chú: preview **một-thời-điểm** — không mô phỏng windowing/watermark thực, không phát hiện reset qua chuỗi sample. | N | Derived Metrics |
| DM-07 ★⭐ | Là operator, tôi muốn **lookup metric alias** khi cùng `alias_metric` tồn tại ở nhiều path, để quyết định đặt `scope_path_alias_id` cho rule. | Tiện ích UI gọi `GET /metric-aliases?alias_metric=...` trả danh sách `{alias_metric, path_alias_id, alias_path, vendor_code}`. Cùng tên ở nhiều path nhưng **cùng nghĩa** (đa-vendor) → để `scope_path_alias_id=null`; **khác nghĩa** → đặt scope cụ thể (API cũng tự cảnh báo khi tạo rule scope-NULL với alias trùng ≥2 path). (Chỉ hỗ trợ UI — Flink không phụ thuộc.) | N | Derived Metrics |
| DM-08 ★⭐ | Là operator, tôi muốn xem **observability của Job 2** (rule cấu hình sai), để phát hiện rule computed thường xuyên thiếu input. | Panel hiển thị per-rule counter `derived_incomplete_total` (số lần hết cửa sổ gom mà chưa đủ input) và counter emit per `output_metric`. Giá trị `incomplete` cao kéo dài → cảnh báo "rule có thể cấu hình sai input/scope". | N | Derived Metrics |
| DM-09 ★⭐ | Là operator, tôi muốn enable/disable & soft-delete derived rule, để tạm dừng hoặc gỡ metric phái sinh mà không mất cấu hình. | Toggle Active/Deprecated (`status`); `DELETE` soft (status=0). Khi disable, Flink ngừng emit ở chu kỳ refresh kế (≤60s); state tự expire theo TTL. Mọi thay đổi đẩy `updated_at`/reset `pushed_at`. Unique `(output_metric, status)`. | P | Derived Metrics |

---

## 3. Cảnh Báo — Alert Engine (AL-*) ⭐

> **Bám HLD:** `03-alert_engine_streaming_job3_hld_vi_v1_4.md` (Flink Job 3, streaming — thay cho poll engine cũ). Mô hình rule & ngữ nghĩa fire/refire/resolve giữ nguyên từ Alert Engine HLD gốc.
>
> **Khái niệm chính (theo HLD):**
> - **Basic rule** (`rule_kind=0`): 1 điều kiện trên 1 metric, `condition_kind` ∈ {`0=threshold`, `1=pct_change_prev`, `2=no_data`, `3=abs_delta_prev`}. **"previous" = đúng 1 sample liền trước** theo `event_time` (KHÔNG phải mốc lịch sử 1h/1d/7d).
> - **Composite rule** (`rule_kind=1`): gộp nhiều basic rule **cùng `entity_keys`** trên một device bằng **AND/OR**. `missing_as` xử lý child thiếu data.
> - **entity_keys:** tập label key xác định "một đối tượng" (vd `["if_name"]` = mỗi interface; `[]` = cấp device). Sinh `entity_fingerprint` làm khóa nhóm + khóa dedup.
> - **Sustain (`sustain_samples`):** số lần evaluate liên tiếp điều kiện đúng mới satisfied — chống nhiễu. Giữ trong **Flink keyed state** (bền qua checkpoint).
> - **Dedup (`dedup_seconds`):** suppress re-fire cùng `(rule, device, entity)`. Nguồn quyết định là **Flink keyed state**; `tlm_alert_state` (MariaDB) là **bản mirror** cho dashboard/seed cold-start. Mặc định 3600s.
> - **Severity (v6.2):** `0=info, 1=warning, 2=error, 3=critical` (mã tăng dần theo mức nghiêm trọng).
> - **Định danh metric:** `(alias_metric, path_alias_id)`. Raw metric dùng `path_alias_id` thực; **derived metric dùng `path_alias_id = 0`**.
> - Engine **streaming (Flink Job 3)** — đánh giá rule **ngay trên luồng** Kafka `processed_metrics` (+ `derived_metrics`), **không poll ClickHouse**; SLO alert **≤ 5s** (đo từ lúc metric sẵn sàng trên `processed_metrics`). **Không** làm escalation/notification/silence (NOC PRO lo). Output fire/refire/resolve qua Kafka `alerts`; history `ipms.alert_history` TTL 90 ngày.

| ID | User Story | Tiêu Chí Chấp Nhận | Ưu Tiên | Màn Hình |
| :-- | :-- | :-- | :-- | :-- |
| AL-01 ◆⭐ | Là operator, tôi muốn xem danh sách alert rule với trạng thái & tóm tắt, để nắm toàn bộ rule đang hoạt động. | Bảng: `rule_name`, `rule_kind` (Basic/Composite), `condition_kind` (với basic), metric `(alias_metric, path_alias_id)`, `entity_keys`, severity (`0=Info/1=Warning/2=Error/3=Critical`, mã màu), `status` (Active/Disabled), `sustain_samples`, `dedup_seconds`, `updated_at`. Lọc theo severity, status, rule_kind; tìm theo tên. | P | Cảnh Báo |
| AL-02 ◆⭐ | Là operator, tôi muốn tạo basic rule **threshold** (`condition_kind=0`), để fire khi giá trị vi phạm ngưỡng cố định. | Form: tên, severity, metric `(alias_metric, path_alias_id)`, `entity_keys`, `comparator` (`0=> ,1=>=,2=< ,3=<=,4== ,5=!=`), `threshold`, `sustain_samples` (≥1), `dedup_seconds` (≥0). Validation: `comparator`+`threshold` bắt buộc. Ghi rõ "FIRE sau `sustain_samples` lần evaluate liên tiếp vi phạm". | P | Cảnh Báo |
| AL-03 ◆⭐ | Là operator, tôi muốn tạo basic rule **pct_change_prev** (`condition_kind=1`), để fire khi % thay đổi so với **sample liền trước** vi phạm. | Form thêm: `pct_abs` (1 = dùng trị tuyệt đối của %), `comparator`, `threshold` (đơn vị %). Logic: `pct=(cur−prev)/prev*100` (hoặc `|pct|` nếu `pct_abs=1`), rồi `pct <comparator> threshold`. Ghi chú rõ: "previous = đúng 1 sample liền trước, cần ≥2 sample (~2 chu kỳ ≈120s)". | P | Cảnh Báo |
| AL-04 ◆⭐ | Là operator, tôi muốn tạo basic rule **no_data** (`condition_kind=2`), để fire khi series **từng có** dữ liệu nhưng ngừng báo. | Form: metric, `entity_keys`, `no_data_seconds` (>0). Validation: `no_data_seconds` bắt buộc; `comparator`/`threshold` **phải vắng**. Ghi chú: chỉ fire cho series đã "known" (từng tồn tại). **Ngữ nghĩa fire (HLD v1.4 §6.5):** mỗi đợt im lặng chỉ **FIRED đúng một lần** — **KHÔNG refire chu kỳ**; tự **RESOLVED** khi series báo lại (resolve-on-return) hoặc khi hết aging-timeout (aging-RESOLVED). Muốn nhắc lại định kỳ → bật tuỳ chọn ở **AL-15**. | P | Cảnh Báo |
| AL-05 ◆⭐ | Là operator, tôi muốn tạo basic rule **abs_delta_prev** (`condition_kind=3`, tùy chọn), để fire theo chênh lệch tuyệt đối với sample trước. | Form như AL-02 + `pct_abs`. Logic: `d=cur−prev` (hoặc `|cur−prev|`), rồi `d <comparator> threshold`. | N | Cảnh Báo |
| AL-06 ◆⭐ | Là operator, tôi muốn tạo **composite rule** (AND/OR), để chỉ fire khi nhiều điều kiện cùng entity đồng thời thỏa — giảm nhiễu. | Form: tên, severity, `entity_keys` (áp cho TẤT CẢ child), `logical_op` (`0=AND,1=OR`), `child_rule_ids[]`, `missing_as` (`0=false`/skip), `dedup_seconds`. Validation: mọi child tồn tại, là **basic** (`rule_kind=0`), `status=1`, **cùng `entity_keys`** với composite; không tự tham chiếu; không lồng composite. | P | Cảnh Báo |
| AL-07 ★⭐ | Là operator, tôi muốn **preview / dry-run** rule trên dữ liệu thực gần nhất, để chỉnh ngưỡng trước khi lưu. | `POST /alerts/rules/preview` với rule chưa lưu (hoặc `{rule_id}`). Trả `evaluated_series`, `matching_series`, `skipped_series` (thiếu prev/chia 0/chưa known), và mẫu `{device_id, entity, cur_value, prev_value, would_fire}`. Ghi chú: preview phản ánh **boolean tức thời**, KHÔNG mô phỏng sustain/dedup theo thời gian. | P | Cảnh Báo |
| AL-08 ★⭐ | Là operator, tôi muốn bật/tắt (disable) rule mà không xóa, để tạm dừng trong bảo trì mà giữ cấu hình. | PATCH `{status:0}`. Rule disabled không được evaluate, engine emit `RESOLVED` cho instance đang FIRING ở chu kỳ kế. Đổi `entity_keys` → cảnh báo state cũ thành mồ côi (sẽ được dọn + RESOLVED). Thay đổi đẩy version (`updated_at`). | P | Cảnh Báo |
| AL-09 ★⭐ | Là operator, tôi muốn xem danh sách **alert đang FIRING**, để biết sự cố nào đang xảy ra. | Bảng "Active Alerts": `fired_at`, rule, severity (màu), device, entity (vd `if_name=Gi0/0/0/0`), giá trị trigger, ngưỡng, trạng thái dedup (FIRING / SUPPRESSED). Lọc theo severity, device. Cập nhật **gần realtime** (engine streaming; bảng đọc từ `tlm_alert_state` mirror + `alert_history`, refresh theo nhịp UI). | P | Cảnh Báo |
| AL-10 ★⭐ | Là operator, tôi muốn xem **firing state** của một rule, để hiểu vì sao một alert đang bị suppress. | `GET /alerts/rules/{id}/state` trả per-instance: `device_id`, `entity_fingerprint`, `last_state`, `last_fired_at`, `last_resolved_at`, `dedup_remaining_seconds`. Read-only, phục vụ debug. | N | Cảnh Báo |
| AL-11 ★⭐ | Là operator, tôi muốn xem **alert history** với FIRED/REFIRED/RESOLVED, để phân tích pattern và MTTR. | Bảng query `ipms.alert_history`: timestamp, rule, severity, device, entity, giá trị trigger, loại sự kiện (FIRED/REFIRED/RESOLVED). Lọc theo khoảng thời gian, rule, device, severity, loại. Mặc định: 24h gần nhất. TTL 90 ngày. Ghi chú: với `no_data`, sự kiện **`REFIRED` không xuất hiện** ở chế độ mặc định (xem AL-04 / AL-15). | P | Cảnh Báo |
| AL-12 ★⭐ | Là operator, tôi muốn **xóa rule** an toàn với cảnh báo phụ thuộc composite, để không phá composite đang dùng. | Soft-delete (`status=0`), trả `204`; emit `RESOLVED` cho instance đang FIRING. **Chặn `409`** nếu basic rule đang được composite Active tham chiếu (kèm danh sách composite phụ thuộc); hoặc `?force=true` để vô hiệu các composite đó. | P | Cảnh Báo |
| AL-13 ★⭐ | Là operator, tôi muốn xem **trạng thái Alert Engine (Flink Job 3)**, để xác nhận engine chạy đúng và đáp ứng SLO ≤ 5s. | Widget phản ánh **sức khoẻ Flink job** (thay cho "poll interval" cũ): trạng thái (Running/Failed/Stopped), checkpoint gần nhất, **consumer lag** (`processed_metrics`/`derived_metrics`), **watermark lag**, **eval latency** (ms), số rule active, số fire trong 1h. Cảnh báo nếu eval latency / lag vượt ngưỡng làm SLO ≤ 5s gặp rủi ro. | P | Cảnh Báo |
| AL-14 ✕ | ~~Cấu hình kênh thông báo (email/Slack/webhook) per rule~~ | **Loại khỏi Alert Engine** — notification delivery thuộc **NOC PRO**. Engine chỉ emit fire/resolve qua Kafka `alerts`. | — | (NOC PRO) |
| AL-15 ★⭐ | Là operator, tôi muốn (tuỳ chọn) cho phép **nhắc lại định kỳ** alert `no_data` khi series vẫn im lặng kéo dài, để không bỏ sót sự cố đang diễn ra. | Ánh xạ **"tuỳ chọn mở rộng" §6.5 HLD v1.4** (KHÔNG phải hành vi mặc định). Khi **bật** (mặc định **TẮT**): engine đăng ký lại no_data timer mỗi `no_data_seconds` ⇒ sinh `REFIRED` mỗi khi vượt cửa sổ `dedup_seconds` (đồng nhất hành vi refire của rule số). Đây là **lựa chọn cấu hình triển khai**; UI hiển thị trạng thái bật/tắt + cảnh báo "cần đồng bộ với impl guide khi bật". Khi TẮT: giữ hành vi mặc định (FIRED một lần/đợt im lặng — AL-04). | C | Cảnh Báo |

---

## 4. Giám Sát Pipeline — Góc Nhìn Job (PL-*) ⭐ (rút gọn cho MVP)

> Chỉ giữ phần phục vụ trực tiếp 3 HLD: trạng thái Flink Job 1, Job 2 & Job 3 (Alert Engine streaming), Kafka topic liên quan, ghi ClickHouse. Phần gNMIc cluster/Consul chuyển **Phase Sau**.

| ID | User Story | Tiêu Chí Chấp Nhận | Ưu Tiên | Màn Hình |
| :-- | :-- | :-- | :-- | :-- |
| PL-01 ◆⭐ | Là operator, tôi muốn xem trạng thái **Flink Job 1, Job 2 & Job 3** và thông lượng, để phát hiện lỗi xử lý hoặc nghẽn. | Panel Flink: mỗi job hiển thị tên, trạng thái (Running/Failed/Stopped), records/s, uptime, số rule active (Job 1: Path/Metric/Label/Filter; Job 2: số derived rule; Job 3: số alert rule active). Cảnh báo nếu không có rule nào được tải hoặc job restart liên tục. | P | Giám Sát Pipeline |
| PL-02 ◆⭐ | Là operator, tôi muốn theo dõi **Kafka topic** liên quan pipeline, để phát hiện backlog. | Panel Kafka: topic `telemetry.raw`, `processed_metrics`, `derived_metrics`, `alerts` — consumer lag, msg/s. Lag vượt ngưỡng → cảnh báo vàng. | P | Giám Sát Pipeline |
| PL-03 ◆⭐ | Là operator, tôi muốn theo dõi **ghi ClickHouse** (`tlm_metrics`, `tlm_metrics_raw`, `alert_history`), để phát hiện sự cố lưu trữ. | Panel ClickHouse: inserts/s, batch size, query latency p50/p99, dung lượng đĩa, error rate, độ trễ sink. Cảnh báo nếu sink fail/retry tăng (HLD Job 1 §7: ~833K rows/s yêu cầu batch lớn). | P | Giám Sát Pipeline |
| PL-04 ⭐ | Là operator, tôi muốn xem **end-to-end latency** Kafka-in → ClickHouse-out, để xác nhận đáp ứng SLO alert. | Hiển thị độ trễ pipeline theo tầng (parse → enrich → rule → sink). Cảnh báo nếu vượt ngân sách latency làm alert SLO 5–30s gặp rủi ro. | N | Giám Sát Pipeline |

---

## 5. Khám Phá Dữ Liệu Tối Thiểu (DE-*) ⭐ (hỗ trợ MVP)

> Màn hình tra cứu nhanh `ipms.tlm_metrics` để soạn rule (FR/DM/AL) và xác minh fallthrough/derived. Không phải charting engine đầy đủ (việc đó để Grafana — Phase Sau).

| ID | User Story | Tiêu Chí Chấp Nhận | Ưu Tiên | Màn Hình |
| :-- | :-- | :-- | :-- | :-- |
| DE-01 ◆⭐ | Là operator, tôi muốn query nhanh metric theo `device`, `metric_name`, `path_id`, `labels`, để kiểm tra dữ liệu thực khi soạn rule. | Form query `ipms.tlm_metrics`: chọn device, metric_name, path_id, khoảng thời gian; trả bảng `event_time`, `device_name`, `raw_path`, `metric_name`, `value_*`, `unit`, `labels`. Phân biệt rõ row **raw** vs **derived** (`raw_path="derived"`). | P | Khám Phá Dữ Liệu |
| DE-02 ⭐ | Là operator, tôi muốn xem raw payload trong `tlm_metrics_raw` (TTL 7 ngày), để debug khi metric không như kỳ vọng. | Bảng raw: `receive_time`, raw JSON payload, device. Ghi chú TTL 7 ngày. | N | Khám Phá Dữ Liệu |

---

# PHẦN II — PHASE SAU 🕓 (ngoài 3 HLD, không thuộc prototype lần này)

> Các phần dưới đây **giữ nguyên nội dung** từ v5.1 để tham chiếu, nhưng **hạ ưu tiên** và **không** nằm trong MVP frontend lần này. Sẽ triển khai khi các module tương ứng (collection/gNMIc cluster, inventory, Grafana, observability) được phát triển.

## 6. Bảng Điều Khiển (Dashboard) 🕓
DS-01…DS-07 — tổng quan router, tốc độ thu thập, thẻ trạng thái pipeline, tóm tắt cảnh báo, phân bổ vendor, light/dark, phân tải gNMIc. *(Phase Sau — phụ thuộc lớp collection & nhiều module.)*

## 7. Kiểm Kê Router 🕓
RI-01…RI-07 — thêm/sửa/xóa router, import CSV, refresh NETCONF, trạng thái kết nối gNMIc. *(Phase Sau — thuộc module inventory; ở 3 HLD device chỉ **read-only** qua `GET /flink/devices`.)*

## 8. Quản Lý Profile gNMI 🕓
PM-01…PM-06 — profile telemetry, sensor path/interval, YAML gNMIc, import JSON. *(Phase Sau — thuộc lớp collection.)*

## 9. Liên Kết Profile 🕓
PA-01…PA-05 — gán profile cho router, trạng thái chờ cluster áp dụng, YAML tổng hợp, import CSV. *(Phase Sau.)*

## 10. Phân Công Agent (gNMIc Cluster + Consul) 🕓
AA-01…AA-07 — trạng thái cluster/leader, rebalance, drain, locked targets, debug. *(Phase Sau — thuộc lớp collection cluster mode.)*

## 11. Chi Tiết Router / Drilldown 🕓
RD-01…RD-03 — thông tin thiết bị, sparkline sensor, raw stream preview. *(Phase Sau; tab utilization cũ thay bằng truy vấn derived metric ở DE-01.)*

## 12. Tích Hợp Grafana 🕓
GR-01…GR-06 — cấu hình URL/datasource, deep-link, mapping dashboard, export dashboard pack. *(Phase Sau — phân tích sâu để Grafana đảm nhận.)*

## 13. Cài Đặt Hạ Tầng & Thông Tin Hệ Thống 🕓
ST-* (kết nối ClickHouse/MariaDB/Kafka, Consul locker, TTL policy), SI-01/02 (phiên bản, chẩn đoán thành phần). *(Phase Sau — riêng cấu hình refresh interval của Flink/Engine và TTL liên quan 3 HLD có thể được nhúng trực tiếp trong màn hình FR/DM/AL khi cần.)*

## 14. Nhật Ký Kiểm Tra 🕓
AU-* — audit log thao tác cấu hình. *(Phase Sau; ở MVP, audit cho rule write được phản ánh tối thiểu qua `created_by`/`updated_at` của từng rule table.)*

---

## Phụ Lục A — Bản Đồ Thay Đổi v5.1 → v6.0

| Nhóm cũ (v5.1) | Trạng thái | Ghi chú |
| :-- | :-- | :-- |
| Utilization Binding (UB-01…11) | ✕ Thay thế | "Resource Utilization Service" không còn; thay bằng **Derived Metrics (DM-*)** — Flink Job 2. |
| Utilization Overview (UV-01/02) | ✕ Thay thế | Xem nhanh resource quá ngưỡng nay là **alert rule (AL-*)** + truy vấn derived ở **DE-01**. |
| Alert Type A/B/C + N-consecutive 5-min bucket + Redis (AL cũ) | ◆ Viết lại | Thay bằng `condition_kind` (threshold/pct_change_prev/no_data/abs_delta_prev), sustain_samples + dedup (MariaDB), "previous = 1 sample liền trước". |
| Flink Rule chỉ Label + Filter (FR cũ) | ◆ Mở rộng | Gộp đủ **4 nhóm** Path/Metric/Label Alias + Filter, kèm preview, refresh-status, versioning `updated_at`. |
| Alias Metric (MA-*) riêng | ◆ Hợp nhất | Gộp vào **FR-02/FR-03** (Path/Metric Alias). |
| Notification channel per rule (AL-13 cũ) | ✕ Xóa | Thuộc **NOC PRO**, không thuộc Alert Engine. |
| Dashboard, Kiểm Kê, Profile, Liên Kết, Phân Công Agent, Grafana, Cài Đặt hạ tầng, Thông Tin HT | 🕓 Hạ ưu tiên | Giữ tham chiếu, chuyển **Phase Sau** — ngoài 3 HLD. |

## Phụ Lục B — Bản Đồ Màn Hình MVP

| Màn Hình | Story | Vị trí đề xuất |
| :-- | :-- | :-- |
| Flink Rule (4 nhóm) | FR-* | Menu → Cấu Hình → Flink Rule |
| Derived Metrics | DM-* | Menu → Cấu Hình → Derived Metrics |
| Cảnh Báo | AL-* | Menu → Cảnh Báo |
| Giám Sát Pipeline (Job view) | PL-* | Menu → Giám Sát → Pipeline |
| Khám Phá Dữ Liệu | DE-* | Menu → Dữ Liệu → Khám Phá |

## Phụ Lục C — Story Count

| Nhóm | Prefix | Số Story | Ưu tiên (P/N/C) | Phạm vi |
| :-- | :-- | :-- | :-- | :-- |
| Flink Rule (4 nhóm) | FR | 9 | 8P / 1N | ⭐ MVP (HLD 01) |
| Derived Metrics | DM | 9 | 6P / 3N | ⭐ MVP (HLD 02) |
| Cảnh Báo | AL | 14 (+1 ✕) | 11P / 2N / 1C | ⭐ MVP (HLD 03) |
| Giám Sát Pipeline | PL | 4 | 3P / 1N | ⭐ MVP (hỗ trợ) |
| Khám Phá Dữ Liệu | DE | 2 | 1P / 1N | ⭐ MVP (hỗ trợ) |
| **Tổng MVP** | | **38** | **29P / 8N / 1C** | |
| Phase Sau (DS, RI, PM, PA, AA, RD, GR, ST, SI, AU) | — | (giữ tham chiếu v5.1) | — | 🕓 ngoài prototype |

> **Đính chính v6.3:** Bản v6.2 ghi sai một số subtotal (FR `7P/2N`, DM `5P/4N`, AL `9P/3N`, Tổng `25P/11N`) so với bảng story thực tế. Số liệu trên đã được đếm lại trực tiếp từ cột "Ưu Tiên" của từng story; AL-15 (mới, Could Have) đã được tính vào.

> **Ghi chú:** Toàn bộ rule write trong MVP tuân hợp đồng chung của HLD: chạy trong một transaction, đẩy `updated_at` (version), reset `pushed_at=NULL`, hỗ trợ `If-Match`/`If-Unmodified-Since` chống ghi đè đồng thời, mã lỗi `200/201/204/400/401/403/404/409/422/500`, và thay đổi có hiệu lực sau ~1 chu kỳ refresh của Flink/Engine.
