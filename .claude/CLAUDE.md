# CLAUDE.md — ipms-fe (TMS Dashboard)
# Đồng bộ: user-stories v6.3 | HLD Job1 v2.4 | HLD Job2 v2.7 | HLD Job3 v1.4

---

## 1. Build & Dev Commands

```bash
npm start        # dev server → http://localhost:3000 (--max_old_space_size=10192)
npm run build    # production build
npm test         # jest
```

> **Node:** Dùng Node 16. Nếu Node 18+ thêm `--openssl-legacy-provider` vào scripts.
> **node-sass v6** là native binding — cần build tools, chạy tốt trên Linux/WSL2.

---

## 2. Tech Stack — KHÔNG thay thế bằng thư viện khác

| Concern        | Library                          | Ghi chú quan trọng                              |
|----------------|----------------------------------|--------------------------------------------------|
| UI components  | `primereact@6.6`                 | DataTable, Dialog, Dropdown, Toast, TabView, Tag |
| Layout         | `primeflex@3`                    | class `p-col-*`, `p-grid`, `p-d-flex`            |
| Icons          | `primeicons@4`                   | class `pi pi-*`                                  |
| Charts         | `recharts@2`                     | LineChart, BarChart, ResponsiveContainer         |
| Forms          | `react-final-form` + `final-form`| Field, Form, validate — KHÔNG dùng react-hook-form |
| State global   | `redux@4` + `react-redux@7`      | KHÔNG dùng Redux Toolkit hay Zustand             |
| Routing        | `react-router-dom@5`             | `<Switch>/<Route>` — KHÔNG dùng v6 `<Routes>`   |
| HTTP           | `axios@0.21`                     | qua redux-axios-middleware                       |
| Date/time      | `moment@2` + `moment-timezone`   | KHÔNG dùng dayjs, date-fns, luxon               |
| Styles         | `node-sass@6` (SCSS)             | File `.scss` per component — KHÔNG dùng CSS-in-JS|
| i18n           | `react-i18next`                  | `useTranslation` hook, keys trong `public/locales/` |

---

## 3. Phạm vi MVP — 5 màn hình (user-stories v6.3)

> Tổng: **38 stories** (29P / 8N / 1C). Tất cả dùng **mock data** — không gọi API thật.
> Mock data đặt trong `src/mock/`. Cấu trúc mock phải khớp schema API bên dưới.

| Route                  | Màn hình                    | Story group | Stories     |
|------------------------|-----------------------------|-------------|-------------|
| `/flink-rules`         | Flink Rule (4 nhóm tab)     | FR-01…09    | 8P / 1N     |
| `/derived-metrics`     | Derived Metrics             | DM-01…09    | 6P / 3N     |
| `/alert-rules`         | Alert Rules + State/History | AL-01…15    | 11P / 2N / 1C |
| `/pipeline-monitor`    | Pipeline Monitor            | PL-01…04    | 3P / 1N     |
| `/data-explorer`       | Data Explorer               | DE-01…02    | 1P / 1N     |

### Không làm trong prototype này (Phase Sau)
- Dashboard tổng quan (DS-*)
- Router inventory, gNMI profile, gNMIc cluster (RI-*, PM-*, PA-*, AA-*)
- Grafana integration (GR-*)
- Notification / escalation / silence (thuộc NOC PRO)

---

## 4. Data Model & Enum Values — PHẢI dùng đúng, không tự ý đặt tên khác

### 4.1 Enum chung

```js
// ⚠️  QUAN TRỌNG — vendor_code vs model_code:
//   - Tên field trong DB/API: "vendor_code"  (giữ nguyên, không đổi BE)
//   - Tên hiển thị trong UI:  "Model Code"   (label cột, form, tooltip)
//   - Lý do: trường này thực chất phản ánh DATA MODEL (Cisco model, OpenConfig model...),
//     không phải vendor của thiết bị. Đã lỡ đặt tên vendor_code từ đầu nên BE giữ nguyên,
//     nhưng FE PHẢI hiển thị là "Model Code" để operator hiểu đúng ngữ nghĩa.
//
// Quy tắc áp dụng mọi nơi trong FE:
//   - Tiêu đề cột DataTable:    "Model Code"
//   - Label form field:         "Model Code"
//   - Tooltip/placeholder:      "Model Code (data model của path)"
//   - Filter dropdown label:    "Model Code"
//   - KHÔNG bao giờ hiển thị:  "Vendor Code" hay "vendor_code" ra UI
//   - API call vẫn gửi field:   vendor_code  (tên key JSON giữ nguyên)

MODEL_CODE_VALUES = ['Cisco', 'Juniper', 'Nokia', 'OpenConfig', 'All']
// Cisco     = data model Cisco IOS-XR (path bắt đầu Cisco-IOS-XR-*)
// Juniper   = data model Junos
// Nokia     = data model Nokia SR OS
// OpenConfig = data model vendor-neutral; áp khi path bắt đầu openconfig-,
//              match bất kể vendor thiết bị là Cisco/Juniper/Nokia
// All       = fallback, áp mọi vendor
//
// Thứ tự ưu tiên Flink resolve khi nhiều rule match:
//   vendor-specific (Cisco/Juniper/Nokia) → OpenConfig → All

// status — mọi bảng rule
STATUS = { 0: 'Inactive/Deprecated', 1: 'Active' }

// value_type
VALUE_TYPE = { 1: 'number', 2: 'string', 3: 'bool' }
```

### 4.2 Flink Rule (FR) — MariaDB tables

**Path Alias** (`tlm_path_aliases`):
```js
{ id,
  vendor_code,   // ← API field name; hiển thị UI là "Model Code"
  original_path, alias_path, status, priority,
  pushed_at, created_by, created_at, updated_at }
// alias_path KHÔNG sửa được sau khi Active — phải Deprecate + tạo mới
```

**Metric Alias** (`tlm_metric_aliases`):
```js
{ id,
  vendor_code,          // ← API field; UI label = "Model Code"
  path_alias_id,        // null=any-path
  original_name, alias_metric,
  value_type_override,  // null|1|2|3
  source_unit, target_unit,
  transform_kind,       // 0=identity,1=linear(scale*x+offset),2=expression,3=enum_mapping
  scale_factor, offset_value, transform_expression, enum_mapping/*JSON*/,
  priority, status, pushed_at, created_by, created_at, updated_at }
// priority: path-specific=100, any-path=50, OpenConfig=30, All=10
```

**Label Alias** (`tlm_label_aliases`):
```js
{ id,
  vendor_code,   // ← API field; UI label = "Model Code"
  path_alias_id, original_key, alias_key,
  lv_kind,       // 0=identity,1=regex,2=enum_mapping
  lv_pattern, lv_replace, lv_mapping/*JSON*/,
  priority, status, pushed_at, created_by, created_at, updated_at }
```

**Filter Rule** (`tlm_filter_rules`):
```js
{ id,
  vendor_code,       // ← API field; UI label = "Model Code"
  match_path,        // rỗng=mọi path
  match_metric,      // null=mọi metric trong path
  filter_expression, // rỗng=luôn TRUE
  filter_action,     // 0=EXCLUDE_IF_MATCH(drop), 1=INCLUDE_IF_MATCH(chỉ giữ)
  priority, status, pushed_at, created_by, created_at, updated_at }
// CẢNH BÁO: Filter drop metric → KHÔNG ghi ClickHouse, KHÔNG phục hồi được
```

### 4.3 Derived Metrics (DM) — `tlm_derived_rules`

```js
{ id, rule_name, output_metric/*unique trong active*/, output_unit,
  derive_kind,            // 0=computed, 1=aggregated, 2=delta  (KHÔNG có 3)
  input_metrics,          // JSON array of string (alias_metric) — KHÔNG phải object
  expression,             // chỉ derive_kind=0; null cho 1,2
  assembly_window_seconds,// chỉ derive_kind=0; null cho 1,2
  window_seconds,         // chỉ derive_kind=1
  agg_function,           // 0=avg,1=max,2=min,3=sum,4=rate — chỉ derive_kind=1
  delta_scale_factor,     // derive_kind=2; default=1.0
  delta_reset_threshold,  // derive_kind=2; null=no reset detection
  static_constants,       // JSON Map<alias_metric,Double> — chỉ derive_kind=0
  scope_path_alias_id,    // null=mọi path; NOT NULL=khoá rule + input matching
  scope_device_ids,       // JSON array device_id; null=mọi thiết bị; tối đa 20 phần tử
  priority, status, created_by, created_at, updated_at }
// KHÔNG có pushed_at — tlm_derived_rules không có ClickHouse mirror;
//   Flink Job 2 đọc thẳng từ MariaDB, versioning chỉ qua updated_at
// scope_device_ids giới hạn <= 20 device (validate tại API, trả 422 nếu vượt)
//   UI: hiển thị counter "X/20 thiết bị", hint "Để trống = áp tất cả thiết bị"
// output_metric KHÔNG được trùng bất kỳ alias_metric nào trong tlm_metric_aliases
// Dual sink: ClickHouse (raw_path="derived") + Kafka topic derived_metrics
```

### 4.4 Alert Rules (AL) — `tlm_alert_rules`

```js
{ id, rule_name,
  rule_kind,      // 0=basic, 1=composite
  condition_kind, // chỉ basic: 0=threshold,1=pct_change_prev,2=no_data,3=abs_delta_prev
  severity,       // 0=info,1=warning,2=error,3=critical  ← thứ tự v6.2+, critical=3 nặng nhất
  status,         // 0=Disabled,1=Active
  alias_metric,   // basic: metric đánh giá; null cho composite
  path_alias_id,  // null=any-path; 0=derived metric (từ Job2)
  entity_keys,    // JSON array string; []=device-level
  entity_filter,  // JSON object; null=mọi entity; key⊆entity_keys
  scope_device_ids,
  // Trường condition (basic — tuỳ condition_kind):
  comparator,     // 0=>,1=>=,2=<,3=<=,4===,5=!= ; PHẢI VẮNG nếu condition_kind=2
  threshold,      // PHẢI VẮNG nếu condition_kind=2
  pct_abs,        // 0|1; dùng cho condition_kind=1,3
  no_data_seconds,// >0; CHỈ khi condition_kind=2; phải VẮNG cho loại khác
  // Trường composite (rule_kind=1):
  logical_op,     // 0=AND,1=OR
  child_rule_ids, // JSON array int; child phải là basic, Active, cùng entity_keys
  missing_as,     // 0=false/skip,1=true
  // Chung:
  sustain_samples,// >=1; số lần vi phạm liên tiếp trước khi FIRE
  dedup_seconds,  // >=0; thời gian chống REFIRE
  emit_independent,// 0|1
  updated_at, created_by, created_at }

// no_data semantics (v1.4): mỗi đợt im lặng FIRED đúng 1 lần, KHÔNG refire chu kỳ
// RESOLVED khi series báo lại (resolve-on-return) hoặc aging-RESOLVED
// AL-15 (C — Could Have): tuỳ chọn nhắc lại định kỳ no_data, mặc định TẮT
```

**Alert State** (`tlm_alert_state` — mirror, chỉ Job3 ghi):
```js
{ rule_id, device_id, entity_fingerprint, last_state,/*FIRING|RESOLVED*/
  last_fired_at, last_resolved_at, dedup_remaining_seconds }
```

**Alert History** (`ipms.alert_history` ClickHouse — TTL 90 ngày):
```js
{ timestamp, rule_id, rule_name, severity, device_id, device_name,
  entity_fingerprint, trigger_value, event_type /*FIRED|REFIRED|RESOLVED*/ }
```

### 4.5 Pipeline Monitor (PL)

```js
// Flink jobs
{ job_name, status,/*Running|Failed|Stopped*/ records_per_sec, uptime_seconds,
  active_rules_count, last_checkpoint_at, consumer_lag, watermark_lag_ms,
  eval_latency_ms }

// Kafka topics: telemetry.raw, processed_metrics, derived_metrics, alerts
{ topic, consumer_lag, messages_per_sec, lag_threshold_warning }

// ClickHouse sinks: tlm_metrics, tlm_metrics_raw, alert_history
{ table, inserts_per_sec, batch_size, query_latency_p50_ms, query_latency_p99_ms,
  disk_usage_gb, error_rate, sink_delay_ms }
```

### 4.6 Data Explorer (DE) — ClickHouse `ipms.tlm_metrics`

```js
{ event_time, device_name, raw_path,/*"derived" nếu từ Job2*/ metric_name,
  value_number, value_string, value_bool, value_type, unit, labels/*JSON*/ }
// raw_path="derived" → phân biệt rõ với raw telemetry
```

---

## 5. API Endpoints — dùng làm chuẩn cho mock data structure

> Base path: `/api/v1`
> Convention: `200/201/204/400/401/403/404/409/422/500`
> Versioning: `If-Match: <updated_at>` → `409` nếu mismatch (chống ghi đè đồng thời)
> Mọi write: đẩy `updated_at` tiến + reset `pushed_at=NULL`
> Soft-delete: `DELETE` = set `status=0`, không xoá vật lý

### Flink Rules (`/flink`)
```
GET    /flink/path-aliases          FR-01  lọc: vendor,status,q; page,page_size(max200),sort=-updated_at
POST   /flink/path-aliases          FR-02
PUT    /flink/path-aliases/{id}     FR-02
DELETE /flink/path-aliases/{id}     FR-09  409 nếu có child active; ?force=true cascade
GET    /flink/metric-aliases        FR-03
POST   /flink/metric-aliases        FR-03
PUT    /flink/metric-aliases/{id}   FR-03
DELETE /flink/metric-aliases/{id}   FR-09
GET    /flink/label-aliases         FR-04
POST   /flink/label-aliases         FR-04
PUT    /flink/label-aliases/{id}    FR-04
DELETE /flink/label-aliases/{id}    FR-09
GET    /flink/filter-rules          FR-05
POST   /flink/filter-rules          FR-05
PUT    /flink/filter-rules/{id}     FR-05
DELETE /flink/filter-rules/{id}     FR-09
POST   /flink/{group}/preview       FR-06  group=path-aliases|metric-aliases|label-aliases|filter-rules
GET    /flink/refresh-status        FR-07  trả max_updated_at,rows_pending_push per table
GET    /flink/devices               FR-01 (device list — read-only trong MVP)
GET    /flink/metric-aliases?alias_metric=... DM-07 (lookup alias_metric across paths)
```

### Derived Metrics (`/derived`)
```
GET    /derived/rules               DM-01  lọc: derive_kind,status,q(output_metric)
POST   /derived/rules               DM-02/03/04
PUT    /derived/rules/{id}          DM-09
PATCH  /derived/rules/{id}          DM-09  {status:0}/{priority:120}
DELETE /derived/rules/{id}          DM-09  soft-delete; KHÔNG reset pushed_at (không có)
POST   /derived/rules/preview       DM-06  sample_inputs trả result,unit,warnings,errors
GET    /derived/observability       DM-08  per-rule derived_incomplete_total, emit counter
// KHÔNG có GET /derived/refresh-status — đã bỏ pushed_at, không còn rows_pending_push
// Validation scope_device_ids: len > 20 → 422 "Tối đa 20 thiết bị cho mỗi rule"
```

### Alert Rules (`/alerts`)
```
GET    /alerts/rules                AL-01  lọc: severity,status,rule_kind,condition_kind,alias_metric,q
GET    /alerts/rules/{id}           AL-01  composite có thêm children[] expand
POST   /alerts/rules               AL-02~06  basic mọi condition_kind + composite
PUT    /alerts/rules/{id}          AL-02~06
PATCH  /alerts/rules/{id}          AL-08  {status:0} hoặc {threshold:90}
DELETE /alerts/rules/{id}          AL-12  409 nếu composite Active tham chiếu; ?force=true cascade
POST   /alerts/rules/preview       AL-07  dry-run trên ClickHouse, không lưu
GET    /alerts/rules/{id}/state    AL-10  per-instance firing state từ tlm_alert_state
GET    /alerts/active              AL-09  danh sách đang FIRING
GET    /alerts/history             AL-11  FIRED/REFIRED/RESOLVED; mặc định 24h; TTL 90 ngày
GET    /alerts/engine/status       AL-13  Flink Job3 health: status,checkpoint,lag,latency,eval_latency_ms
GET    /alerts/refresh-status                hỗ trợ vận hành
```

---

## 6. Mock Data Guidelines

```js
// Thiết bị mẫu (dùng nhất quán)
const DEVICES = [
  { id: 1, name: 'R-HCM-001', vendor: 'Cisco',   model: 'ASR9001', ip: '10.1.1.1' },
  { id: 2, name: 'R-HCM-002', vendor: 'Cisco',   model: 'NCS5500', ip: '10.1.1.2' },
  { id: 3, name: 'R-HN-001',  vendor: 'Juniper', model: 'MX480',   ip: '10.2.1.1' },
  { id: 4, name: 'R-HN-002',  vendor: 'Juniper', model: 'PTX5000', ip: '10.2.1.2' },
  { id: 5, name: 'R-DN-001',  vendor: 'Nokia',   model: 'SR7750',  ip: '10.3.1.1' },
]

// YANG path mẫu (Path Alias)
const YANG_PATHS = {
  cisco:   'Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters',
  juniper: 'interfaces/interface/state/counters',
  nokia:   'state/port',
  openconfig: 'openconfig-interfaces:interfaces/interface/state/counters',
}

// Metric alias mẫu
const METRIC_ALIASES = [
  'if_in_octets', 'if_out_octets', 'if_in_errors', 'if_out_errors',
  'if_in_discards', 'if_out_discards', 'if_in_ucast_pkts', 'if_out_ucast_pkts',
  'cpu_utilization_5min', 'memory_used_bytes', 'memory_free_bytes',
  'bgp_prefixes_received', 'bgp_prefixes_sent', 'bgp_state',
]

// Tạo đủ để test
// - Path Alias: >=12 rows, mỗi vendor >=2
// - Metric Alias: >=15 rows, mix path-scoped và any-path
// - Label Alias: >=10 rows, mix regex và enum_mapping
// - Filter Rules: >=5 rows, mix EXCLUDE và INCLUDE
// - Derived Rules: >=8 rows, mix 3 loại (computed/aggregated/delta)
// - Alert Rules: >=10 basic + 3 composite, mix severity và condition_kind
// - Alert State: >=5 FIRING, 3 RESOLVED
// - Alert History: >=20 events, 24h gần nhất, mix FIRED/REFIRED/RESOLVED
// - Pipeline: 3 Flink jobs (Job1/2/3), 4 Kafka topics, 3 ClickHouse tables
```

---

## 7. UI Design Pattern (theo phong cách IPMS 4.0)

Xem ảnh tham chiếu: `docs/ipms4-screenshot.png`

### Màu sắc

```js
// Màu nền & text chung
BACKGROUND_PAGE    = '#f0f2f5'   // nền tổng thể (xám rất nhạt)
BACKGROUND_WHITE   = '#ffffff'   // sidebar, topbar, card, table
TEXT_PRIMARY       = '#262626'   // text chính trong bảng, form
TEXT_SECONDARY     = '#595959'   // label menu sidebar
TEXT_MUTED         = '#8c8c8c'   // breadcrumb, placeholder
BORDER_COLOR       = '#f0f0f0'   // border bảng, divider
TABLE_HEADER_BG    = '#fafafa'   // nền header cột DataTable
TABLE_ROW_HOVER    = '#e6f7ff'   // hover row

// Màu primary / action
PRIMARY            = '#1890ff'   // link trong bảng, icon edit, button Tìm kiếm, active menu
SIDEBAR_ACTIVE_BG  = '#e6f7ff'   // background item menu đang active

// Action buttons (góc phải header)
BTN_SEARCH         = '#1890ff'   // Tìm kiếm   — icon: pi-search
BTN_ADD            = '#52c41a'   // Thêm mới   — icon: pi-plus
BTN_RESCAN         = '#722ed1'   // Rescan      — icon: pi-refresh
BTN_EXPORT         = '#fa8c16'   // Xuất file  — icon: pi-cloud-download
BTN_IMPORT         = '#1890ff'   // Nhập        — icon: pi-cloud-upload

// Icon hành động trong bảng
ICON_EDIT          = '#52c41a'   // pi-pencil  — màu xanh lá
ICON_DELETE        = '#ff4d4f'   // pi-trash   — màu đỏ

// Severity badge
SEVERITY_INFO      = '#1890ff'   // Info     (0)
SEVERITY_WARNING   = '#faad14'   // Warning  (1)
SEVERITY_ERROR     = '#ff7a45'   // Error    (2)
SEVERITY_CRITICAL  = '#ff4d4f'   // Critical (3)
```

### Font chữ

```js
FONT_FAMILY   = 'inherit'   // dùng font mặc định PrimeReact (system-ui / sans-serif)
FONT_SIZE_TABLE     = '13px'   // text trong DataTable rows
FONT_SIZE_HEADER    = '13px'   // header cột (chữ đậm)
FONT_SIZE_PAGE_TITLE = '18px'  // tiêu đề màn hình (vd "THIẾT BỊ"), font-weight: 600
FONT_SIZE_MENU      = '13px'   // item sidebar
FONT_SIZE_BUTTON    = '13px'   // label button action
```

### Icons (PrimeIcons — class `pi`)

```js
// Action buttons góc phải
'pi-search'         // Tìm kiếm
'pi-plus'           // Thêm mới
'pi-refresh'        // Rescan
'pi-cloud-download' // Xuất file
'pi-cloud-upload'   // Nhập

// Hành động trong bảng (icon-only, không label)
'pi-pencil'         // Edit
'pi-trash'          // Delete

// Sidebar menu
'pi-home'           // Trang chủ
'pi-sitemap'        // Topology
'pi-desktop'        // Giám sát
'pi-list'           // Danh mục
'pi-cog'            // Admin / settings

// Bảng
'pi-chevron-right'  // Row expand
'pi-sort-alt'       // Sortable column (↑↓)
'pi-filter'         // Filter row toggle
'pi-sliders-h'      // Column settings (gear đầu bảng)
```

### DataTable pattern (PrimeReact DataTable v6)
- `filterDisplay="row"` — filter inline dưới header
- Cột 1: checkbox selection
- Cột 2: STT (số thứ tự)
- Cột link (tên thiết bị, alias...): màu `#1890ff`, cursor pointer
- Cột cuối "Hành động": icon `pi-pencil` (`#52c41a`) + `pi-trash` (`#ff4d4f`), không có label
- `sortable` mặc định, `paginator`, `rows={20}`
- Pagination text: "Hiển thị X đến Y trên tổng số Z bản ghi"

### Flink Rule — dùng TabView 4 tab
```
[Path Alias] [Metric Alias] [Label Alias] [Filter Rule]
```

### Alert Rules — dùng TabView 3 tab
```
[Danh sách Rule] [Active Alerts (AL-09)] [Alert History (AL-11)]
```

---

## 8. Code Style

- **Ngôn ngữ**: JavaScript (KHÔNG dùng TypeScript)
- **Components**: functional + hooks (`useState`, `useEffect`, `useCallback`, `useMemo`)
- **KHÔNG dùng** class components
- **State global**: Redux actions/reducers — pattern hiện tại của project (spread operator, không Immer)
- **SCSS**: file `.scss` riêng per component, import vào component
- **i18n**: `useTranslation` hook, key dạng `namespace:key`

---

## 9. Common Pitfalls

- **`vendor_code` ≠ "Vendor Code" trong UI** — field DB/API tên là `vendor_code` nhưng UI phải
  hiển thị là **"Model Code"** ở mọi nơi (tiêu đề cột, form label, filter dropdown, tooltip).
  Tên key JSON gửi lên API vẫn là `vendor_code` — chỉ đổi phần hiển thị, không đổi API contract.
  Không bao giờ hiển thị chuỗi "vendor_code" hay "Vendor Code" ra màn hình người dùng.
- `node-sass@6` không build trên Node 18+ nếu thiếu flag `--openssl-legacy-provider`
- `primereact@6` DataTable: không có `pt` prop (đó là v7+); dùng `className` và `style`
- `react-router-dom@5`: `<Switch>` không phải `<Routes>`; `useHistory` không phải `useNavigate`
- `axios@0.21`: response là `res.data`, không phải `res.json()`; error handling qua interceptors
- Redux không có Immer — reducers **phải return new object** (dùng spread `{...state, field: value}`)
- `derive_kind` chỉ có `{0,1,2}` — KHÔNG có giá trị `3` (Composite interval-join đã bị xóa từ v2.3)
- `severity` order: `0=info < 1=warning < 2=error < 3=critical` — critical là CAO nhất
- `path_alias_id=0` cho alert rule trên **derived metric** (từ Job2), khác với `null` (any-path)
- `no_data` alert: KHÔNG refire chu kỳ theo mặc định — chỉ FIRED 1 lần/đợt im lặng (v1.4)
- `input_metrics` trong derived rule là **array of string** (`alias_metric`), KHÔNG phải array of object
- `alias_path` của Path Alias KHÔNG sửa được sau Active — muốn đổi phải Deprecate + tạo mới
- Filter Rule drop metric là **KHÔNG THỂ PHỤC HỒI** — cần cảnh báo nổi bật trong UI
