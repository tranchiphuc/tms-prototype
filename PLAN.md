# TMS Dashboard — Implementation Plan
# Đồng bộ: user-stories v6.3 | HLD Job1 v2.4 | HLD Job2 v2.8 | HLD Job3 v1.4

---

## Tổng quan

Xây dựng prototype frontend TMS Dashboard (ipms-fe) gồm **5 màn hình**, **38 user story** (29P / 8N / 1C), toàn bộ dùng **mock data** — không gọi API thật.

---

## 1. Trạng thái hiện tại

- Thư mục `/docs` chứa toàn bộ tài liệu thiết kế (HLD, user stories, schema, package.json mẫu)
- **Chưa có code nào** — cần scaffolding từ đầu
- `docs/package.json` là template dependency cần dùng

---

## 2. Tech Stack (KHÔNG thay đổi)

| Concern | Library | Version |
|---|---|---|
| UI | primereact | 6.6 |
| Layout | primeflex | 3 |
| Icons | primeicons | 4 |
| Charts | recharts | 2 |
| Forms | react-final-form + final-form | latest |
| State | redux@4 + react-redux@7 | — |
| Routing | react-router-dom@5 | — |
| HTTP | axios@0.21 | — |
| Date | moment@2 + moment-timezone | — |
| Styles | node-sass@6 (SCSS) | — |
| i18n | react-i18next | — |

---

## 3. Cấu trúc thư mục

```
tms-prototype/
├── public/
│   ├── index.html
│   └── locales/
│       └── vi/
│           ├── flink.json
│           ├── derived.json
│           ├── alert.json
│           ├── pipeline.json
│           ├── explorer.json
│           └── common.json
├── src/
│   ├── index.js
│   ├── App.js
│   ├── App.scss
│   ├── styles/
│   │   ├── _variables.scss      # design tokens (màu, font)
│   │   └── _common.scss         # DataTable, button pattern
│   ├── mock/
│   │   ├── devices.js           # 5 thiết bị mẫu
│   │   ├── pathAliases.js       # >=12 rows
│   │   ├── metricAliases.js     # >=15 rows
│   │   ├── labelAliases.js      # >=10 rows
│   │   ├── filterRules.js       # >=5 rows
│   │   ├── derivedRules.js      # >=8 rows
│   │   ├── alertRules.js        # >=13 rules (10 basic + 3 composite)
│   │   ├── alertState.js        # >=5 FIRING + 3 RESOLVED
│   │   ├── alertHistory.js      # >=20 events trong 24h
│   │   └── pipeline.js          # 3 Flink jobs, 4 Kafka topics, 3 CH tables
│   ├── services/
│   │   └── mockApi.js           # axios instance + mock interceptor (trả mock data)
│   ├── redux/
│   │   ├── store.js
│   │   ├── actions/
│   │   │   ├── flinkActions.js
│   │   │   ├── derivedActions.js
│   │   │   ├── alertActions.js
│   │   │   ├── pipelineActions.js
│   │   │   └── explorerActions.js
│   │   └── reducers/
│   │       ├── index.js          # combineReducers
│   │       ├── flinkReducer.js
│   │       ├── derivedReducer.js
│   │       ├── alertReducer.js
│   │       ├── pipelineReducer.js
│   │       └── explorerReducer.js
│   ├── layout/
│   │   ├── AppLayout.js          # wrapper: Sidebar + Topbar + main content
│   │   ├── AppLayout.scss
│   │   ├── AppSidebar.js         # menu điều hướng
│   │   ├── AppSidebar.scss
│   │   └── AppTopbar.js          # breadcrumb + user info
│   ├── components/
│   │   ├── StatusBadge.js        # Active/Inactive tag
│   │   ├── SeverityBadge.js      # Info/Warning/Error/Critical với màu
│   │   ├── ConfirmDialog.js      # wrapper PrimeReact ConfirmDialog
│   │   ├── ActionButtons.js      # Search/Add/Export group button góc phải
│   │   └── RefreshStatusPanel.js # panel hiển thị refresh-status
│   └── pages/
│       ├── flink-rules/
│       │   ├── FlinkRulesPage.js       # TabView 4 tab
│       │   ├── FlinkRulesPage.scss
│       │   ├── tabs/
│       │   │   ├── PathAliasTab.js     # FR-01, FR-02, FR-09
│       │   │   ├── MetricAliasTab.js   # FR-03, FR-09
│       │   │   ├── LabelAliasTab.js    # FR-04, FR-09
│       │   │   └── FilterRuleTab.js    # FR-05, FR-09
│       │   ├── dialogs/
│       │   │   ├── PathAliasDialog.js
│       │   │   ├── MetricAliasDialog.js
│       │   │   ├── LabelAliasDialog.js
│       │   │   └── FilterRuleDialog.js
│       │   ├── preview/
│       │   │   └── RulePreviewDialog.js # FR-06
│       │   └── FallthroughMonitor.js    # FR-08
│       ├── derived-metrics/
│       │   ├── DerivedMetricsPage.js    # list + CRUD
│       │   ├── DerivedMetricsPage.scss
│       │   ├── DerivedRuleDialog.js     # form wizard: chọn kind → fields
│       │   ├── tabs/
│       │   │   ├── ComputedForm.js      # DM-02
│       │   │   ├── AggregatedForm.js    # DM-03
│       │   │   └── DeltaForm.js         # DM-04
│       │   ├── ScopeSelector.js         # DM-05 (device_ids + path_alias_id)
│       │   ├── DerivedPreviewDialog.js  # DM-06
│       │   ├── MetricAliasLookup.js     # DM-07
│       │   └── ObservabilityPanel.js    # DM-08
│       ├── alert-rules/
│       │   ├── AlertRulesPage.js        # TabView 3 tab
│       │   ├── AlertRulesPage.scss
│       │   ├── tabs/
│       │   │   ├── RuleListTab.js       # AL-01
│       │   │   ├── ActiveAlertsTab.js   # AL-09
│       │   │   └── AlertHistoryTab.js   # AL-11
│       │   ├── dialogs/
│       │   │   ├── BasicRuleDialog.js   # AL-02/03/04/05
│       │   │   ├── CompositeRuleDialog.js # AL-06
│       │   │   └── AlertPreviewDialog.js  # AL-07
│       │   ├── AlertStatePanel.js       # AL-10
│       │   └── EngineStatusWidget.js    # AL-13
│       ├── pipeline-monitor/
│       │   ├── PipelineMonitorPage.js   # PL-01/02/03/04
│       │   ├── PipelineMonitorPage.scss
│       │   ├── FlinkJobsPanel.js        # PL-01
│       │   ├── KafkaTopicsPanel.js      # PL-02
│       │   ├── ClickHouseSinkPanel.js   # PL-03
│       │   └── LatencyPanel.js          # PL-04
│       └── data-explorer/
│           ├── DataExplorerPage.js      # DE-01/DE-02
│           ├── DataExplorerPage.scss
│           ├── MetricsQueryForm.js      # DE-01
│           └── RawPayloadTable.js       # DE-02
├── .env
├── package.json
└── PLAN.md
```

---

## 4. Design System (theo IPMS 4.0)

### 4.1 Màu sắc (định nghĩa trong `_variables.scss`)

```scss
// Nền & text
$bg-page:          #f0f2f5;
$bg-white:         #ffffff;
$text-primary:     #262626;
$text-secondary:   #595959;
$text-muted:       #8c8c8c;
$border-color:     #f0f0f0;
$table-header-bg:  #fafafa;
$table-row-hover:  #e6f7ff;

// Primary
$primary:          #1890ff;
$sidebar-active-bg: #e6f7ff;

// Action buttons
$btn-search:  #1890ff;
$btn-add:     #52c41a;
$btn-rescan:  #722ed1;
$btn-export:  #fa8c16;

// Table icons
$icon-edit:   #52c41a;  // pi-pencil
$icon-delete: #ff4d4f;  // pi-trash

// Severity
$severity-info:     #1890ff;
$severity-warning:  #faad14;
$severity-error:    #ff7a45;
$severity-critical: #ff4d4f;
```

### 4.2 DataTable pattern

- `filterDisplay="row"` — filter inline
- Cột 1: checkbox selection
- Cột 2: STT (số thứ tự, tính từ paginator offset)
- `sortable` mặc định, `paginator`, `rows={20}`
- Cột cuối "Hành động": `pi-pencil` (xanh lá) + `pi-trash` (đỏ), no label
- vendor_code luôn hiển thị là **"Model Code"** (không bao giờ "Vendor Code")

---

## 5. Mock Data Layer

File `services/mockApi.js`: axios instance với request interceptor, bắt mọi URL theo pattern và trả mock data có delay giả lập (100–300ms).

### Phương pháp:
1. `mockApi.js` export một axios instance với interceptor tùy chỉnh
2. Interceptor match URL regex → gọi handler tương ứng → trả `{ data: ... }` sau timeout
3. Hỗ trợ CRUD: GET list, GET by id, POST, PUT, PATCH, DELETE (soft-delete status=0)
4. Simulate lỗi: 409 conflict, 422 validation, khi cần

### Mock data constraints (từ CLAUDE.md §6):
- Path Alias: >=12 rows, mỗi vendor >=2 (Cisco/Juniper/Nokia/OpenConfig/All)
- Metric Alias: >=15 rows, mix path-scoped và any-path
- Label Alias: >=10 rows, mix lv_kind 0/1/2
- Filter Rules: >=5 rows, mix filter_action 0/1
- Derived Rules: >=8 rows, mix derive_kind 0/1/2
- Alert Rules: >=10 basic (mix 4 condition_kind) + 3 composite, mix severity
- Alert State: >=5 FIRING, 3 RESOLVED
- Alert History: >=20 events, 24h gần nhất, mix FIRED/REFIRED/RESOLVED
- Pipeline: 3 Flink jobs (Job1/2/3), 4 Kafka topics, 3 ClickHouse tables

---

## 6. Redux State Shape

```js
{
  flink: {
    pathAliases: { list: [], loading: false, total: 0 },
    metricAliases: { list: [], loading: false, total: 0 },
    labelAliases: { list: [], loading: false, total: 0 },
    filterRules: { list: [], loading: false, total: 0 },
    refreshStatus: null,
    devices: [],
  },
  derived: {
    rules: { list: [], loading: false, total: 0 },
    observability: null,
  },
  alert: {
    rules: { list: [], loading: false, total: 0 },
    activeAlerts: [],
    history: { list: [], loading: false },
    engineStatus: null,
  },
  pipeline: {
    flinkJobs: [],
    kafkaTopics: [],
    clickhouseSinks: [],
    loading: false,
  },
  explorer: {
    queryResult: [],
    rawPayload: [],
    loading: false,
  },
}
```

---

## 7. Routing (react-router-dom v5)

```jsx
<Switch>
  <Route exact path="/" component={RedirectToFlink} />
  <Route path="/flink-rules" component={FlinkRulesPage} />
  <Route path="/derived-metrics" component={DerivedMetricsPage} />
  <Route path="/alert-rules" component={AlertRulesPage} />
  <Route path="/pipeline-monitor" component={PipelineMonitorPage} />
  <Route path="/data-explorer" component={DataExplorerPage} />
</Switch>
```

---

## 8. Kế hoạch triển khai theo phase

### Phase 0 — Scaffold & Infrastructure (Prerequisite) ✅ DONE (2026-06-06)
**Mục tiêu:** Project chạy được, layout hiển thị, routing hoạt động.

> **Toolchain:** chạy trên **Node 16** qua nvm (`nvm use 16`) — máy mặc định Node 24 không build được node-sass@6. Verified: `npm install`, `npm run build` (Compiled successfully), `npm start` (serve localhost:3000 OK).

- [x] 0.1 Tạo project React (package.json tập trung stack MVP, pin version khớp docs/)
- [x] 0.2 Cài đặt dependencies (node-sass@6 build OK trên Node 16)
- [x] 0.3 Setup `src/styles/_variables.scss` + `_common.scss`
- [x] 0.4 Tạo `AppLayout` (Sidebar + Topbar + content area)
- [x] 0.5 Setup react-router-dom routing cơ bản (5 route placeholder)
- [x] 0.6 Setup Redux store + combineReducers (redux-axios-middleware + mock adapter)
- [x] 0.7 Setup i18n (react-i18next) với locale vi/ (6 namespace)
- [x] 0.8 Tạo `services/mockApi.js` (custom axios adapter + registerHandler)
- [x] 0.9 Tạo shared components: StatusBadge, SeverityBadge, ActionButtons

### Phase 1 — Flink Rules (FR-01 đến FR-09) ✅ DONE (2026-06-06)
**Stories P:** FR-01, FR-02, FR-03, FR-04, FR-05, FR-06, FR-07, FR-09
**Story N:** FR-08

> Build + dev server verified clean. Shared infra: RuleListShell, FilterBar, FormFields, ToastProvider, useRuleTab.

- [x] 1.1 Mock data: `pathAliases.js`(14), `metricAliases.js`(17), `labelAliases.js`(11), `filterRules.js`(6), `devices.js` + `helpers.js`
- [x] 1.2 Mock API handlers cho `/flink/*` (generic CRUD factory + validation + cascade + preview + refresh-status + fallthrough)
- [x] 1.3 Redux: `flinkReducer.js` + `flinkActions.js`
- [x] 1.4 `FlinkRulesPage` — TabView 4 tab
- [x] 1.5 `PathAliasTab` (FR-01/02/09) — alias_path lock sau Active, OpenConfig hint
- [x] 1.6 `MetricAliasTab` (FR-03/09) — conditional transform fields, validation 422
- [x] 1.7 `LabelAliasTab` (FR-04) — preview realtime transform (regex/enum)
- [x] 1.8 `FilterRuleTab` (FR-05) — cảnh báo EXCLUDE nổi bật
- [x] 1.9 `RulePreviewDialog` (FR-06)
- [x] 1.10 `RefreshStatusDialog` (FR-07)
- [x] 1.11 `FallthroughMonitor` (FR-08, N)

**Validation quan trọng (FR-02/03/04/05/09):**
- `alias_path` lock sau Active (cảnh báo edit)
- `vendor_code` luôn label là "Model Code" trong UI
- Cascade 409 khi Deprecate Path Alias có con Active
- Filter EXCLUDE cần warning nổi bật

### Phase 2 — Derived Metrics (DM-01 đến DM-09)
**Stories P:** DM-01, DM-02, DM-03, DM-04, DM-05, DM-09
**Stories N:** DM-06, DM-07, DM-08

- [ ] 2.1 Mock data: `derivedRules.js`
- [ ] 2.2 Mock API handlers cho `/derived/*`
- [ ] 2.3 Redux: `derivedReducer.js` + actions
- [ ] 2.4 `DerivedMetricsPage` — DataTable (DM-01) + toggle status (DM-09)
- [ ] 2.5 `DerivedRuleDialog` — wizard chọn derive_kind → render form tương ứng
  - `ComputedForm` (DM-02): input_metrics[], expression, assembly_window_seconds, static_constants
  - `AggregatedForm` (DM-03): 1 input, window_seconds, agg_function (avg/max/min/sum/rate)
  - `DeltaForm` (DM-04): 1 input, delta_scale_factor, delta_reset_threshold
- [ ] 2.6 `ScopeSelector` (DM-05): scope_device_ids (counter X/20) + scope_path_alias_id
- [ ] 2.7 `DerivedPreviewDialog` (DM-06, N-priority)
- [ ] 2.8 `MetricAliasLookup` (DM-07, N-priority)
- [ ] 2.9 `ObservabilityPanel` (DM-08, N-priority)

**Validation quan trọng:**
- `scope_device_ids` max 20 (422 nếu vượt, hiển thị counter "X/20 thiết bị")
- `derive_kind` chỉ có 0/1/2 (không có 3)
- `output_metric` unique, không trùng `alias_metric` nào trong `tlm_metric_aliases`
- `input_metrics` là array of string (alias_metric), NOT array of object

### Phase 3 — Alert Rules (AL-01 đến AL-15)
**Stories P:** AL-01, AL-02, AL-03, AL-04, AL-05 (N), AL-06, AL-07, AL-08, AL-09, AL-11, AL-12, AL-13
**Stories N:** AL-05, AL-10
**Story C:** AL-15

- [ ] 3.1 Mock data: `alertRules.js`, `alertState.js`, `alertHistory.js`
- [ ] 3.2 Mock API handlers cho `/alerts/*`
- [ ] 3.3 Redux: `alertReducer.js` + actions
- [ ] 3.4 `AlertRulesPage` — TabView 3 tab: [Danh sách Rule] [Active Alerts] [Alert History]
- [ ] 3.5 `RuleListTab` — DataTable với SeverityBadge màu (AL-01)
  - PATCH enable/disable (AL-08)
  - DELETE với cascade check (AL-12)
- [ ] 3.6 `BasicRuleDialog` — form 4 condition_kind:
  - threshold (AL-02): comparator + threshold, NO no_data_seconds
  - pct_change_prev (AL-03): + pct_abs, ghi chú "1 sample liền trước"
  - no_data (AL-04): NO comparator/threshold, CHỈ no_data_seconds; ghi chú "FIRED 1 lần/đợt"
  - abs_delta_prev (AL-05): như AL-02 + pct_abs
- [ ] 3.7 `CompositeRuleDialog` — chọn child rules (basic, Active, cùng entity_keys) (AL-06)
- [ ] 3.8 `AlertPreviewDialog` — dry-run preview (AL-07)
- [ ] 3.9 `ActiveAlertsTab` — bảng FIRING từ `tlm_alert_state` (AL-09)
- [ ] 3.10 `AlertStatePanel` — per-instance state, dedup_remaining_seconds (AL-10, N)
- [ ] 3.11 `AlertHistoryTab` — query history, lọc 24h, filter event_type (AL-11)
  - Ghi chú: no_data không có REFIRED ở chế độ mặc định
- [ ] 3.12 `EngineStatusWidget` — Flink Job3 health: status/checkpoint/lag/latency (AL-13)
- [ ] 3.13 AL-15: toggle no_data refire option + cảnh báo (C-priority, làm sau cùng)

**Validation quan trọng:**
- severity: 0=info < 1=warning < 2=error < 3=critical (critical là NẶNG NHẤT)
- no_data: PHẢI vắng comparator/threshold; không refire mặc định
- path_alias_id=0 → derived metric (từ Job 2), khác với null (any-path)
- composite: tất cả child phải basic + Active + cùng entity_keys; không lồng composite

### Phase 4 — Pipeline Monitor (PL-01 đến PL-04)
**Stories P:** PL-01, PL-02, PL-03
**Story N:** PL-04

- [ ] 4.1 Mock data: `pipeline.js` (3 Flink jobs, 4 Kafka topics, 3 CH tables)
- [ ] 4.2 Mock API handlers cho `/pipeline/*`
- [ ] 4.3 Redux: `pipelineReducer.js` + actions
- [ ] 4.4 `PipelineMonitorPage` — 3 panel chính + biểu đồ Recharts
- [ ] 4.5 `FlinkJobsPanel` — trạng thái Job1/2/3, records/s, rule active count (PL-01)
- [ ] 4.6 `KafkaTopicsPanel` — 4 topics: lag + msg/s + cảnh báo lag vượt ngưỡng (PL-02)
- [ ] 4.7 `ClickHouseSinkPanel` — 3 tables: inserts/s, latency p50/p99, error_rate (PL-03)
- [ ] 4.8 `LatencyPanel` — end-to-end latency theo tầng (PL-04, N-priority)

### Phase 5 — Data Explorer (DE-01 đến DE-02)
**Story P:** DE-01
**Story N:** DE-02

- [ ] 5.1 Mock data trong `pipeline.js` (hoặc explorer.js riêng)
- [ ] 5.2 Redux: `explorerReducer.js` + actions
- [ ] 5.3 `DataExplorerPage`
- [ ] 5.4 `MetricsQueryForm` — filter device/metric/path/time, bảng kết quả phân biệt raw vs derived (DE-01)
- [ ] 5.5 `RawPayloadTable` — raw JSON payload từ tlm_metrics_raw, ghi chú TTL 7 ngày (DE-02, N)

---

## 9. Thứ tự ưu tiên tổng thể

```
Phase 0 (Infrastructure)
  → Phase 1 (Flink Rules — 8P/1N, màn hình phức tạp nhất)
  → Phase 2 (Derived Metrics — 6P/3N)
  → Phase 3 (Alert Rules — 11P/2N/1C, màn hình nhiều nhất)
  → Phase 4 (Pipeline Monitor — 3P/1N)
  → Phase 5 (Data Explorer — 1P/1N, đơn giản nhất)
```

Trong mỗi phase: làm P trước → N → C.

---

## 10. Các ràng buộc & pitfall cần nhớ

### UI/Display
- `vendor_code` trong API → hiển thị **"Model Code"** ở mọi nơi (cột, form, filter, tooltip)
- Severity: `0=Info(xanh), 1=Warning(vàng), 2=Error(cam), 3=Critical(đỏ)` — critical là cao nhất
- Filter Rule EXCLUDE: cảnh báo nổi bật "KHÔNG phục hồi được"

### Data Model
- `derive_kind` chỉ có `{0, 1, 2}` — KHÔNG có 3
- `input_metrics` trong derived = **array of string** (alias_metric), KHÔNG phải array of object
- `no_data` alert: không refire chu kỳ mặc định; không có comparator/threshold
- `path_alias_id=0` cho derived metric alerts ≠ `null` (any-path)
- `alias_path` lock sau Active — cần cảnh báo trong form edit
- `scope_device_ids` max 20 phần tử (422 nếu vượt)
- `output_metric` derived không được trùng bất kỳ `alias_metric` nào

### Tech Stack
- PrimeReact v6: KHÔNG có `pt` prop (đó là v7+) — dùng `className`/`style`
- react-router-dom v5: `<Switch>` KHÔNG phải `<Routes>`, dùng `useHistory` KHÔNG phải `useNavigate`
- Redux không có Immer — reducers PHẢI return new object (spread)
- axios 0.21: response là `res.data`

---

## 11. i18n Key Structure

```
public/locales/vi/
  common.json     # chung: nút, trạng thái, thông báo
  flink.json      # FR-*: labels, messages
  derived.json    # DM-*
  alert.json      # AL-*
  pipeline.json   # PL-*
  explorer.json   # DE-*
```

Key format: `namespace:section.key` (ví dụ: `flink:pathAlias.title`)

---

## 12. Kiểm tra hoàn thành (Definition of Done)

Mỗi story được coi là hoàn thành khi:
- [ ] UI render đúng dữ liệu mock
- [ ] Filter/search hoạt động (nếu có)
- [ ] CRUD thao tác được với mock data
- [ ] Validation form đúng theo spec (bao gồm các trường hợp lỗi)
- [ ] Responsive trong layout IPMS 4.0 (sidebar + content)
- [ ] Không có lỗi console.error liên quan đến component
- [ ] Màu sắc và font khớp design system

---

## 13. Không làm trong prototype này (Phase Sau)

- Dashboard tổng quan (DS-*)
- Router inventory, gNMI profile (RI-*, PM-*, PA-*, AA-*)
- Grafana integration (GR-*)
- Notification/escalation/silence (NOC PRO)
- Audit log (AU-*)
- Cài đặt hạ tầng (ST-*)
