# PROGRESS — TMS Dashboard (ipms-fe)

> Sổ tay tiếp tục công việc giữa các máy. Cập nhật mỗi khi kết thúc một phiên.
> Đọc kèm `PLAN.md` (kế hoạch đầy đủ) và `.claude/CLAUDE.md` (ràng buộc tech stack / data model).
> **Cập nhật lần cuối:** 2026-06-07 — kết thúc Phase 3 (Alert Rules) + tinh chỉnh UI.

---

## 0. Resume nhanh trên máy mới

```bash
# Node BẮT BUỘC dùng v16 — node-sass@6 KHÔNG build trên Node 18+/20+/24
nvm install 16 && nvm use 16      # hoặc chỉ `nvm use 16` nếu đã có
node -v                            # phải là v16.x

npm install                        # cài lại deps (node_modules KHÔNG nằm trong git)
npm start                          # dev server → http://localhost:3000
CI=true npm run build              # build kiểm tra (CI=true coi warning là lỗi)
npm test                           # jest
```

- `.env` đã commit (SKIP_PREFLIGHT_CHECK, GENERATE_SOURCEMAP=false, PORT=3000).
- Tất cả dùng **mock data** trong `src/mock/`, KHÔNG gọi API thật. Mock adapter: `src/services/mockApi.js`; handlers đăng ký trong `src/mock/handlers/index.js`.
- Smoke test runtime (khi cần xác nhận DOM): headless Chrome + CDP qua `google-chrome --headless=new --remote-debugging-port=9222` và module `ws` trong node_modules (xem cách dùng trong lịch sử phiên).

---

## 1. Trạng thái phase

| Phase | Màn hình | Route | Trạng thái |
|-------|----------|-------|------------|
| 0 | Scaffold (layout, routing, redux, i18n, design tokens, shared components) | — | ✅ Done (2026-06-06) |
| 1 | Flink Rules (FR-01..09) — TabView 4 tab | `/flink-rules` | ✅ Done (2026-06-06) + post-review fixes (2026-06-07) |
| 2 | Derived Metrics (DM-01..09) | `/derived-metrics` | ✅ Done (2026-06-07) |
| 3 | Alert Rules (AL-01..15) — TabView 3 tab | `/alert-rules` | ✅ Done (2026-06-07) |
| 4 | Pipeline Monitor (PL-01..04) | `/pipeline-monitor` | ⬜ **NEXT** |
| 5 | Data Explorer (DE-01..02) | `/data-explorer` | ⬜ Pending |

---

## 2. Đã làm — chi tiết

### Phase 0 (scaffold)
Layout (sidebar + topbar, sidebar collapse lưu `localStorage.sidebarCollapsed`), routing react-router v5 (`<Switch>/<Route>`), Redux store (`redux-axios-middleware` + mock adapter, `returnRejectedPromiseOnError:true`), i18n vi (`public/locales/vi/*.json`), design tokens (`src/styles/_variables.scss`), shared components (`StatusBadge`/`SeverityBadge`/`ActionButtons`), 5 page placeholders.

### Phase 1 (Flink Rules)
Mock: `src/mock/{pathAliases,metricAliases,labelAliases,filterRules,devices,helpers}.js`. Handlers: `src/mock/handlers/flinkHandlers.js` (CRUD generic + validation 409/422 + cascade + preview + refresh-status + fallthrough). Redux `flinkActions`/`flinkReducer` (multi-slice qua `meta.slice`). UI: TabView 4 tab, `RuleListShell`+`FilterBar`+`FormFields`+`ToastProvider`, hook `useRuleTab`, 4 dialog CRUD, `RulePreviewDialog`, `RefreshStatusDialog`, `FallthroughMonitor`.
Post-review: font 13px toàn cục; layout không tràn (sidebar fixed + margin-left); bảng `table-layout:fixed` + wrap; "Refresh status" lên page header; **bỏ "All" khỏi Model Code**.

### Phase 2 (Derived Metrics)
Mock `derivedRules.js` (11 rows: 4 computed / 5 aggregated / 1 deprecated / 2 delta). `handlers/derivedHandlers.js` — CRUD + validate theo `derive_kind∈{0,1,2}` (computed: expression + null window/agg; aggregated: đúng 1 input + window + agg; delta: đúng 1 input; `scope_device_ids≤20`→422; `output_metric` unique-in-active + không trùng `alias_metric`) + preview (DM-06) + observability (DM-08). **KHÔNG có `pushed_at`** (handler `delete updated.pushed_at`). Redux `derivedActions`/`derivedReducer`. UI: `DerivedRulesList` (filter bar riêng), `DerivedRuleDialog` (kind-aware), `DerivedPreviewDialog`, `MetricLookupDialog` (DM-07), `ObservabilityDialog`, `derivedConstants.js`.
Tiện ích thêm: `src/utils/exportFile.js` (`toCSV`/`downloadBlob`/`fileStamp`, BOM UTF-8); export dùng action type riêng (`EXPORT_*`) để reducer bỏ qua (không ghi đè bảng đang lọc); `MultiSelectField` trong `FormFields.js`.

### Phase 3 (Alert Rules) — vừa hoàn thành
Mock: `alertRules.js` (12 basic + 3 composite: đủ 4 `condition_kind`, derived `path_alias_id=0`, 1 Disabled, 1 có `entity_filter`, composite AND/OR), `alertState.js` (6 FIRING + 3 RESOLVED), `alertHistory.js` (25 sự kiện 24h+; **no_data KHÔNG có REFIRED** theo HLD v1.4 §6.5 / AL-04).
Handlers `alertHandlers.js`: CRUD + validate kind-aware (basic: `condition_kind∈{0,1,2,3}`; no_data cần `no_data_seconds>0` và VẮNG comparator/threshold; loại khác cần comparator+threshold; composite: `logical_op∈{0,1}`, `child_rule_ids` non-empty/distinct, mỗi child phải basic+Active+**cùng entity_keys**, không self-ref/lồng) + GET expand `children[]` + PATCH (AL-08) + DELETE 409-nếu-composite-Active-tham-chiếu / `?force` cascade (AL-12) + preview boolean (AL-07) + `/rules/{id}/state` (AL-10) + `/active` (AL-09) + `/history` (AL-11, lọc time/rule/device/severity/event_type, mặc định 24h) + `/engine/status` (AL-13) + `/refresh-status`.
Redux `alertActions.js` + `alertReducer.js` (slices: rules / activeAlerts / history + engineStatus).
UI (`src/pages/alert-rules/`): `alertConstants.js`, `AlertRulesPage.js` (TabView 3 tab + header export/engine-status), `AlertRulesList.js` (AL-01), `AlertRuleDialog.js` (AL-02..06 kind-aware), `AlertPreviewDialog.js`, `RuleStateDialog.js`, `ActiveAlertsTab.js`, `AlertHistoryTab.js` (+CSV export), `EngineStatusDialog.js`, `fmt.js`.
Tinh chỉnh UI cuối phiên: sửa tràn ngang bảng (xem Gotcha #3); icon hành động (xem Gotcha #4); làm rõ tooltip Tắt/Bật vs Xóa.

---

## 3. Việc tiếp theo — Phase 4: Pipeline Monitor (PL-01..04)

- Route `/pipeline-monitor`. User stories: xem `docs/00-user_stories_vi_v6_3.md` (PL-01..04).
- Data model: `.claude/CLAUDE.md` §4.5 — 3 đối tượng: **Flink jobs** (job_name, status Running/Failed/Stopped, records_per_sec, uptime, active_rules_count, last_checkpoint_at, consumer_lag, watermark_lag_ms, eval_latency_ms), **Kafka topics** (telemetry.raw, processed_metrics, derived_metrics, alerts — consumer_lag, messages_per_sec, lag_threshold_warning), **ClickHouse sinks** (tlm_metrics, tlm_metrics_raw, alert_history — inserts_per_sec, batch_size, query_latency p50/p99, disk_usage_gb, error_rate, sink_delay_ms).
- Mock cần: 3 Flink jobs (Job1/2/3), 4 Kafka topics, 3 ClickHouse tables.
- Charts: dùng `recharts@2` (LineChart/BarChart/ResponsiveContainer) — đã có trong deps.
- Tái dùng: pattern handler factory, redux `meta.slice`, `useToast`, design tokens. Đăng ký handler mới trong `src/mock/handlers/index.js`.

### Sau đó — Phase 5: Data Explorer (DE-01..02)
Route `/data-explorer`. ClickHouse `ipms.tlm_metrics` (CLAUDE.md §4.6). `raw_path="derived"` để phân biệt derived vs raw telemetry.

---

## 4. Quy ước / pattern đã chốt (tái dùng ở Phase 4-5)

- **Toolchain:** LUÔN `nvm use 16` trước mọi lệnh npm. Node 18+ vỡ node-sass@6 (thiếu thì thêm `--openssl-legacy-provider`).
- **Redux:** action `redux-axios-middleware` với `payload.request` + (đa-slice) `meta.slice`; reducer KHÔNG Immer → luôn return object mới (spread). Dialog `.then()/.catch((rej)=>errInfo(rej))`.
- **Mock handler factory:** `registerHandler(regex, ctx => ({status, data}))`; throw qua `httpError(status, msg, details)`. Route cụ thể đăng ký TRƯỚC route có `{id}`.
- **Bảng:** mọi cột đặt **width tường minh** (dưới `table-layout:fixed`, cột thiếu width sẽ bị co/“chữ dọc”). Giữ **tổng width cột vừa khít vùng nội dung** (xem Gotcha #3).
- **Form:** `FormFields.js` (TextField/TextAreaField/NumberField/DropdownField/MultiSelectField/CheckboxField + `required`) cho `react-final-form`.
- **Export CSV:** `src/utils/exportFile.js`; dùng action type `EXPORT_*` (reducer bỏ qua) để không ghi đè bảng đang lọc.
- **Lỗi API:** `src/utils/apiError.js` `errInfo(rej)` → `{status, message, details}`.

---

## 5. GOTCHAS QUAN TRỌNG (đọc trước khi code tiếp)

1. **`vendor_code` ≠ "Model Code":** field API/DB tên `vendor_code`, UI LUÔN hiển thị "Model Code". Và **bỏ "All"** khỏi tập giá trị Model Code (chỉ Cisco/Juniper/Nokia/OpenConfig) — đã override docs.
2. **`applyFilters` quy `status===""` → `1` (chỉ Active):** nên filter "Tất cả trạng thái" mà truyền `""` sẽ ÂM THẦM chỉ ra Active (ảnh hưởng cả flink/derived và CSV export của chúng — latent bug). Cách Phase 3 xử lý: dùng sentinel **`"all"`** (chuỗi không-số → `Number("all")=NaN` → `applyFilters` bỏ qua lọc status). KHÔNG sửa `helpers.applyFilters` để tránh lan sang Phase 1/2.
3. **Tràn ngang bảng:** dưới `table-layout:fixed`, used-width của bảng = MAX(100%, Σ width cột). Nếu Σ > vùng nội dung → bảng tự ép rộng và **đẩy cả trang tràn ngang**. Đã thêm guard `.rule-table/.preview-table .p-datatable-wrapper { overflow-x:auto }` trong `_common.scss` (dư thì cuộn nội bộ, trang không tràn) + giữ Σ width cột nhỏ + `$sidebar-width` 230→210px. **Giữ Σ width cột vừa phải ở Phase 4-5.**
4. **primeicons@4.1.0 thiếu icon:** một số tên icon KHÔNG có trong v4.1.0 → render nút rỗng (vẫn chiếm chỗ). Đã gặp `pi-bolt`, `pi-server`, `pi-history`. **Luôn grep `node_modules/primeicons/primeicons.css` (`pi-NAME:before`) trước khi dùng.**
5. **`derive_kind` chỉ {0,1,2}** (không có 3). **`severity` 0=info<1=warning<2=error<3=critical** (critical cao nhất). **`path_alias_id=0`** = derived metric (khác `null`=any-path).
6. **no_data alert:** mặc định FIRED 1 lần/đợt im lặng, KHÔNG refire chu kỳ (v1.4). `input_metrics` derived là **array of string**. `alias_path` Path Alias KHÔNG sửa được sau Active. Filter Rule drop metric KHÔNG phục hồi.
7. **Thư viện cố định:** primereact@6 (KHÔNG `pt` prop), react-router-dom@5 (`useHistory` không phải `useNavigate`), axios@0.21 (`res.data`), redux@4 (KHÔNG Toolkit), moment (KHÔNG dayjs/date-fns). KHÔNG TypeScript, KHÔNG class component.

---

## 6. Bản đồ file (định hướng nhanh)

```
src/
  services/mockApi.js          # axios mock adapter + registerHandler
  mock/
    helpers.js                 # applyFilters/applySort/paginate/touch/httpError/nextId/isoMinutesAgo
    devices.js  pathAliases.js  metricAliases.js  labelAliases.js  filterRules.js
    derivedRules.js  alertRules.js  alertState.js  alertHistory.js
    handlers/index.js          # đăng ký tất cả handler (import side-effect)
    handlers/{flink,derived,alert}Handlers.js
  redux/
    store.js  reducers/index.js
    actions/{flink,derived,alert}Actions.js
    reducers/{flink,derived,alert,pipeline,explorer}Reducer.js
  components/                  # StatusBadge SeverityBadge FormFields FilterBar RuleListShell ToastProvider ActionButtons
  utils/                       # exportFile.js  apiError.js
  styles/_variables.scss  _common.scss
  pages/{flink-rules,derived-metrics,alert-rules,pipeline-monitor,data-explorer}/
public/locales/vi/*.json       # i18n (common flink derived alert pipeline explorer)
docs/                          # user stories + HLD (nguồn ràng buộc nghiệp vụ)
```
