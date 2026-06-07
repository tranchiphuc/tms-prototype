// ============================================================
// Flink actions — qua redux-axios-middleware.
// Mỗi action có payload.request (axios config). Middleware tự
// dispatch TYPE / TYPE_SUCCESS / TYPE_FAIL và trả promise từ dispatch.
// ============================================================

// slice (state key) ↔ group (API path segment)
export const GROUP_PATH = {
  pathAliases: "path-aliases",
  metricAliases: "metric-aliases",
  labelAliases: "label-aliases",
  filterRules: "filter-rules",
};

const upper = (s) => s.replace(/([A-Z])/g, "_$1").toUpperCase();

// GET list — reducer lưu vào state.flink[slice]
export const fetchList = (slice, params = {}) => ({
  type: `FETCH_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}`, method: "get", params },
  },
});

// Export — lấy toàn bộ rule của group nhưng KHÔNG ghi vào store.
// Type EXPORT_* không khớp `FETCH_`/`CREATE_`... nên flinkReducer bỏ qua →
// bảng đang lọc trên màn hình không bị thay đổi.
export const exportList = (slice, params = {}) => ({
  type: `EXPORT_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}`, method: "get", params },
  },
});

export const createRule = (slice, body) => ({
  type: `CREATE_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}`, method: "post", data: body },
  },
});

export const updateRule = (slice, id, body) => ({
  type: `UPDATE_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}/${id}`, method: "put", data: body },
  },
});

export const patchRule = (slice, id, body) => ({
  type: `PATCH_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}/${id}`, method: "patch", data: body },
  },
});

export const deleteRule = (slice, id, force = false) => ({
  type: `DELETE_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: {
      url: `/flink/${GROUP_PATH[slice]}/${id}`,
      method: "delete",
      params: force ? { force: true } : {},
    },
  },
});

export const previewRule = (slice, body) => ({
  type: `PREVIEW_${upper(slice)}`,
  meta: { slice },
  payload: {
    request: { url: `/flink/${GROUP_PATH[slice]}/preview`, method: "post", data: body },
  },
});

export const fetchDevices = () => ({
  type: "FETCH_DEVICES",
  payload: { request: { url: "/flink/devices", method: "get" } },
});

export const fetchRefreshStatus = () => ({
  type: "FETCH_REFRESH_STATUS",
  payload: { request: { url: "/flink/refresh-status", method: "get" } },
});

export const fetchFallthrough = () => ({
  type: "FETCH_FALLTHROUGH",
  payload: { request: { url: "/flink/fallthrough", method: "get" } },
});
