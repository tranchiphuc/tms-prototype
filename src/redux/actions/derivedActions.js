// ============================================================
// Derived Metrics actions (DM-*) — qua redux-axios-middleware.
// ============================================================

export const fetchDerivedRules = (params = {}) => ({
  type: "FETCH_DERIVED_RULES",
  payload: {
    request: { url: "/derived/rules", method: "get", params },
  },
});

// Export — lấy toàn bộ rule nhưng KHÔNG ghi vào store (type riêng để reducer
// bỏ qua) → không làm thay đổi bảng đang lọc trên màn hình.
export const exportDerivedRules = (params = {}) => ({
  type: "EXPORT_DERIVED_RULES",
  payload: { request: { url: "/derived/rules", method: "get", params } },
});

export const createDerivedRule = (body) => ({
  type: "CREATE_DERIVED_RULE",
  payload: { request: { url: "/derived/rules", method: "post", data: body } },
});

export const updateDerivedRule = (id, body) => ({
  type: "UPDATE_DERIVED_RULE",
  payload: { request: { url: `/derived/rules/${id}`, method: "put", data: body } },
});

export const patchDerivedRule = (id, body) => ({
  type: "PATCH_DERIVED_RULE",
  payload: { request: { url: `/derived/rules/${id}`, method: "patch", data: body } },
});

export const deleteDerivedRule = (id) => ({
  type: "DELETE_DERIVED_RULE",
  payload: { request: { url: `/derived/rules/${id}`, method: "delete" } },
});

export const previewDerivedRule = (body) => ({
  type: "PREVIEW_DERIVED_RULE",
  payload: { request: { url: "/derived/rules/preview", method: "post", data: body } },
});

export const fetchObservability = () => ({
  type: "FETCH_DERIVED_OBSERVABILITY",
  payload: { request: { url: "/derived/observability", method: "get" } },
});

// DM-07 — lookup alias_metric trải trên nhiều path (dùng endpoint Flink)
export const lookupMetricAlias = (aliasMetric) => ({
  type: "LOOKUP_METRIC_ALIAS",
  payload: {
    request: {
      url: "/flink/metric-aliases",
      method: "get",
      params: { alias_metric: aliasMetric, status: "", page_size: 200 },
    },
  },
});
