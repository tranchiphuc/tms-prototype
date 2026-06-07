// ============================================================
// Alert Rules actions (AL-*) — qua redux-axios-middleware.
// ============================================================

export const fetchAlertRules = (params = {}) => ({
  type: "FETCH_ALERT_RULES",
  payload: { request: { url: "/alerts/rules", method: "get", params } },
});

// Export — type riêng để reducer bỏ qua (không ghi đè bảng đang lọc).
export const exportAlertRules = (params = {}) => ({
  type: "EXPORT_ALERT_RULES",
  payload: { request: { url: "/alerts/rules", method: "get", params } },
});

export const fetchAlertRule = (id) => ({
  type: "FETCH_ALERT_RULE",
  payload: { request: { url: `/alerts/rules/${id}`, method: "get" } },
});

export const createAlertRule = (body) => ({
  type: "CREATE_ALERT_RULE",
  payload: { request: { url: "/alerts/rules", method: "post", data: body } },
});

export const updateAlertRule = (id, body) => ({
  type: "UPDATE_ALERT_RULE",
  payload: { request: { url: `/alerts/rules/${id}`, method: "put", data: body } },
});

export const patchAlertRule = (id, body) => ({
  type: "PATCH_ALERT_RULE",
  payload: { request: { url: `/alerts/rules/${id}`, method: "patch", data: body } },
});

export const deleteAlertRule = (id, force = false) => ({
  type: "DELETE_ALERT_RULE",
  payload: {
    request: { url: `/alerts/rules/${id}`, method: "delete", params: force ? { force: true } : {} },
  },
});

export const previewAlertRule = (body) => ({
  type: "PREVIEW_ALERT_RULE",
  payload: { request: { url: "/alerts/rules/preview", method: "post", data: body } },
});

// AL-10 — firing state per-instance của một rule
export const fetchRuleState = (id) => ({
  type: "FETCH_ALERT_RULE_STATE",
  payload: { request: { url: `/alerts/rules/${id}/state`, method: "get" } },
});

// AL-09 — alert đang FIRING
export const fetchActiveAlerts = (params = {}) => ({
  type: "FETCH_ACTIVE_ALERTS",
  payload: { request: { url: "/alerts/active", method: "get", params } },
});

// AL-11 — alert history
export const fetchAlertHistory = (params = {}) => ({
  type: "FETCH_ALERT_HISTORY",
  payload: { request: { url: "/alerts/history", method: "get", params } },
});

export const exportAlertHistory = (params = {}) => ({
  type: "EXPORT_ALERT_HISTORY",
  payload: { request: { url: "/alerts/history", method: "get", params } },
});

// AL-13 — Flink Job 3 health
export const fetchEngineStatus = () => ({
  type: "FETCH_ALERT_ENGINE_STATUS",
  payload: { request: { url: "/alerts/engine/status", method: "get" } },
});
