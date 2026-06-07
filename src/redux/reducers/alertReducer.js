const initialState = {
  rules: { list: [], loading: false, total: 0 },
  activeAlerts: { list: [], loading: false, total: 0 },
  history: { list: [], loading: false, total: 0 },
  engineStatus: null,
};

// Redux KHÔNG dùng Immer — luôn return new object (spread).
export default function alertReducer(state = initialState, action) {
  switch (action.type) {
    // ---- Rules list (AL-01) ----
    case "FETCH_ALERT_RULES":
      return { ...state, rules: { ...state.rules, loading: true } };
    case "FETCH_ALERT_RULES_SUCCESS": {
      const data = action.payload.data || {};
      return { ...state, rules: { list: data.items || [], total: data.total || 0, loading: false } };
    }
    case "FETCH_ALERT_RULES_FAIL":
      return { ...state, rules: { ...state.rules, loading: false } };

    // ---- Active alerts (AL-09) ----
    case "FETCH_ACTIVE_ALERTS":
      return { ...state, activeAlerts: { ...state.activeAlerts, loading: true } };
    case "FETCH_ACTIVE_ALERTS_SUCCESS": {
      const data = action.payload.data || {};
      return {
        ...state,
        activeAlerts: { list: data.items || [], total: data.total || 0, loading: false },
      };
    }
    case "FETCH_ACTIVE_ALERTS_FAIL":
      return { ...state, activeAlerts: { ...state.activeAlerts, loading: false } };

    // ---- History (AL-11) ----
    case "FETCH_ALERT_HISTORY":
      return { ...state, history: { ...state.history, loading: true } };
    case "FETCH_ALERT_HISTORY_SUCCESS": {
      const data = action.payload.data || {};
      return {
        ...state,
        history: { list: data.items || [], total: data.total || 0, loading: false },
      };
    }
    case "FETCH_ALERT_HISTORY_FAIL":
      return { ...state, history: { ...state.history, loading: false } };

    // ---- Engine status (AL-13) ----
    case "FETCH_ALERT_ENGINE_STATUS_SUCCESS":
      return { ...state, engineStatus: action.payload.data };

    default:
      return state;
  }
}
