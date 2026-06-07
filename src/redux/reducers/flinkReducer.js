const emptyList = { list: [], loading: false, total: 0 };

const initialState = {
  pathAliases: { ...emptyList },
  metricAliases: { ...emptyList },
  labelAliases: { ...emptyList },
  filterRules: { ...emptyList },
  refreshStatus: null,
  fallthrough: { items: [], note: "" },
  devices: [],
};

const LIST_SLICES = ["pathAliases", "metricAliases", "labelAliases", "filterRules"];

// slice từ action (pending) hoặc previousAction (success/fail của redux-axios-middleware)
const getSlice = (action) => {
  if (action.meta && action.meta.slice) return action.meta.slice;
  if (action.meta && action.meta.previousAction && action.meta.previousAction.meta)
    return action.meta.previousAction.meta.slice;
  return null;
};

// Redux KHÔNG dùng Immer — luôn return new object (spread).
export default function flinkReducer(state = initialState, action) {
  const { type } = action;

  // ---- List fetch (FETCH_<SLICE>[_SUCCESS|_FAIL]) ----
  if (type.startsWith("FETCH_")) {
    const slice = getSlice(action);
    if (slice && LIST_SLICES.includes(slice)) {
      if (type.endsWith("_SUCCESS")) {
        const data = action.payload.data || {};
        return {
          ...state,
          [slice]: { list: data.items || [], total: data.total || 0, loading: false },
        };
      }
      if (type.endsWith("_FAIL")) {
        return { ...state, [slice]: { ...state[slice], loading: false } };
      }
      // pending
      return { ...state, [slice]: { ...state[slice], loading: true } };
    }
  }

  switch (type) {
    case "FETCH_DEVICES_SUCCESS":
      return { ...state, devices: (action.payload.data || {}).items || [] };
    case "FETCH_REFRESH_STATUS_SUCCESS":
      return { ...state, refreshStatus: action.payload.data };
    case "FETCH_FALLTHROUGH_SUCCESS":
      return { ...state, fallthrough: action.payload.data };
    default:
      return state;
  }
}
