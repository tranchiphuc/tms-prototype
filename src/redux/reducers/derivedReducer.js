const initialState = {
  rules: { list: [], loading: false, total: 0 },
  observability: null,
};

// Redux KHÔNG dùng Immer — luôn return new object (spread).
export default function derivedReducer(state = initialState, action) {
  switch (action.type) {
    case "FETCH_DERIVED_RULES":
      return { ...state, rules: { ...state.rules, loading: true } };
    case "FETCH_DERIVED_RULES_SUCCESS": {
      const data = action.payload.data || {};
      return {
        ...state,
        rules: { list: data.items || [], total: data.total || 0, loading: false },
      };
    }
    case "FETCH_DERIVED_RULES_FAIL":
      return { ...state, rules: { ...state.rules, loading: false } };

    case "FETCH_DERIVED_OBSERVABILITY_SUCCESS":
      return { ...state, observability: action.payload.data };

    default:
      return state;
  }
}
