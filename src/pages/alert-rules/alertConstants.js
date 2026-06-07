// Nhãn & tuỳ chọn dùng chung cho màn Alert Rules (AL-*)

export const RULE_KIND = { 0: "Basic", 1: "Composite" };
export const RULE_KIND_OPTIONS = [
  { label: "0 - Basic (1 điều kiện / 1 metric)", value: 0 },
  { label: "1 - Composite (AND/OR nhiều child)", value: 1 },
];
export const RULE_KIND_FILTER = [{ label: "Tất cả loại rule", value: "" }].concat(
  Object.entries(RULE_KIND).map(([v, l]) => ({ label: l, value: Number(v) }))
);

export const CONDITION_KIND = {
  0: "threshold",
  1: "pct_change_prev",
  2: "no_data",
  3: "abs_delta_prev",
};
export const CONDITION_KIND_OPTIONS = [
  { label: "0 - threshold (ngưỡng cố định)", value: 0 },
  { label: "1 - pct_change_prev (% so sample trước)", value: 1 },
  { label: "2 - no_data (series ngừng báo)", value: 2 },
  { label: "3 - abs_delta_prev (chênh lệch tuyệt đối)", value: 3 },
];
export const CONDITION_KIND_FILTER = [{ label: "Tất cả điều kiện", value: "" }].concat(
  Object.entries(CONDITION_KIND).map(([v, l]) => ({ label: l, value: Number(v) }))
);

export const COMPARATOR = { 0: ">", 1: ">=", 2: "<", 3: "<=", 4: "==", 5: "!=" };
export const COMPARATOR_OPTIONS = Object.entries(COMPARATOR).map(([v, l]) => ({
  label: `${v} - ${l}`,
  value: Number(v),
}));

export const LOGICAL_OP = { 0: "AND", 1: "OR" };
export const LOGICAL_OP_OPTIONS = [
  { label: "0 - AND (mọi child cùng thỏa)", value: 0 },
  { label: "1 - OR (ít nhất 1 child thỏa)", value: 1 },
];

export const MISSING_AS_OPTIONS = [
  { label: "0 - false (child thiếu data = false)", value: 0 },
  { label: "1 - skip (bỏ child khỏi AND/OR)", value: 1 },
];

export const SEVERITY = { 0: "Info", 1: "Warning", 2: "Error", 3: "Critical" };
export const SEVERITY_OPTIONS = [
  { label: "0 - Info", value: 0 },
  { label: "1 - Warning", value: 1 },
  { label: "2 - Error", value: 2 },
  { label: "3 - Critical", value: 3 },
];
export const SEVERITY_FILTER = [{ label: "Mọi severity", value: "" }].concat(SEVERITY_OPTIONS);

export const STATUS = { 0: "Disabled", 1: "Active" };
// "all" (chuỗi không-số) → applyFilters bỏ qua lọc status (hiện cả Active+Disabled).
// KHÔNG dùng "" vì applyFilters quy "" về status=1 (chỉ Active).
export const STATUS_FILTER = [
  { label: "Active", value: 1 },
  { label: "Disabled", value: 0 },
  { label: "Tất cả", value: "all" },
];

export const EVENT_TYPE_FILTER = [
  { label: "Tất cả loại", value: "" },
  { label: "FIRED", value: "FIRED" },
  { label: "REFIRED", value: "REFIRED" },
  { label: "RESOLVED", value: "RESOLVED" },
];

export const TIME_RANGE_OPTIONS = [
  { label: "1 giờ", value: 60 },
  { label: "6 giờ", value: 360 },
  { label: "24 giờ", value: 1440 },
  { label: "7 ngày", value: 10080 },
];

// entity_keys hay dùng (gợi ý cho MultiSelect)
export const ENTITY_KEY_OPTIONS = [
  { label: "if_name", value: "if_name" },
  { label: "neighbor_address", value: "neighbor_address" },
  { label: "cpu_id", value: "cpu_id" },
  { label: "process_name", value: "process_name" },
  { label: "queue_id", value: "queue_id" },
];

// màu Tag theo severity (PrimeReact severity prop không đủ 4 mức → tự style)
export const SEVERITY_TAG_SEVERITY = { 0: "info", 1: "warning", 2: "danger", 3: "danger" };

// mô tả gọn metric của basic rule
export const metricLabel = (r) => {
  if (Number(r.rule_kind) === 1) return "—";
  const path =
    r.path_alias_id === 0 ? "derived" : r.path_alias_id == null ? "any-path" : `path#${r.path_alias_id}`;
  return `${r.alias_metric} (${path})`;
};

// mô tả gọn điều kiện
export const conditionLabel = (r) => {
  if (Number(r.rule_kind) === 1) {
    return `${LOGICAL_OP[r.logical_op]} [${(r.child_rule_ids || []).join(", ")}]`;
  }
  const ck = Number(r.condition_kind);
  if (ck === 2) return `no_data ${r.no_data_seconds}s`;
  const pct = (ck === 1 || ck === 3) && Number(r.pct_abs) ? " |abs|" : "";
  const unit = ck === 1 ? "%" : "";
  return `${CONDITION_KIND[ck]}${pct} ${COMPARATOR[r.comparator]} ${r.threshold}${unit}`;
};
