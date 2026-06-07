// ============================================================
// Cấu hình cột export CSV cho 4 group Flink Rule.
// LƯU Ý: vendor_code hiển thị header là "Model Code" (CLAUDE.md §9).
// ============================================================

const STATUS = (v) => (Number(v) === 1 ? "Active" : "Deprecated");
const VALUE_TYPE = { 1: "number", 2: "string", 3: "bool" };
const TRANSFORM_KIND = { 0: "identity", 1: "linear", 2: "expression", 3: "enum_mapping" };
const LV_KIND = { 0: "identity", 1: "regex", 2: "enum_mapping" };
const FILTER_ACTION = { 0: "EXCLUDE_IF_MATCH", 1: "INCLUDE_IF_MATCH" };

const enumVal = (map) => (v) => (v == null || v === "" ? "" : map[v] ?? v);

const COMMON_TAIL = [
  { header: "priority", value: (r) => r.priority },
  { header: "status", value: (r) => STATUS(r.status) },
  { header: "pushed_at", value: (r) => r.pushed_at },
  { header: "created_by", value: (r) => r.created_by },
  { header: "created_at", value: (r) => r.created_at },
  { header: "updated_at", value: (r) => r.updated_at },
];

export const EXPORT_CONFIG = {
  pathAliases: {
    filenameBase: "flink-path-aliases",
    columns: [
      { header: "id", value: (r) => r.id },
      { header: "Model Code", value: (r) => r.vendor_code },
      { header: "original_path", value: (r) => r.original_path },
      { header: "alias_path", value: (r) => r.alias_path },
      ...COMMON_TAIL,
    ],
  },
  metricAliases: {
    filenameBase: "flink-metric-aliases",
    columns: [
      { header: "id", value: (r) => r.id },
      { header: "Model Code", value: (r) => r.vendor_code },
      { header: "path_alias_id", value: (r) => (r.path_alias_id == null ? "any-path" : r.path_alias_id) },
      { header: "original_name", value: (r) => r.original_name },
      { header: "alias_metric", value: (r) => r.alias_metric },
      { header: "value_type_override", value: (r) => enumVal(VALUE_TYPE)(r.value_type_override) },
      { header: "source_unit", value: (r) => r.source_unit },
      { header: "target_unit", value: (r) => r.target_unit },
      { header: "transform_kind", value: (r) => enumVal(TRANSFORM_KIND)(r.transform_kind) },
      { header: "scale_factor", value: (r) => r.scale_factor },
      { header: "offset_value", value: (r) => r.offset_value },
      { header: "transform_expression", value: (r) => r.transform_expression },
      { header: "enum_mapping", value: (r) => r.enum_mapping },
      ...COMMON_TAIL,
    ],
  },
  labelAliases: {
    filenameBase: "flink-label-aliases",
    columns: [
      { header: "id", value: (r) => r.id },
      { header: "Model Code", value: (r) => r.vendor_code },
      { header: "path_alias_id", value: (r) => (r.path_alias_id == null ? "any-path" : r.path_alias_id) },
      { header: "original_key", value: (r) => r.original_key },
      { header: "alias_key", value: (r) => r.alias_key },
      { header: "lv_kind", value: (r) => enumVal(LV_KIND)(r.lv_kind) },
      { header: "lv_pattern", value: (r) => r.lv_pattern },
      { header: "lv_replace", value: (r) => r.lv_replace },
      { header: "lv_mapping", value: (r) => r.lv_mapping },
      ...COMMON_TAIL,
    ],
  },
  filterRules: {
    filenameBase: "flink-filter-rules",
    columns: [
      { header: "id", value: (r) => r.id },
      { header: "Model Code", value: (r) => r.vendor_code },
      { header: "match_path", value: (r) => r.match_path },
      { header: "match_metric", value: (r) => r.match_metric },
      { header: "filter_expression", value: (r) => r.filter_expression },
      { header: "filter_action", value: (r) => enumVal(FILTER_ACTION)(r.filter_action) },
      ...COMMON_TAIL,
    ],
  },
};
