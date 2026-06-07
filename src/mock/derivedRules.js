// ============================================================
// Mock data — Derived Rules (tlm_derived_rules) — Phase 2 / DM-*
// ------------------------------------------------------------
// derive_kind: 0=computed, 1=aggregated, 2=delta  (KHÔNG có 3)
// input_metrics: ARRAY OF STRING (alias_metric) — KHÔNG phải object
// KHÔNG có pushed_at (tlm_derived_rules không có ClickHouse mirror;
//   Job 2 đọc thẳng từ MariaDB, versioning chỉ qua updated_at)
// agg_function: 0=avg,1=max,2=min,3=sum,4=rate (chỉ derive_kind=1)
// ============================================================

const ts = (mins) =>
  new Date(new Date("2026-06-06T09:00:00+07:00").getTime() - mins * 60000).toISOString();

// Khung đầy đủ — mọi field tồn tại, null khi N/A theo derive_kind.
const base = {
  expression: null,
  assembly_window_seconds: null,
  window_seconds: null,
  agg_function: null,
  delta_scale_factor: null,
  delta_reset_threshold: null,
  static_constants: null,
  scope_path_alias_id: null,
  scope_device_ids: null,
  priority: 100,
  status: 1,
  created_by: "operator",
};

export const DERIVED_RULES_DATA = [
  // ---- Computed (derive_kind=0) ----
  {
    ...base,
    id: 501,
    rule_name: "Tổng octet interface (in+out)",
    output_metric: "if_total_octets",
    output_unit: "By",
    derive_kind: 0,
    input_metrics: ["if_in_octets", "if_out_octets"],
    expression: "if_in_octets + if_out_octets",
    assembly_window_seconds: 90,
    priority: 100,
    created_at: ts(2400),
    updated_at: ts(120),
  },
  {
    ...base,
    id: 502,
    rule_name: "Tỉ lệ sử dụng bộ nhớ (%)",
    output_metric: "memory_utilization_pct",
    output_unit: "%",
    derive_kind: 0,
    input_metrics: ["memory_used_bytes", "memory_free_bytes"],
    expression: "memory_used_bytes / (memory_used_bytes + memory_free_bytes) * 100",
    assembly_window_seconds: 90,
    priority: 100,
    created_at: ts(2300),
    updated_at: ts(300),
  },
  {
    ...base,
    id: 503,
    rule_name: "Link utilization vào (% capacity 10G)",
    output_metric: "if_in_utilization_pct",
    output_unit: "%",
    derive_kind: 0,
    input_metrics: ["if_in_bps"],
    expression: "if_in_bps / link_capacity_bps * 100",
    assembly_window_seconds: 75,
    static_constants: { link_capacity_bps: 10000000000 },
    scope_path_alias_id: 9, // oc_if_counters — khử nhập nhằng path
    priority: 110,
    created_at: ts(1800),
    updated_at: ts(45),
  },
  {
    ...base,
    id: 504,
    rule_name: "Tổng lỗi interface (in+out errors)",
    output_metric: "if_total_errors",
    output_unit: "",
    derive_kind: 0,
    input_metrics: ["if_in_errors", "if_out_errors"],
    expression: "if_in_errors + if_out_errors",
    assembly_window_seconds: 90,
    scope_device_ids: [1, 2],
    priority: 100,
    created_at: ts(900),
    updated_at: ts(900),
  },

  // ---- Aggregated (derive_kind=1) ----
  {
    ...base,
    id: 505,
    rule_name: "CPU trung bình 5 phút",
    output_metric: "cpu_util_avg_5m",
    output_unit: "%",
    derive_kind: 1,
    input_metrics: ["cpu_utilization_5min"],
    window_seconds: 300,
    agg_function: 0, // avg
    priority: 100,
    created_at: ts(2100),
    updated_at: ts(200),
  },
  {
    ...base,
    id: 506,
    rule_name: "if_in_octets max 1 phút",
    output_metric: "if_in_octets_max_1m",
    output_unit: "By",
    derive_kind: 1,
    input_metrics: ["if_in_octets"],
    window_seconds: 60,
    agg_function: 1, // max
    priority: 100,
    created_at: ts(1500),
    updated_at: ts(75),
  },
  {
    ...base,
    id: 507,
    rule_name: "BGP prefixes nhận — tổng 5 phút",
    output_metric: "bgp_prefixes_recv_sum_5m",
    output_unit: "",
    derive_kind: 1,
    input_metrics: ["bgp_prefixes_received"],
    window_seconds: 300,
    agg_function: 3, // sum
    scope_path_alias_id: 13, // xr_bgp_neighbor
    priority: 100,
    created_at: ts(1200),
    updated_at: ts(600),
  },
  {
    ...base,
    id: 508,
    rule_name: "if_in_octets rate 1 phút",
    output_metric: "if_in_octets_rate_1m",
    output_unit: "By/s",
    derive_kind: 1,
    input_metrics: ["if_in_octets"],
    window_seconds: 60,
    agg_function: 4, // rate
    priority: 100,
    created_at: ts(800),
    updated_at: ts(30),
  },
  {
    ...base,
    id: 509,
    rule_name: "Bộ nhớ trống — min 5 phút",
    output_metric: "memory_free_min_5m",
    output_unit: "By",
    derive_kind: 1,
    input_metrics: ["memory_free_bytes"],
    window_seconds: 300,
    agg_function: 2, // min
    status: 0, // Deprecated — test filter
    priority: 100,
    created_at: ts(2000),
    updated_at: ts(1000),
  },

  // ---- Delta (derive_kind=2) ----
  {
    ...base,
    id: 510,
    rule_name: "if_in_octets → Mbps (delta)",
    output_metric: "if_in_mbps",
    output_unit: "Mbps",
    derive_kind: 2,
    input_metrics: ["if_in_octets"],
    delta_scale_factor: 0.000008, // 8 / 1_000_000 (bytes → Mbps)
    delta_reset_threshold: null,
    scope_path_alias_id: 1, // xr_if_generic_counters
    priority: 100,
    created_at: ts(2200),
    updated_at: ts(90),
  },
  {
    ...base,
    id: 511,
    rule_name: "if_out_octets → Mbps (delta, reset counter32)",
    output_metric: "if_out_mbps",
    output_unit: "Mbps",
    derive_kind: 2,
    input_metrics: ["if_out_octets"],
    delta_scale_factor: 0.000008,
    delta_reset_threshold: 4294967295, // counter32 wrap
    scope_device_ids: [3, 4, 5],
    priority: 100,
    created_at: ts(1700),
    updated_at: ts(150),
  },
];
