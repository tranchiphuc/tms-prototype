// ============================================================
// Mock data — Alert Rules (tlm_alert_rules) — Phase 3 / AL-*
// ------------------------------------------------------------
// rule_kind:      0=basic, 1=composite
// condition_kind: 0=threshold, 1=pct_change_prev, 2=no_data, 3=abs_delta_prev (chỉ basic)
// severity:       0=info, 1=warning, 2=error, 3=critical (critical NẶNG nhất)
// status:         0=Disabled, 1=Active
// comparator:     0=>,1=>=,2=<,3=<=,4===,5=!=  (PHẢI VẮNG nếu condition_kind=2)
// path_alias_id:  null=any-path; 0=derived metric (từ Job 2)
// entity_keys:    JSON array string; []=device-level
// emit_independent: 0|1 (mặc định 1 — basic vẫn fire độc lập kể cả khi là child)
// no_data_seconds: >0; CHỈ khi condition_kind=2
// composite: logical_op (0=AND,1=OR), child_rule_ids[], missing_as (0=false,1=true)
// KHÔNG có pushed_at (tlm_alert_rules là nguồn rule broadcast cho Job 3)
// ============================================================

const ts = (mins) =>
  new Date(new Date("2026-06-06T09:00:00+07:00").getTime() - mins * 60000).toISOString();

// Khung đầy đủ — mọi field tồn tại; null khi N/A theo rule_kind/condition_kind.
const base = {
  rule_kind: 0,
  condition_kind: null,
  alias_metric: null,
  path_alias_id: null,
  entity_keys: [],
  entity_filter: null,
  scope_device_ids: null,
  comparator: null,
  threshold: null,
  pct_abs: 0,
  no_data_seconds: null,
  logical_op: null,
  child_rule_ids: null,
  missing_as: 0,
  sustain_samples: 1,
  dedup_seconds: 1800,
  emit_independent: 1,
  status: 1,
  created_by: "operator",
};

export const ALERT_RULES_DATA = [
  // ---------- basic / threshold (condition_kind=0) ----------
  {
    ...base,
    id: 701,
    rule_name: "Interface in-errors cao",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "if_in_errors",
    path_alias_id: null,
    entity_keys: ["if_name"],
    severity: 2, // error
    comparator: 0, // >
    threshold: 100,
    sustain_samples: 2,
    dedup_seconds: 1800,
    created_at: ts(4000),
    updated_at: ts(120),
  },
  {
    ...base,
    id: 702,
    rule_name: "Interface in-discards cảnh báo",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "if_in_discards",
    path_alias_id: null,
    entity_keys: ["if_name"],
    severity: 1, // warning
    comparator: 1, // >=
    threshold: 50,
    sustain_samples: 3,
    dedup_seconds: 1800,
    created_at: ts(3900),
    updated_at: ts(240),
  },
  {
    ...base,
    id: 706,
    rule_name: "CPU quá tải nghiêm trọng",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "cpu_utilization_5min",
    path_alias_id: null,
    entity_keys: [], // device-level
    severity: 3, // critical
    comparator: 0, // >
    threshold: 90,
    sustain_samples: 2,
    dedup_seconds: 900,
    created_at: ts(3600),
    updated_at: ts(60),
  },
  {
    ...base,
    id: 707,
    rule_name: "Bộ nhớ dùng vượt 8GB",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "memory_used_bytes",
    path_alias_id: null,
    entity_keys: [],
    severity: 2,
    comparator: 0,
    threshold: 8000000000,
    sustain_samples: 1,
    dedup_seconds: 1800,
    created_at: ts(3500),
    updated_at: ts(800),
  },
  {
    ...base,
    id: 708,
    rule_name: "BGP rời trạng thái Established",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "bgp_state",
    path_alias_id: 13, // xr_bgp_neighbor
    entity_keys: ["neighbor_address"],
    severity: 3,
    comparator: 5, // !=
    threshold: 6, // 6 = Established
    sustain_samples: 1,
    dedup_seconds: 600,
    created_at: ts(3400),
    updated_at: ts(500),
  },
  {
    ...base,
    id: 710,
    rule_name: "CPU avg 5m (derived) cao",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "cpu_util_avg_5m", // derived metric (Job 2)
    path_alias_id: 0, // 0 = derived metric
    entity_keys: [],
    severity: 2,
    comparator: 0,
    threshold: 85,
    sustain_samples: 1,
    dedup_seconds: 1800,
    created_at: ts(3000),
    updated_at: ts(300),
  },
  {
    ...base,
    id: 711,
    rule_name: "Băng thông vào (derived Mbps) cao",
    rule_kind: 0,
    condition_kind: 0,
    alias_metric: "if_in_mbps", // derived metric
    path_alias_id: 0,
    entity_keys: ["if_name"],
    severity: 1,
    comparator: 2, // <  (ví dụ comparator khác)
    threshold: 8000,
    pct_abs: 0,
    sustain_samples: 1,
    dedup_seconds: 1200,
    status: 0, // Disabled — test filter trạng thái
    created_at: ts(2800),
    updated_at: ts(2000),
  },

  // ---------- basic / pct_change_prev (condition_kind=1) ----------
  {
    ...base,
    id: 703,
    rule_name: "Octet vào tăng đột biến (%)",
    rule_kind: 0,
    condition_kind: 1,
    alias_metric: "if_in_octets",
    path_alias_id: null,
    entity_keys: ["if_name"],
    entity_filter: { if_name: ["GigabitEthernet0/0/0/0", "GigabitEthernet0/0/0/1"] },
    severity: 1,
    comparator: 0, // >
    threshold: 50, // %
    pct_abs: 1, // dùng |%|
    sustain_samples: 1,
    dedup_seconds: 1800,
    created_at: ts(3300),
    updated_at: ts(180),
  },

  // ---------- basic / no_data (condition_kind=2) ----------
  {
    ...base,
    id: 704,
    rule_name: "CPU ngừng báo (no_data)",
    rule_kind: 0,
    condition_kind: 2,
    alias_metric: "cpu_utilization_5min",
    path_alias_id: null,
    entity_keys: [], // device-level
    severity: 2,
    comparator: null,
    threshold: null,
    no_data_seconds: 300,
    sustain_samples: 1,
    dedup_seconds: 0,
    created_at: ts(3200),
    updated_at: ts(150),
  },
  {
    ...base,
    id: 709,
    rule_name: "Interface ngừng báo counter (no_data)",
    rule_kind: 0,
    condition_kind: 2,
    alias_metric: "if_in_octets",
    path_alias_id: null,
    entity_keys: ["if_name"],
    severity: 1,
    no_data_seconds: 180,
    sustain_samples: 1,
    dedup_seconds: 0,
    created_at: ts(3100),
    updated_at: ts(400),
  },

  // ---------- basic / abs_delta_prev (condition_kind=3) ----------
  {
    ...base,
    id: 705,
    rule_name: "Octet vào nhảy bậc (abs delta)",
    rule_kind: 0,
    condition_kind: 3,
    alias_metric: "if_in_octets",
    path_alias_id: null,
    entity_keys: ["if_name"],
    severity: 1,
    comparator: 0, // >
    threshold: 1000000000,
    pct_abs: 1,
    sustain_samples: 2,
    dedup_seconds: 1800,
    scope_device_ids: [1, 2],
    created_at: ts(3000),
    updated_at: ts(220),
  },

  // ---------- composite (rule_kind=1) ----------
  {
    ...base,
    id: 720,
    rule_name: "Interface lỗi + discard đồng thời (AND)",
    rule_kind: 1,
    condition_kind: null,
    alias_metric: null,
    entity_keys: ["if_name"], // mọi child cùng entity_keys
    severity: 2,
    logical_op: 0, // AND
    child_rule_ids: [701, 702],
    missing_as: 0, // false
    sustain_samples: 1,
    dedup_seconds: 1800,
    created_at: ts(2600),
    updated_at: ts(90),
  },
  {
    ...base,
    id: 721,
    rule_name: "Octet bất thường (% HOẶC delta) (OR)",
    rule_kind: 1,
    entity_keys: ["if_name"],
    severity: 1,
    logical_op: 1, // OR
    child_rule_ids: [703, 705],
    missing_as: 1, // skip
    sustain_samples: 1,
    dedup_seconds: 1800,
    created_at: ts(2500),
    updated_at: ts(160),
  },
  {
    ...base,
    id: 722,
    rule_name: "CPU + Memory cùng cao (AND, device)",
    rule_kind: 1,
    entity_keys: [], // device-level
    severity: 3,
    logical_op: 0, // AND
    child_rule_ids: [706, 707],
    missing_as: 0,
    sustain_samples: 1,
    dedup_seconds: 900,
    created_at: ts(2400),
    updated_at: ts(70),
  },
];
