import { isoMinutesAgo } from "./helpers";

// tlm_filter_rules — CLAUDE.md §4.2
// filter_action: 0=EXCLUDE_IF_MATCH(drop khi TRUE), 1=INCLUDE_IF_MATCH(chỉ giữ khi TRUE)
// CẢNH BÁO: EXCLUDE drop metric → KHÔNG ghi ClickHouse, KHÔNG phục hồi
const r = (o) => ({ created_by: "operator", ...o });

export const FILTER_RULES_DATA = [
  r({
    id: 301,
    vendor_code: "OpenConfig",
    match_path: "",
    match_metric: "if_in_discards",
    filter_expression: "value == 0",
    filter_action: 0, // EXCLUDE: drop discard=0 (noise)
    priority: 50,
    status: 1,
    pushed_at: isoMinutesAgo(300),
    created_at: isoMinutesAgo(5000),
    updated_at: isoMinutesAgo(310),
  }),
  r({
    id: 302,
    vendor_code: "Cisco",
    match_path: "xr_watchdog_memory",
    match_metric: null,
    filter_expression: "value < 1000",
    filter_action: 0, // EXCLUDE
    priority: 100,
    status: 1,
    pushed_at: isoMinutesAgo(150),
    created_at: isoMinutesAgo(4500),
    updated_at: isoMinutesAgo(155),
  }),
  r({
    id: 303,
    vendor_code: "Nokia",
    match_path: "",
    match_metric: "cpu_utilization_5min",
    filter_expression: "value > 0",
    filter_action: 1, // INCLUDE: chỉ giữ cpu > 0
    priority: 50,
    status: 1,
    pushed_at: isoMinutesAgo(400),
    created_at: isoMinutesAgo(5500),
    updated_at: isoMinutesAgo(405),
  }),
  r({
    id: 304,
    vendor_code: "Juniper",
    match_path: "junos_if_counters",
    match_metric: "if_in_errors",
    filter_expression: "",
    filter_action: 1, // INCLUDE unconditional (luôn giữ)
    priority: 100,
    status: 1,
    pushed_at: isoMinutesAgo(200),
    created_at: isoMinutesAgo(5000),
    updated_at: isoMinutesAgo(205),
  }),
  r({
    id: 305,
    vendor_code: "Nokia",
    match_path: "nokia_port_stats",
    match_metric: null,
    filter_expression: "value == 0",
    filter_action: 0, // EXCLUDE
    priority: 100,
    status: 0,
    pushed_at: isoMinutesAgo(1000),
    created_at: isoMinutesAgo(6000),
    updated_at: isoMinutesAgo(500),
  }),
  r({
    id: 306,
    vendor_code: "OpenConfig",
    match_path: "",
    match_metric: null,
    filter_expression: "metric_name LIKE '%_debug'",
    filter_action: 0, // EXCLUDE rộng — ví dụ rule nguy hiểm
    priority: 30,
    status: 1,
    pushed_at: null,
    created_at: isoMinutesAgo(40),
    updated_at: isoMinutesAgo(18),
  }),
];
