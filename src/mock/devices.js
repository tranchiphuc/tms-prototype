// Thiết bị mẫu — dùng nhất quán toàn app (CLAUDE.md §6)
export const DEVICES = [
  { id: 1, name: "R-HCM-001", vendor: "Cisco", model: "ASR9001", ip: "10.1.1.1" },
  { id: 2, name: "R-HCM-002", vendor: "Cisco", model: "NCS5500", ip: "10.1.1.2" },
  { id: 3, name: "R-HN-001", vendor: "Juniper", model: "MX480", ip: "10.2.1.1" },
  { id: 4, name: "R-HN-002", vendor: "Juniper", model: "PTX5000", ip: "10.2.1.2" },
  { id: 5, name: "R-DN-001", vendor: "Nokia", model: "SR7750", ip: "10.3.1.1" },
];

// Model Code values (UI label; API field name vẫn là vendor_code) — CLAUDE.md §4.1
// LƯU Ý: bỏ "All" theo yêu cầu (coi là giá trị không hợp lệ) — chỉ còn 4 data model.
export const MODEL_CODE_VALUES = ["Cisco", "Juniper", "Nokia", "OpenConfig"];

export const YANG_PATHS = {
  cisco:
    "Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters",
  juniper: "interfaces/interface/state/counters",
  nokia: "state/port",
  openconfig: "openconfig-interfaces:interfaces/interface/state/counters",
};

export const METRIC_ALIASES = [
  "if_in_octets",
  "if_out_octets",
  "if_in_errors",
  "if_out_errors",
  "if_in_discards",
  "if_out_discards",
  "if_in_ucast_pkts",
  "if_out_ucast_pkts",
  "cpu_utilization_5min",
  "memory_used_bytes",
  "memory_free_bytes",
  "bgp_prefixes_received",
  "bgp_prefixes_sent",
  "bgp_state",
];
