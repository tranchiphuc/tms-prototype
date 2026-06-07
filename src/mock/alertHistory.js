// ============================================================
// Mock data — Alert History (ipms.alert_history) — Phase 3 / AL-11
// ------------------------------------------------------------
// ClickHouse audit, TTL 90 ngày. event_type: FIRED | REFIRED | RESOLVED
// LƯU Ý: với no_data (704/709), REFIRED KHÔNG xuất hiện ở chế độ mặc định
//   (HLD v1.4 §6.5 / AL-04). Chỉ FIRED rồi RESOLVED.
// { timestamp, rule_id, rule_name, rule_kind, severity, condition_kind,
//   device_id, device_name, entity_fingerprint, trigger_value, threshold, event_type }
// ============================================================

const ts = (mins) =>
  new Date(new Date("2026-06-06T09:00:00+07:00").getTime() - mins * 60000).toISOString();

const E = (mins, rule_id, rule_name, severity, device_id, device_name, fp, value, threshold, event_type, extra = {}) => ({
  timestamp: ts(mins),
  rule_id,
  rule_name,
  rule_kind: extra.rule_kind ?? 0,
  condition_kind: extra.condition_kind ?? 0,
  severity,
  device_id,
  device_name,
  entity_fingerprint: fp,
  trigger_value: value,
  threshold,
  event_type,
});

// 24h gần nhất (mins ≤ 1440). Sắp xếp ngược thời gian khi GET.
export const ALERT_HISTORY_DATA = [
  // 701 — in-errors, device 1 Gi0/0/0/0: FIRED → REFIRED → vẫn FIRING
  E(1380, 701, "Interface in-errors cao", 2, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/0", 150, 100, "FIRED"),
  E(420, 701, "Interface in-errors cao", 2, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/0", 220, 100, "REFIRED"),
  E(20, 701, "Interface in-errors cao", 2, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/0", 310, 100, "REFIRED"),
  // 701 — device 3 ge-0/0/3
  E(2000, 701, "Interface in-errors cao", 2, 3, "R-HN-001", "if_name=ge-0/0/3", 180, 100, "FIRED"),
  E(1100, 701, "Interface in-errors cao", 2, 3, "R-HN-001", "if_name=ge-0/0/3", 40, 100, "RESOLVED"),
  E(8, 701, "Interface in-errors cao", 2, 3, "R-HN-001", "if_name=ge-0/0/3", 205, 100, "FIRED"),

  // 702 — in-discards warning, device 4: FIRED → RESOLVED
  E(600, 702, "Interface in-discards cảnh báo", 1, 4, "R-HN-002", "if_name=ge-0/0/7", 75, 50, "FIRED", { condition_kind: 0 }),
  E(180, 702, "Interface in-discards cảnh báo", 1, 4, "R-HN-002", "if_name=ge-0/0/7", 12, 50, "RESOLVED", { condition_kind: 0 }),

  // 706 — CPU critical, device 2: FIRED → REFIRED (dedup hết)
  E(900, 706, "CPU quá tải nghiêm trọng", 3, 2, "R-HCM-002", "", 94, 90, "FIRED"),
  E(35, 706, "CPU quá tải nghiêm trọng", 3, 2, "R-HCM-002", "", 97, 90, "REFIRED"),

  // 707 — memory, device 5: FIRED → RESOLVED
  E(900, 707, "Bộ nhớ dùng vượt 8GB", 2, 5, "R-DN-001", "", 8600000000, 8000000000, "FIRED"),
  E(420, 707, "Bộ nhớ dùng vượt 8GB", 2, 5, "R-DN-001", "", 6200000000, 8000000000, "RESOLVED"),

  // 703 — pct_change, device 2 Gi0/0/0/1: FIRED → RESOLVED
  E(700, 703, "Octet vào tăng đột biến (%)", 1, 2, "R-HCM-002", "if_name=GigabitEthernet0/0/0/1", 62, 50, "FIRED", { condition_kind: 1 }),
  E(500, 703, "Octet vào tăng đột biến (%)", 1, 2, "R-HCM-002", "if_name=GigabitEthernet0/0/0/1", 8, 50, "RESOLVED", { condition_kind: 1 }),

  // 704 — no_data CPU, device 5: FIRED rồi (đợt này vẫn firing). KHÔNG có REFIRED.
  E(1300, 704, "CPU ngừng báo (no_data)", 2, 5, "R-DN-001", "", 300, null, "FIRED", { condition_kind: 2 }),
  E(800, 704, "CPU ngừng báo (no_data)", 2, 5, "R-DN-001", "", 300, null, "RESOLVED", { condition_kind: 2 }),
  E(12, 704, "CPU ngừng báo (no_data)", 2, 5, "R-DN-001", "", 305, null, "FIRED", { condition_kind: 2 }),

  // 709 — no_data interface, device 3: FIRED → RESOLVED (resolve-on-return)
  E(300, 709, "Interface ngừng báo counter (no_data)", 1, 3, "R-HN-001", "if_name=ge-0/0/1", 185, null, "FIRED", { condition_kind: 2 }),
  E(240, 709, "Interface ngừng báo counter (no_data)", 1, 3, "R-HN-001", "if_name=ge-0/0/1", 185, null, "RESOLVED", { condition_kind: 2 }),

  // 705 — abs_delta, device 1: FIRED → RESOLVED
  E(1000, 705, "Octet vào nhảy bậc (abs delta)", 1, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/2", 1500000000, 1000000000, "FIRED", { condition_kind: 3 }),
  E(650, 705, "Octet vào nhảy bậc (abs delta)", 1, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/2", 200000000, 1000000000, "RESOLVED", { condition_kind: 3 }),

  // 708 — BGP not established, device 1: FIRED (đang firing)
  E(5, 708, "BGP rời trạng thái Established", 3, 1, "R-HCM-001", "neighbor_address=10.255.0.2", 3, 6, "FIRED"),

  // 720 — composite AND, device 1: FIRED
  E(15, 720, "Interface lỗi + discard đồng thời (AND)", 2, 1, "R-HCM-001", "if_name=GigabitEthernet0/0/0/0", 1, null, "FIRED", { rule_kind: 1, condition_kind: null }),

  // 722 — composite AND device-level, device 2: FIRED → RESOLVED
  E(1200, 722, "CPU + Memory cùng cao (AND, device)", 3, 2, "R-HCM-002", "", 1, null, "FIRED", { rule_kind: 1, condition_kind: null }),
  E(700, 722, "CPU + Memory cùng cao (AND, device)", 3, 2, "R-HCM-002", "", 0, null, "RESOLVED", { rule_kind: 1, condition_kind: null }),
];
