// ============================================================
// Mock data — Alert State (tlm_alert_state mirror) — Phase 3 / AL-09, AL-10
// ------------------------------------------------------------
// Job 3 upsert khi state đổi; dashboard đọc (read-only ở FE).
// { rule_id, device_id, entity_fingerprint, last_state (FIRING|RESOLVED),
//   last_fired_at, last_resolved_at, dedup_remaining_seconds }
// entity_fingerprint: "k=v" (vd "if_name=Gi0/0/0/0") | "" device-level
// ============================================================

const ts = (mins) =>
  new Date(new Date("2026-06-06T09:00:00+07:00").getTime() - mins * 60000).toISOString();

export const ALERT_STATE_DATA = [
  // ---- FIRING (đang có sự cố) ----
  {
    rule_id: 701, // Interface in-errors cao
    device_id: 1,
    entity_fingerprint: "if_name=GigabitEthernet0/0/0/0",
    last_state: "FIRING",
    last_fired_at: ts(20),
    last_resolved_at: null,
    dedup_remaining_seconds: 1680, // 1800 - 120
  },
  {
    rule_id: 701,
    device_id: 3,
    entity_fingerprint: "if_name=ge-0/0/3",
    last_state: "FIRING",
    last_fired_at: ts(8),
    last_resolved_at: ts(2000),
    dedup_remaining_seconds: 1320,
  },
  {
    rule_id: 706, // CPU critical
    device_id: 2,
    entity_fingerprint: "",
    last_state: "FIRING",
    last_fired_at: ts(35),
    last_resolved_at: null,
    dedup_remaining_seconds: 0, // 900 dedup đã hết → eligible refire
  },
  {
    rule_id: 704, // no_data CPU
    device_id: 5,
    entity_fingerprint: "",
    last_state: "FIRING",
    last_fired_at: ts(12),
    last_resolved_at: null,
    dedup_remaining_seconds: 0, // no_data: dedup=0
  },
  {
    rule_id: 720, // composite AND
    device_id: 1,
    entity_fingerprint: "if_name=GigabitEthernet0/0/0/0",
    last_state: "FIRING",
    last_fired_at: ts(15),
    last_resolved_at: null,
    dedup_remaining_seconds: 990,
  },
  {
    rule_id: 708, // BGP not established
    device_id: 1,
    entity_fingerprint: "neighbor_address=10.255.0.2",
    last_state: "FIRING",
    last_fired_at: ts(5),
    last_resolved_at: null,
    dedup_remaining_seconds: 300,
  },

  // ---- RESOLVED (đã hết) ----
  {
    rule_id: 702,
    device_id: 4,
    entity_fingerprint: "if_name=ge-0/0/7",
    last_state: "RESOLVED",
    last_fired_at: ts(600),
    last_resolved_at: ts(180),
    dedup_remaining_seconds: 0,
  },
  {
    rule_id: 707,
    device_id: 5,
    entity_fingerprint: "",
    last_state: "RESOLVED",
    last_fired_at: ts(900),
    last_resolved_at: ts(420),
    dedup_remaining_seconds: 0,
  },
  {
    rule_id: 722,
    device_id: 2,
    entity_fingerprint: "",
    last_state: "RESOLVED",
    last_fired_at: ts(1200),
    last_resolved_at: ts(700),
    dedup_remaining_seconds: 0,
  },
];
