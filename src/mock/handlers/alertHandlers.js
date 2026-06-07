import { registerHandler } from "../../services/mockApi";
import { applyFilters, applySort, paginate, httpError, nextId, isoMinutesAgo } from "../helpers";
import { ALERT_RULES_DATA } from "../alertRules";
import { ALERT_STATE_DATA } from "../alertState";
import { ALERT_HISTORY_DATA } from "../alertHistory";
import { DEVICES } from "../devices";

// ---- In-memory stores ----
let store = [...ALERT_RULES_DATA];
let stateStore = [...ALERT_STATE_DATA];
let historyStore = [...ALERT_HISTORY_DATA];

const SEARCH_FIELDS = ["rule_name"];
const COMPARATORS = [0, 1, 2, 3, 4, 5];
const CONDITION_KINDS = [0, 1, 2, 3];

const findRow = (id) => store.find((r) => r.id === Number(id));
const replaceRow = (row) => {
  const idx = store.findIndex((r) => r.id === row.id);
  if (idx >= 0) store[idx] = row;
};
const deviceName = (id) => (DEVICES.find((d) => d.id === Number(id)) || {}).name || `device#${id}`;

// composite Active đang tham chiếu basic `childId`
const activeCompositesReferencing = (childId, exceptId = null) =>
  store.filter(
    (r) =>
      r.id !== exceptId &&
      Number(r.rule_kind) === 1 &&
      Number(r.status) === 1 &&
      Array.isArray(r.child_rule_ids) &&
      r.child_rule_ids.includes(Number(childId))
  );

// so sánh hai entity_keys (mảng) — không phụ thuộc thứ tự
const sameEntityKeys = (a = [], b = []) => {
  const sa = [...a].sort();
  const sb = [...b].sort();
  return sa.length === sb.length && sa.every((k, i) => k === sb[i]);
};

// ---- Validation (AL-02..06) ----
const validate = (body, selfId) => {
  if (!body.rule_name) httpError(422, "rule_name bắt buộc");
  const kind = Number(body.rule_kind);
  if (![0, 1].includes(kind)) httpError(422, "rule_kind ∈ {0=basic, 1=composite}");
  if (![0, 1, 2, 3].includes(Number(body.severity))) {
    httpError(422, "severity ∈ {0=info,1=warning,2=error,3=critical}");
  }
  if (body.sustain_samples != null && Number(body.sustain_samples) < 1) {
    httpError(422, "sustain_samples phải ≥ 1");
  }
  if (body.dedup_seconds != null && Number(body.dedup_seconds) < 0) {
    httpError(422, "dedup_seconds phải ≥ 0");
  }

  if (kind === 0) {
    validateBasic(body);
  } else {
    validateComposite(body, selfId);
  }
};

const validateBasic = (body) => {
  const ck = Number(body.condition_kind);
  if (!CONDITION_KINDS.includes(ck)) {
    httpError(422, "condition_kind ∈ {0=threshold,1=pct_change_prev,2=no_data,3=abs_delta_prev}");
  }
  if (!body.alias_metric) httpError(422, "alias_metric bắt buộc cho basic rule");

  if (ck === 2) {
    // no_data — comparator/threshold PHẢI VẮNG; no_data_seconds > 0
    if (body.comparator != null || body.threshold != null) {
      httpError(422, "no_data: comparator và threshold phải VẮNG", [
        { field: "comparator", reason: "không dùng cho no_data" },
      ]);
    }
    if (body.no_data_seconds == null || Number(body.no_data_seconds) <= 0) {
      httpError(422, "no_data: no_data_seconds bắt buộc và > 0");
    }
  } else {
    // threshold / pct / abs_delta — cần comparator + threshold; no_data_seconds VẮNG
    if (!COMPARATORS.includes(Number(body.comparator))) {
      httpError(422, "comparator ∈ {0=>,1=>=,2=<,3=<=,4===,5=!=} bắt buộc");
    }
    if (body.threshold == null || body.threshold === "") {
      httpError(422, "threshold bắt buộc cho threshold/pct/abs_delta");
    }
    if (body.no_data_seconds != null) {
      httpError(422, "no_data_seconds chỉ dùng cho condition_kind=2");
    }
  }
};

const validateComposite = (body, selfId) => {
  if (![0, 1].includes(Number(body.logical_op))) httpError(422, "logical_op ∈ {0=AND,1=OR}");
  const children = body.child_rule_ids;
  if (!Array.isArray(children) || children.length === 0) {
    httpError(422, "composite cần child_rule_ids (≥1 basic rule)");
  }
  if (new Set(children).size !== children.length) {
    httpError(422, "child_rule_ids không được trùng lặp");
  }
  if (selfId != null && children.includes(Number(selfId))) {
    httpError(422, "composite không được tự tham chiếu");
  }
  const entityKeys = body.entity_keys || [];
  for (const cid of children) {
    const child = findRow(cid);
    if (!child) httpError(422, `child rule #${cid} không tồn tại`);
    if (Number(child.rule_kind) !== 0) {
      httpError(422, `child #${cid} phải là basic (không được lồng composite)`);
    }
    if (Number(child.status) !== 1) httpError(422, `child #${cid} phải đang Active (status=1)`);
    if (!sameEntityKeys(child.entity_keys, entityKeys)) {
      httpError(422, `child #${cid} phải cùng entity_keys với composite`, [
        { field: "child_rule_ids", reason: `entity_keys của #${cid} khác composite` },
      ]);
    }
  }
};

// ---- Chuẩn hoá body → row (dọn field không liên quan về null) ----
const normalize = (body) => {
  const kind = Number(body.rule_kind);
  const out = {
    rule_name: body.rule_name,
    rule_kind: kind,
    severity: Number(body.severity),
    entity_keys: Array.isArray(body.entity_keys) ? body.entity_keys : [],
    entity_filter:
      body.entity_filter && Object.keys(body.entity_filter).length ? body.entity_filter : null,
    scope_device_ids:
      Array.isArray(body.scope_device_ids) && body.scope_device_ids.length
        ? body.scope_device_ids.map(Number)
        : null,
    sustain_samples: body.sustain_samples == null ? 1 : Number(body.sustain_samples),
    dedup_seconds: body.dedup_seconds == null ? 0 : Number(body.dedup_seconds),
    emit_independent: body.emit_independent == null ? 1 : Number(body.emit_independent),
    status: body.status == null ? 1 : Number(body.status),
    // basic
    condition_kind: null,
    alias_metric: null,
    path_alias_id: null,
    comparator: null,
    threshold: null,
    pct_abs: 0,
    no_data_seconds: null,
    // composite
    logical_op: null,
    child_rule_ids: null,
    missing_as: 0,
  };
  if (kind === 0) {
    const ck = Number(body.condition_kind);
    out.condition_kind = ck;
    out.alias_metric = body.alias_metric;
    out.path_alias_id = body.path_alias_id == null ? null : Number(body.path_alias_id);
    if (ck === 2) {
      out.no_data_seconds = Number(body.no_data_seconds);
    } else {
      out.comparator = Number(body.comparator);
      out.threshold = Number(body.threshold);
      if (ck === 1 || ck === 3) out.pct_abs = Number(body.pct_abs) ? 1 : 0;
    }
  } else {
    out.logical_op = Number(body.logical_op);
    out.child_rule_ids = body.child_rule_ids.map(Number);
    out.missing_as = Number(body.missing_as) ? 1 : 0;
  }
  return out;
};

// tlm_alert_rules KHÔNG có pushed_at — chỉ đẩy updated_at
const touchRule = (row) => ({ ...row, updated_at: new Date().toISOString() });

// ---- Preview (AL-07) — boolean tức thời, KHÔNG mô phỏng sustain/dedup ----
const cmp = (a, op, b) => {
  switch (Number(op)) {
    case 0: return a > b;
    case 1: return a >= b;
    case 2: return a < b;
    case 3: return a <= b;
    case 4: return a === b;
    case 5: return a !== b;
    default: return false;
  }
};

const buildPreview = (body) => {
  const kind = Number(body.rule_kind);
  if (kind === 1) {
    return {
      note: "Composite preview: dry-run đánh giá boolean tức thời của children (không sustain/dedup).",
      evaluated_series: 0,
      matching_series: 0,
      skipped_series: 0,
      samples: [],
      warnings: ["Preview composite chỉ minh hoạ — cần children được lưu để gộp boolean theo entity."],
    };
  }
  const ck = Number(body.condition_kind);
  const entityKeys = body.entity_keys || [];
  const scope = Array.isArray(body.scope_device_ids) && body.scope_device_ids.length
    ? DEVICES.filter((d) => body.scope_device_ids.includes(d.id))
    : DEVICES;

  const samples = [];
  let matching = 0;
  let skipped = 0;
  scope.forEach((d, di) => {
    // mỗi device sinh 1-2 series mẫu (entity)
    const entities = entityKeys.length ? [`if_name=Gi0/0/0/${di}`, `if_name=Gi0/0/0/${di + 5}`] : [""];
    entities.forEach((fp, ei) => {
      // giá trị pseudo tất định theo (device, entity)
      const cur = 40 + di * 37 + ei * 53;
      const prev = 30 + di * 29 + ei * 41;
      let would = false;
      let observed = cur;
      if (ck === 2) {
        // no_data — preview chỉ cho biết series "known"
        skipped += 1;
        samples.push({
          device_id: d.id,
          device_name: d.name,
          entity: fp,
          cur_value: cur,
          prev_value: null,
          would_fire: false,
          note: "no_data đánh giá bằng timer — preview không mô phỏng im lặng",
        });
        return;
      }
      if (ck === 0) {
        observed = cur;
        would = cmp(cur, body.comparator, Number(body.threshold));
      } else if (ck === 1) {
        if (prev === 0) {
          skipped += 1;
          samples.push({ device_id: d.id, device_name: d.name, entity: fp, cur_value: cur, prev_value: prev, would_fire: false, note: "prev=0 → skip (chia 0)" });
          return;
        }
        let p = ((cur - prev) / prev) * 100;
        if (Number(body.pct_abs)) p = Math.abs(p);
        observed = Number(p.toFixed(2));
        would = cmp(observed, body.comparator, Number(body.threshold));
      } else if (ck === 3) {
        let dd = cur - prev;
        if (Number(body.pct_abs)) dd = Math.abs(dd);
        observed = dd;
        would = cmp(dd, body.comparator, Number(body.threshold));
      }
      if (would) matching += 1;
      samples.push({
        device_id: d.id,
        device_name: d.name,
        entity: fp,
        cur_value: cur,
        prev_value: ck === 0 ? null : prev,
        observed_value: observed,
        would_fire: would,
      });
    });
  });

  return {
    note: "Preview phản ánh boolean tức thời, KHÔNG mô phỏng sustain/dedup theo thời gian (AL-07).",
    evaluated_series: samples.length,
    matching_series: matching,
    skipped_series: skipped,
    samples,
    warnings: ck === 1 || ck === 3 ? ["pct/abs_delta cần ≥2 sample (~2 chu kỳ ≈120s) để có prev."] : [],
  };
};

// ---- Engine status (AL-13) — sức khoẻ Flink Job 3 ----
const buildEngineStatus = () => {
  const activeRules = store.filter((r) => Number(r.status) === 1).length;
  const firesLastHour = historyStore.filter(
    (h) => h.event_type !== "RESOLVED" && new Date(h.timestamp).getTime() >= new Date(isoMinutesAgo(60)).getTime()
  ).length;
  return {
    job_name: "alert-evaluator-job3",
    status: "Running",
    last_checkpoint_at: isoMinutesAgo(1),
    consumer_lag: { processed_metrics: 42, derived_metrics: 7 },
    watermark_lag_ms: 1200,
    eval_latency_ms: 180,
    active_rules_count: activeRules,
    fires_last_hour: firesLastHour,
    slo_target_seconds: 5,
    slo_ok: true,
    note: "AL-13: phản ánh sức khoẻ Flink job (không còn poll interval). Cảnh báo nếu eval latency/lag đe doạ SLO ≤ 5s.",
  };
};

// ============================================================
// Đăng ký handlers — route cụ thể TRƯỚC route có {id}
// ============================================================
registerHandler(/^\/alerts\/rules\/preview$/, (ctx) => ({
  status: 200,
  data: buildPreview(ctx.data || {}),
}));

// AL-10 — firing state per-instance của một rule
registerHandler(/^\/alerts\/rules\/(\d+)\/state$/, (ctx) => {
  const rid = Number(ctx.match[1]);
  if (!findRow(rid)) httpError(404, "Không tìm thấy rule");
  const instances = stateStore
    .filter((s) => s.rule_id === rid)
    .map((s) => ({
      ...s,
      device_name: deviceName(s.device_id),
    }));
  return { status: 200, data: { rule_id: rid, instances } };
});

// AL-09 — danh sách alert đang FIRING
registerHandler(/^\/alerts\/active$/, (ctx) => {
  const p = ctx.params || {};
  let rows = stateStore.filter((s) => s.last_state === "FIRING");
  if (p.severity !== undefined && p.severity !== "") {
    rows = rows.filter((s) => Number((findRow(s.rule_id) || {}).severity) === Number(p.severity));
  }
  if (p.device_id !== undefined && p.device_id !== "") {
    rows = rows.filter((s) => Number(s.device_id) === Number(p.device_id));
  }
  const items = rows
    .map((s) => {
      const rule = findRow(s.rule_id) || {};
      // giá trị trigger gần nhất từ history (nếu có)
      const last = historyStore
        .filter((h) => h.rule_id === s.rule_id && h.device_id === s.device_id && h.entity_fingerprint === s.entity_fingerprint && h.event_type !== "RESOLVED")
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
      return {
        rule_id: s.rule_id,
        rule_name: rule.rule_name,
        rule_kind: rule.rule_kind,
        condition_kind: rule.condition_kind,
        severity: rule.severity,
        device_id: s.device_id,
        device_name: deviceName(s.device_id),
        entity_fingerprint: s.entity_fingerprint,
        fired_at: s.last_fired_at,
        trigger_value: last ? last.trigger_value : null,
        threshold: rule.threshold ?? null,
        dedup_remaining_seconds: s.dedup_remaining_seconds,
        dedup_state: s.dedup_remaining_seconds > 0 ? "SUPPRESSED" : "FIRING",
      };
    })
    .sort((a, b) => new Date(b.fired_at) - new Date(a.fired_at));
  return { status: 200, data: { items, total: items.length } };
});

// AL-11 — alert history (mặc định 24h)
registerHandler(/^\/alerts\/history$/, (ctx) => {
  const p = ctx.params || {};
  const fromMin = p.from_minutes !== undefined && p.from_minutes !== "" ? Number(p.from_minutes) : 1440;
  const cutoff = new Date(isoMinutesAgo(fromMin)).getTime();
  let rows = historyStore.filter((h) => new Date(h.timestamp).getTime() >= cutoff);
  if (p.rule_id !== undefined && p.rule_id !== "") rows = rows.filter((h) => h.rule_id === Number(p.rule_id));
  if (p.device_id !== undefined && p.device_id !== "") rows = rows.filter((h) => h.device_id === Number(p.device_id));
  if (p.severity !== undefined && p.severity !== "") rows = rows.filter((h) => Number(h.severity) === Number(p.severity));
  if (p.event_type) rows = rows.filter((h) => h.event_type === p.event_type);
  if (p.q) {
    const q = String(p.q).toLowerCase();
    rows = rows.filter((h) => String(h.rule_name || "").toLowerCase().includes(q));
  }
  const sorted = [...rows].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return { status: 200, data: paginate(sorted, p) };
});

// AL-13 — engine status
registerHandler(/^\/alerts\/engine\/status$/, () => ({ status: 200, data: buildEngineStatus() }));

// refresh-status (hỗ trợ vận hành)
registerHandler(/^\/alerts\/refresh-status$/, () => {
  const max = store.reduce((m, r) => (r.updated_at > m ? r.updated_at : m), "");
  return {
    status: 200,
    data: {
      table: "tlm_alert_rules",
      max_updated_at: max,
      active_rules: store.filter((r) => Number(r.status) === 1).length,
      note: "Job 3 broadcast refresh rule snapshot runtime (không có pushed_at).",
    },
  };
});

// CRUD rules — /alerts/rules[/{id}]
registerHandler(/^\/alerts\/rules(?:\/(\d+))?$/, (ctx) => {
  const id = ctx.match[1];
  const method = ctx.method;

  if (!id) {
    if (method === "get") {
      let rows = applyFilters(store, ctx.params, SEARCH_FIELDS);
      const p = ctx.params || {};
      if (p.severity !== undefined && p.severity !== "") {
        rows = rows.filter((r) => Number(r.severity) === Number(p.severity));
      }
      if (p.rule_kind !== undefined && p.rule_kind !== "") {
        rows = rows.filter((r) => Number(r.rule_kind) === Number(p.rule_kind));
      }
      if (p.condition_kind !== undefined && p.condition_kind !== "") {
        rows = rows.filter((r) => Number(r.condition_kind) === Number(p.condition_kind));
      }
      const sorted = applySort(rows, p.sort || "-updated_at");
      return { status: 200, data: paginate(sorted, p) };
    }
    if (method === "post") {
      const body = ctx.data || {};
      validate(body, null);
      const row = {
        ...normalize(body),
        id: nextId(),
        created_by: "operator",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      store.unshift(row);
      return { status: 201, data: row };
    }
  } else {
    const row = findRow(id);
    if (!row) httpError(404, "Không tìm thấy alert rule");

    if (method === "get") {
      // composite → expand children[]
      if (Number(row.rule_kind) === 1 && Array.isArray(row.child_rule_ids)) {
        const children = row.child_rule_ids.map((cid) => findRow(cid)).filter(Boolean);
        return { status: 200, data: { ...row, children } };
      }
      return { status: 200, data: row };
    }

    if (method === "put") {
      const body = ctx.data || {};
      validate(body, row.id);
      const updated = touchRule({
        ...normalize(body),
        id: row.id,
        created_at: row.created_at,
        created_by: row.created_by,
      });
      replaceRow(updated);
      return { status: 200, data: updated };
    }

    if (method === "patch") {
      // AL-08: {status:0} hoặc {threshold:90} ...
      const body = ctx.data || {};
      const updated = touchRule({ ...row, ...body, id: row.id });
      replaceRow(updated);
      return { status: 200, data: updated };
    }

    if (method === "delete") {
      // AL-12: 409 nếu composite Active tham chiếu; ?force=true cascade
      if (Number(row.rule_kind) === 0) {
        const refs = activeCompositesReferencing(row.id);
        if (refs.length && String(ctx.params.force) !== "true") {
          httpError(409, "Basic rule đang được composite Active tham chiếu", {
            composites: refs.map((c) => ({ id: c.id, rule_name: c.rule_name })),
          });
        }
        if (refs.length) {
          // force: vô hiệu các composite phụ thuộc
          refs.forEach((c) => replaceRow(touchRule({ ...c, status: 0 })));
        }
      }
      replaceRow(touchRule({ ...row, status: 0 }));
      return { status: 204, data: null };
    }
  }
  httpError(400, `Method ${method} không hỗ trợ`);
});
