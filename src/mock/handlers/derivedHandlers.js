import { registerHandler } from "../../services/mockApi";
import { applyFilters, applySort, paginate, touch, httpError, nextId } from "../helpers";
import { DERIVED_RULES_DATA } from "../derivedRules";
import { METRIC_ALIASES_DATA } from "../metricAliases";

// ---- In-memory store ----
let store = [...DERIVED_RULES_DATA];

const AGG_FUNCTIONS = [0, 1, 2, 3, 4]; // avg,max,min,sum,rate
const SEARCH_FIELDS = ["output_metric", "rule_name"];

const findRow = (id) => store.find((r) => r.id === Number(id));
const replaceRow = (row) => {
  const idx = store.findIndex((r) => r.id === row.id);
  if (idx >= 0) store[idx] = row;
};

// alias_metric đang tồn tại trong tlm_metric_aliases (Active) — output_metric không được trùng
const aliasMetricSet = () =>
  new Set(METRIC_ALIASES_DATA.filter((m) => Number(m.status) === 1).map((m) => m.alias_metric));

// ---- Validation (DM-02/03/04/05/09) ----
const validate = (body, selfId) => {
  const kind = Number(body.derive_kind);
  if (![0, 1, 2].includes(kind)) {
    httpError(422, "derive_kind phải ∈ {0=computed, 1=aggregated, 2=delta}");
  }
  if (!body.output_metric) httpError(422, "output_metric bắt buộc");

  // output_metric unique trong active + không trùng alias_metric
  const dupActive = store.some(
    (r) => r.id !== selfId && Number(r.status) === 1 && r.output_metric === body.output_metric
  );
  if (dupActive && Number(body.status ?? 1) === 1) {
    httpError(409, `output_metric "${body.output_metric}" đã tồn tại ở một active derived rule`);
  }
  if (aliasMetricSet().has(body.output_metric)) {
    httpError(422, `output_metric "${body.output_metric}" trùng một alias_metric trong tlm_metric_aliases`, [
      { field: "output_metric", reason: "không được trùng alias_metric của metric alias" },
    ]);
  }

  // input_metrics: array of string, pairwise distinct
  const inputs = body.input_metrics;
  if (!Array.isArray(inputs) || inputs.length === 0) {
    httpError(422, "input_metrics phải là mảng ≥1 alias_metric (chuỗi)");
  }
  if (inputs.some((x) => typeof x !== "string" || !x)) {
    httpError(422, "input_metrics phải là mảng chuỗi (alias_metric)");
  }
  if (new Set(inputs).size !== inputs.length) {
    httpError(422, "input_metrics phải đôi một khác nhau");
  }

  // scope_device_ids ≤ 20 (DM-05)
  if (Array.isArray(body.scope_device_ids) && body.scope_device_ids.length > 20) {
    httpError(422, "Tối đa 20 thiết bị cho mỗi rule", [
      { field: "scope_device_ids", reason: `đang có ${body.scope_device_ids.length}, tối đa 20` },
    ]);
  }

  // Ràng buộc theo derive_kind
  if (kind === 0) {
    if (!body.expression) httpError(422, "Computed: expression bắt buộc");
    if (body.window_seconds != null || body.agg_function != null) {
      httpError(422, "Computed: window_seconds/agg_function phải NULL");
    }
    if (body.assembly_window_seconds != null && Number(body.assembly_window_seconds) <= 0) {
      httpError(422, "Computed: assembly_window_seconds phải > 0 nếu khai báo");
    }
  } else if (kind === 1) {
    if (inputs.length !== 1) httpError(422, "Aggregated: đúng 1 input_metric");
    if (body.window_seconds == null || Number(body.window_seconds) <= 0) {
      httpError(422, "Aggregated: window_seconds bắt buộc và > 0");
    }
    if (!AGG_FUNCTIONS.includes(Number(body.agg_function))) {
      httpError(422, "Aggregated: agg_function ∈ {0=avg,1=max,2=min,3=sum,4=rate}");
    }
    if (body.expression) httpError(422, "Aggregated: expression phải NULL");
  } else if (kind === 2) {
    if (inputs.length !== 1) httpError(422, "Delta: đúng 1 input_metric");
    if (body.expression || body.window_seconds != null) {
      httpError(422, "Delta: expression + window_seconds phải NULL");
    }
  }
};

// ---- Default body khi tạo / cập nhật ----
// Chuẩn hoá field theo derive_kind: dọn field không liên quan về null.
const normalize = (body) => {
  const kind = Number(body.derive_kind);
  const out = {
    rule_name: body.rule_name || "",
    output_metric: body.output_metric,
    output_unit: body.output_unit || "",
    derive_kind: kind,
    input_metrics: body.input_metrics || [],
    expression: null,
    assembly_window_seconds: null,
    window_seconds: null,
    agg_function: null,
    delta_scale_factor: null,
    delta_reset_threshold: null,
    static_constants: null,
    scope_path_alias_id: body.scope_path_alias_id == null ? null : Number(body.scope_path_alias_id),
    scope_device_ids:
      Array.isArray(body.scope_device_ids) && body.scope_device_ids.length
        ? body.scope_device_ids.map(Number)
        : null,
    priority: body.priority == null ? 100 : Number(body.priority),
    status: body.status == null ? 1 : Number(body.status),
  };
  if (kind === 0) {
    out.expression = body.expression;
    out.assembly_window_seconds =
      body.assembly_window_seconds == null ? null : Number(body.assembly_window_seconds);
    out.static_constants =
      body.static_constants && Object.keys(body.static_constants).length ? body.static_constants : null;
  } else if (kind === 1) {
    out.window_seconds = Number(body.window_seconds);
    out.agg_function = Number(body.agg_function);
  } else if (kind === 2) {
    out.delta_scale_factor = body.delta_scale_factor == null ? 1.0 : Number(body.delta_scale_factor);
    out.delta_reset_threshold =
      body.delta_reset_threshold == null || body.delta_reset_threshold === ""
        ? null
        : Number(body.delta_reset_threshold);
  }
  return out;
};

// ---- Preview (DM-06) — một-thời-điểm, không windowing thực ----
const buildPreview = (body) => {
  const warnings = [];
  const errors = [];
  const kind = Number(body.derive_kind);
  const sample = body.sample_inputs || {};
  let result = null;

  try {
    if (kind === 0) {
      // computed — eval expression với sample_inputs + static_constants
      const vars = { ...(body.static_constants || {}), ...sample };
      const missing = (body.input_metrics || []).filter((m) => vars[m] == null);
      if (missing.length) {
        warnings.push(
          `Thiếu input: ${missing.join(", ")} — Job 2 sẽ thay bằng static_constants nếu có, ngược lại bỏ qua chu kỳ.`
        );
      }
      if (body.expression) {
        // eslint-disable-next-line no-new-func
        const fn = new Function(...Object.keys(vars), `return (${body.expression});`);
        result = fn(...Object.values(vars));
      } else {
        errors.push("expression rỗng");
      }
    } else if (kind === 1) {
      const m = (body.input_metrics || [])[0];
      const series = Array.isArray(sample[m]) ? sample[m] : [];
      if (!series.length) {
        warnings.push("Chưa nhập chuỗi mẫu — preview aggregated cần mảng giá trị trong window.");
      } else {
        const agg = Number(body.agg_function);
        if (agg === 0) result = series.reduce((a, b) => a + b, 0) / series.length;
        else if (agg === 1) result = Math.max(...series);
        else if (agg === 2) result = Math.min(...series);
        else if (agg === 3) result = series.reduce((a, b) => a + b, 0);
        else if (agg === 4) result = (series[series.length - 1] - series[0]) / Number(body.window_seconds || 1);
      }
    } else if (kind === 2) {
      const m = (body.input_metrics || [])[0];
      const pair = sample[m] || {};
      const { prev, curr, dt } = pair;
      if (prev == null || curr == null || dt == null) {
        warnings.push("Delta preview cần {prev, curr, dt} cho input. Sample đầu chỉ lưu state, không emit.");
      } else if (Number(dt) <= 0) {
        warnings.push("Δt ≤ 0 → Job 2 bỏ emit.");
      } else if (body.delta_reset_threshold != null && curr < prev) {
        warnings.push("curr < prev → nghi counter reset; Job 2 bỏ sample này và restart tracking.");
      } else {
        const scale = body.delta_scale_factor == null ? 1 : Number(body.delta_scale_factor);
        result = ((curr - prev) * scale) / Number(dt);
      }
    }
  } catch (e) {
    errors.push(`Lỗi tính: ${e.message}`);
  }

  return {
    result: result == null ? null : Number(Number(result).toFixed(6)),
    unit: body.output_unit || "",
    output_metric: body.output_metric,
    warnings,
    errors,
    note: "Preview một-thời-điểm — không mô phỏng windowing/watermark thực, không phát hiện reset qua chuỗi sample.",
  };
};

// ---- Observability (DM-08) ----
const buildObservability = () => {
  // giả lập counter per-rule; rule có nhiều input/computed → incomplete cao hơn
  const rules = store
    .filter((r) => Number(r.status) === 1)
    .map((r, i) => {
      const incomplete =
        Number(r.derive_kind) === 0 ? (r.input_metrics.length - 1) * 37 + (i % 5) * 11 : i % 3;
      const emit = 5200 - i * 213 - incomplete * 5;
      return {
        rule_id: r.id,
        rule_name: r.rule_name,
        output_metric: r.output_metric,
        derive_kind: r.derive_kind,
        derived_incomplete_total: incomplete,
        emit_total: Math.max(0, emit),
        misconfig_suspect: incomplete > 80,
      };
    });
  return {
    rules,
    note: "incomplete cao kéo dài ⇒ rule có thể cấu hình sai input/scope (cửa sổ gom hết hạn mà chưa đủ input).",
  };
};

// ============================================================
// Đăng ký handlers — route cụ thể TRƯỚC route có {id}
// ============================================================
registerHandler(/^\/derived\/rules\/preview$/, (ctx) => ({
  status: 200,
  data: buildPreview(ctx.data || {}),
}));

registerHandler(/^\/derived\/observability$/, () => ({
  status: 200,
  data: buildObservability(),
}));

registerHandler(/^\/derived\/rules(?:\/(\d+))?$/, (ctx) => {
  const id = ctx.match[1];
  const method = ctx.method;

  if (!id) {
    if (method === "get") {
      let rows = applyFilters(store, ctx.params, SEARCH_FIELDS);
      // lọc thêm derive_kind nếu có
      if (ctx.params.derive_kind !== undefined && ctx.params.derive_kind !== "") {
        rows = rows.filter((r) => Number(r.derive_kind) === Number(ctx.params.derive_kind));
      }
      const sorted = applySort(rows, ctx.params.sort || "-updated_at");
      return { status: 200, data: paginate(sorted, ctx.params) };
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
    if (!row) httpError(404, "Không tìm thấy derived rule");

    if (method === "get") return { status: 200, data: row };

    if (method === "put") {
      const body = ctx.data || {};
      validate(body, row.id);
      const updated = touch({
        ...normalize(body),
        id: row.id,
        created_at: row.created_at,
        created_by: row.created_by,
      });
      // tlm_derived_rules KHÔNG có pushed_at — gỡ field touch() vừa thêm
      delete updated.pushed_at;
      replaceRow(updated);
      return { status: 200, data: updated };
    }

    if (method === "patch") {
      // DM-09: {status:0} hoặc {priority:120}
      const body = ctx.data || {};
      const updated = touch({ ...row, ...body, id: row.id });
      delete updated.pushed_at;
      replaceRow(updated);
      return { status: 200, data: updated };
    }

    if (method === "delete") {
      // soft-delete; KHÔNG reset pushed_at (không có)
      const updated = touch({ ...row, status: 0 });
      delete updated.pushed_at;
      replaceRow(updated);
      return { status: 204, data: null };
    }
  }
  httpError(400, `Method ${method} không hỗ trợ`);
});
