import { registerHandler } from "../../services/mockApi";
import { listResponse, touch, httpError, nextId, isoMinutesAgo } from "../helpers";
import { DEVICES, MODEL_CODE_VALUES } from "../devices";
import { PATH_ALIASES } from "../pathAliases";
import { METRIC_ALIASES_DATA } from "../metricAliases";
import { LABEL_ALIASES_DATA } from "../labelAliases";
import { FILTER_RULES_DATA } from "../filterRules";

// ---- In-memory stores (mutable bản sao của seed data) ----
const stores = {
  "path-aliases": [...PATH_ALIASES],
  "metric-aliases": [...METRIC_ALIASES_DATA],
  "label-aliases": [...LABEL_ALIASES_DATA],
  "filter-rules": [...FILTER_RULES_DATA],
};

const SEARCH_FIELDS = {
  "path-aliases": ["original_path", "alias_path"],
  "metric-aliases": ["original_name", "alias_metric"],
  "label-aliases": ["original_key", "alias_key"],
  "filter-rules": ["match_path", "match_metric", "filter_expression"],
};

// ---- Validation per group ----
const validateModelCode = (body) => {
  if (!MODEL_CODE_VALUES.includes(body.vendor_code)) {
    httpError(422, "Model Code không hợp lệ", [
      { field: "vendor_code", reason: `phải ∈ {${MODEL_CODE_VALUES.join(", ")}}` },
    ]);
  }
};

const uniqueActive = (group, predicate, selfId) =>
  stores[group].some(
    (r) => r.id !== selfId && Number(r.status) === 1 && predicate(r)
  );

const validators = {
  "path-aliases": (body, selfId) => {
    validateModelCode(body);
    if (!body.original_path) httpError(422, "original_path bắt buộc");
    if (!body.alias_path) httpError(422, "alias_path bắt buộc");
    if (
      uniqueActive(
        "path-aliases",
        (r) => r.vendor_code === body.vendor_code && r.original_path === body.original_path,
        selfId
      )
    ) {
      httpError(409, "Trùng (Model Code, original_path)");
    }
    if (
      uniqueActive(
        "path-aliases",
        (r) => r.vendor_code === body.vendor_code && r.alias_path === body.alias_path,
        selfId
      )
    ) {
      httpError(409, "Trùng (Model Code, alias_path, status=Active)");
    }
  },

  "metric-aliases": (body, selfId) => {
    validateModelCode(body);
    if (!body.original_name) httpError(422, "original_name bắt buộc");
    if (!body.alias_metric) httpError(422, "alias_metric bắt buộc");
    const tk = Number(body.transform_kind);
    if (tk === 2 && !body.transform_expression) {
      httpError(422, "transform_kind=2 yêu cầu transform_expression", [
        { field: "transform_expression", reason: "bắt buộc khi transform_kind=2" },
      ]);
    }
    if (tk !== 2 && body.transform_expression) {
      httpError(422, "transform_expression chỉ dùng khi transform_kind=2");
    }
    if (tk === 3 && !body.enum_mapping) {
      httpError(422, "transform_kind=3 yêu cầu enum_mapping", [
        { field: "enum_mapping", reason: "bắt buộc khi transform_kind=3" },
      ]);
    }
    if (tk !== 3 && body.enum_mapping) {
      httpError(422, "enum_mapping chỉ dùng khi transform_kind=3");
    }
    const vt = body.value_type_override;
    if (vt === 2 || vt === 3) {
      if (body.source_unit || body.target_unit) {
        httpError(422, "value_type string/bool ⇒ source_unit/target_unit phải rỗng");
      }
      if (Number(body.scale_factor) !== 1 || Number(body.offset_value) !== 0) {
        httpError(422, "value_type string/bool ⇒ scale_factor=1, offset_value=0");
      }
      if (![0, 3].includes(tk)) {
        httpError(422, "value_type string/bool ⇒ transform_kind ∈ {0,3}");
      }
    }
    const scopeKey = body.path_alias_id == null ? 0 : Number(body.path_alias_id);
    const sameScope = (r) =>
      r.vendor_code === body.vendor_code && (r.path_alias_id == null ? 0 : Number(r.path_alias_id)) === scopeKey;
    if (uniqueActive("metric-aliases", (r) => sameScope(r) && r.original_name === body.original_name, selfId)) {
      httpError(409, "Trùng (Model Code, path scope, original_name)");
    }
    if (uniqueActive("metric-aliases", (r) => sameScope(r) && r.alias_metric === body.alias_metric, selfId)) {
      httpError(409, "Trùng (Model Code, path scope, alias_metric)");
    }
  },

  "label-aliases": (body, selfId) => {
    validateModelCode(body);
    if (!body.original_key) httpError(422, "original_key bắt buộc");
    if (!body.alias_key) httpError(422, "alias_key bắt buộc");
    const lk = Number(body.lv_kind);
    if (lk === 1 && (!body.lv_pattern || !body.lv_replace)) {
      httpError(422, "lv_kind=regex yêu cầu lv_pattern + lv_replace");
    }
    if (lk !== 1 && (body.lv_pattern || body.lv_replace)) {
      httpError(422, "lv_pattern/lv_replace chỉ dùng khi lv_kind=regex");
    }
    if (lk === 2 && !body.lv_mapping) {
      httpError(422, "lv_kind=enum_mapping yêu cầu lv_mapping");
    }
    if (lk !== 2 && body.lv_mapping) {
      httpError(422, "lv_mapping chỉ dùng khi lv_kind=enum_mapping");
    }
    const scopeKey = body.path_alias_id == null ? 0 : Number(body.path_alias_id);
    const sameScope = (r) =>
      r.vendor_code === body.vendor_code && (r.path_alias_id == null ? 0 : Number(r.path_alias_id)) === scopeKey;
    if (uniqueActive("label-aliases", (r) => sameScope(r) && r.original_key === body.original_key, selfId)) {
      httpError(409, "Trùng (Model Code, path scope, original_key)");
    }
    if (uniqueActive("label-aliases", (r) => sameScope(r) && r.alias_key === body.alias_key, selfId)) {
      httpError(409, "Trùng (Model Code, path scope, alias_key)");
    }
  },

  "filter-rules": (body) => {
    validateModelCode(body);
    if (![0, 1].includes(Number(body.filter_action))) {
      httpError(422, "filter_action phải ∈ {0,1}");
    }
  },
};

// ---- Default body khi tạo mới ----
const withDefaults = (group, body) => {
  const base = {
    ...body,
    id: nextId(),
    status: body.status == null ? 1 : Number(body.status),
    created_by: "operator",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    pushed_at: null,
  };
  return base;
};

const findRow = (group, id) => stores[group].find((r) => r.id === Number(id));

// ---- Cascade khi Deprecate Path Alias ----
const childrenOfPath = (pathId) => {
  const kids = [];
  stores["metric-aliases"].forEach((r) => {
    if (Number(r.path_alias_id) === Number(pathId) && Number(r.status) === 1)
      kids.push({ group: "metric-aliases", id: r.id, name: r.alias_metric });
  });
  stores["label-aliases"].forEach((r) => {
    if (Number(r.path_alias_id) === Number(pathId) && Number(r.status) === 1)
      kids.push({ group: "label-aliases", id: r.id, name: r.alias_key });
  });
  return kids;
};

// ---- CRUD dispatch cho mỗi group ----
const registerGroup = (group) => {
  // Preview (đăng ký trước item pattern)
  registerHandler(new RegExp(`^/flink/${group}/preview$`), (ctx) => {
    return { status: 200, data: buildPreview(group, ctx.data || {}) };
  });

  // List + create: /flink/{group}
  // Item: /flink/{group}/{id}
  registerHandler(new RegExp(`^/flink/${group}(?:/(\\d+))?$`), (ctx) => {
    const id = ctx.match[1];
    const method = ctx.method;

    if (!id) {
      if (method === "get") {
        return { status: 200, data: listResponse(stores[group], ctx.params, SEARCH_FIELDS[group]) };
      }
      if (method === "post") {
        const body = ctx.data || {};
        validators[group](body, null);
        const row = withDefaults(group, body);
        stores[group].unshift(row);
        return { status: 201, data: row };
      }
    } else {
      const row = findRow(group, id);
      if (!row) httpError(404, "Không tìm thấy bản ghi");

      if (method === "get") return { status: 200, data: row };

      if (method === "put") {
        const body = ctx.data || {};
        // alias_path không sửa được sau Active (path-aliases)
        if (group === "path-aliases" && Number(row.status) === 1 && body.alias_path !== row.alias_path) {
          httpError(422, "alias_path không sửa được sau khi Active — hãy Deprecate row cũ + tạo mới");
        }
        validators[group](body, row.id);
        const updated = touch({ ...row, ...body, id: row.id, created_at: row.created_at, created_by: row.created_by });
        replaceRow(group, updated);
        return { status: 200, data: updated };
      }

      if (method === "patch") {
        const body = ctx.data || {};
        const updated = touch({ ...row, ...body, id: row.id });
        replaceRow(group, updated);
        return { status: 200, data: updated };
      }

      if (method === "delete") {
        // Cascade cho path-aliases
        if (group === "path-aliases") {
          const kids = childrenOfPath(row.id);
          const force = String(ctx.params.force) === "true";
          if (kids.length && !force) {
            httpError(409, "Còn alias con Active trỏ tới path này — dùng ?force=true để cascade", kids);
          }
          if (kids.length && force) {
            kids.forEach((k) => {
              const child = findRow(k.group, k.id);
              if (child) replaceRow(k.group, touch({ ...child, status: 0 }));
            });
          }
        }
        replaceRow(group, touch({ ...row, status: 0 }));
        return { status: 204, data: null };
      }
    }
    httpError(400, `Method ${method} không hỗ trợ cho ${group}`);
  });
};

const replaceRow = (group, row) => {
  const idx = stores[group].findIndex((r) => r.id === row.id);
  if (idx >= 0) stores[group][idx] = row;
};

// ---- Preview mock ----
const buildPreview = (group, body) => {
  if (group === "filter-rules") {
    const sampled = 540;
    const matched = Math.floor(sampled * 0.42);
    const action = Number(body.filter_action) === 0 ? "drop" : "keep";
    return {
      sampled_records: sampled,
      matched_records: matched,
      filter_action: Number(body.filter_action),
      affected: action === "drop" ? matched : sampled - matched,
      note: `Sẽ ${action === "drop" ? "DROP" : "GIỮ"} ${
        action === "drop" ? matched : matched
      } record. Preview một-rule, một-thời-điểm.`,
      samples: [
        { device_name: "R-HCM-001", metric_name: body.match_metric || "if_in_discards", value_number: 0, decision: action === "drop" ? "DROP" : "KEEP" },
        { device_name: "R-HN-001", metric_name: body.match_metric || "if_in_octets", value_number: 1024, decision: action === "drop" ? "KEEP" : "DROP" },
      ],
    };
  }
  if (group === "metric-aliases") {
    const scale = Number(body.scale_factor) || 1;
    const offset = Number(body.offset_value) || 0;
    return {
      sampled_records: 540,
      matched_records: 312,
      samples: [
        {
          raw_metric_name: `...:${body.original_name || "free-application-memory"}`,
          before: { metric_name: body.original_name || "free-application-memory", value_number: 1572864, unit: body.source_unit || "" },
          after: {
            metric_name: body.alias_metric || "node_free_memory",
            value_number: Number((1572864 * scale + offset).toFixed(4)),
            unit: body.target_unit || "",
          },
        },
      ],
    };
  }
  if (group === "label-aliases") {
    return {
      sampled_records: 480,
      matched_records: 290,
      samples: [
        { original_key: body.original_key, original_value: "Gi0/0/0/1", alias_key: body.alias_key, alias_value: "GigabitEthernet0/0/0/1" },
      ],
    };
  }
  // path-aliases
  return {
    sampled_records: 600,
    matched_records: 358,
    samples: [
      { raw_path: body.original_path, alias_path: body.alias_path, path_id: 999 },
    ],
  };
};

// ---- Đăng ký ----
["path-aliases", "metric-aliases", "label-aliases", "filter-rules"].forEach(registerGroup);

// devices (read-only)
registerHandler(/^\/flink\/devices$/, () => ({ status: 200, data: { items: DEVICES } }));

// refresh-status
registerHandler(/^\/flink\/refresh-status$/, () => {
  const tableMeta = (group, tableName) => {
    const rows = stores[group];
    const pending = rows.filter((r) => r.pushed_at == null);
    const maxUpdated = rows.reduce((m, r) => (r.updated_at > m ? r.updated_at : m), "");
    return {
      table_name: tableName,
      max_updated_at: maxUpdated,
      rows_pending_push: pending.length,
      oldest_pending_pushed_at: null,
      last_push_completed_at: isoMinutesAgo(3),
    };
  };
  return {
    status: 200,
    data: {
      tables: [
        tableMeta("path-aliases", "tlm_path_aliases"),
        tableMeta("metric-aliases", "tlm_metric_aliases"),
        tableMeta("label-aliases", "tlm_label_aliases"),
        tableMeta("filter-rules", "tlm_filter_rules"),
      ],
      note: "Flink refresh interval ~ vài phút. pushed_at=NULL nghĩa là chưa mirror sang ClickHouse.",
    },
  };
});

// Fallthrough monitor (FR-08) — metric chưa khớp alias trong 1h
registerHandler(/^\/flink\/fallthrough$/, () => ({
  status: 200,
  data: {
    items: [
      { raw_path: "Cisco-IOS-XR-clns-isis-oper:isis/instances", raw_metric_name: "Cisco-IOS-XR-clns-isis-oper:isis/instances/instance/levels/level/adjacencies/adjacency/adjacency-state", vendor_code: "Cisco", device_name: "R-HCM-002", occurrences: 1284 },
      { raw_path: "openconfig-platform:components/component", raw_metric_name: "openconfig-platform:components/component/fan/state/speed", vendor_code: "OpenConfig", device_name: "R-DN-001", occurrences: 642 },
      { raw_path: "junos/system/linecard/npu", raw_metric_name: "junos/system/linecard/npu/memory/utilization", vendor_code: "Juniper", device_name: "R-HN-002", occurrences: 410 },
      { raw_path: "state/router/mpls", raw_metric_name: "state/router/mpls/lsp/transmit-packets", vendor_code: "Nokia", device_name: "R-DN-001", occurrences: 233 },
    ],
    note: "Fallthrough vẫn được lưu với tên thô — không mất dữ liệu.",
  },
}));
