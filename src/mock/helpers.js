// ============================================================
// Mock helpers — filter / sort / paginate + ID + timestamp
// ============================================================

let _seq = 1000;
export const nextId = () => ++_seq;

// Mốc thời gian cố định quanh "hiện tại" của prototype (2026-06-06)
export const NOW = "2026-06-06T09:00:00+07:00";

export const isoMinutesAgo = (mins) => {
  const base = new Date("2026-06-06T09:00:00+07:00").getTime();
  return new Date(base - mins * 60000).toISOString();
};

// Lọc theo status (mặc định 1), vendor_code, và q (tìm chuỗi trên các field chỉ định)
export const applyFilters = (rows, params, searchFields = []) => {
  let out = rows;
  const status = params.status === undefined || params.status === "" ? 1 : Number(params.status);
  if (!Number.isNaN(status)) {
    out = out.filter((r) => Number(r.status) === status);
  }
  if (params.vendor_code && params.vendor_code !== "All_filter") {
    out = out.filter((r) => r.vendor_code === params.vendor_code);
  }
  if (params.alias_metric) {
    out = out.filter((r) => r.alias_metric === params.alias_metric);
  }
  if (params.path_alias_id !== undefined && params.path_alias_id !== "") {
    const pid = Number(params.path_alias_id);
    out = out.filter((r) => Number(r.path_alias_id) === pid);
  }
  if (params.q) {
    const q = String(params.q).toLowerCase();
    out = out.filter((r) =>
      searchFields.some((f) => String(r[f] || "").toLowerCase().includes(q))
    );
  }
  return out;
};

// Sort theo "-field" (desc) hoặc "field" (asc); mặc định -updated_at
export const applySort = (rows, sort = "-updated_at") => {
  const desc = sort.startsWith("-");
  const field = desc ? sort.slice(1) : sort;
  const out = [...rows].sort((a, b) => {
    const av = a[field];
    const bv = b[field];
    if (av === bv) return 0;
    return av > bv ? 1 : -1;
  });
  return desc ? out.reverse() : out;
};

// Phân trang → { page, page_size, total, items }
export const paginate = (rows, params) => {
  const page = Math.max(1, Number(params.page) || 1);
  const pageSize = Math.min(200, Number(params.page_size) || 50);
  const total = rows.length;
  const start = (page - 1) * pageSize;
  return {
    page,
    page_size: pageSize,
    total,
    items: rows.slice(start, start + pageSize),
  };
};

// Pipeline đầy đủ cho GET list
export const listResponse = (rows, params, searchFields, defaultSort = "-updated_at") => {
  const filtered = applyFilters(rows, params, searchFields);
  const sorted = applySort(filtered, params.sort || defaultSort);
  return paginate(sorted, params);
};

// Stamp version mới khi write
export const touch = (row) => {
  const ts = new Date().toISOString();
  return { ...row, updated_at: ts, pushed_at: null };
};

// Lỗi mô phỏng — throw object { status, data }
export const httpError = (status, message, details) => {
  // eslint-disable-next-line no-throw-literal
  throw { status, data: { error: errName(status), message, details } };
};

const errName = (status) =>
  ({
    400: "bad_request",
    404: "not_found",
    409: "conflict",
    422: "validation_failed",
  }[status] || "error");
