// Nhãn & tuỳ chọn dùng chung cho màn Derived Metrics (DM-*)

export const DERIVE_KIND = { 0: "Computed", 1: "Aggregated", 2: "Delta" };

export const DERIVE_KIND_OPTIONS = [
  { label: "0 - Computed (công thức nhiều input)", value: 0 },
  { label: "1 - Aggregated (tumbling window)", value: 1 },
  { label: "2 - Delta (rate giữa 2 sample)", value: 2 },
];

export const DERIVE_KIND_FILTER = [{ label: "Tất cả loại", value: "" }].concat(
  Object.entries(DERIVE_KIND).map(([v, l]) => ({ label: l, value: Number(v) }))
);

export const AGG_FUNCTION = { 0: "avg", 1: "max", 2: "min", 3: "sum", 4: "rate" };

export const AGG_FUNCTION_OPTIONS = Object.entries(AGG_FUNCTION).map(([v, l]) => ({
  label: `${v} - ${l}`,
  value: Number(v),
}));

export const STATUS_FILTER = [
  { label: "Active", value: 1 },
  { label: "Deprecated", value: 0 },
  { label: "Tất cả", value: "" },
];
