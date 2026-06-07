import React from "react";
import { InputText } from "primereact/inputtext";
import { Dropdown } from "primereact/dropdown";
import { Button } from "primereact/button";
import { MODEL_CODE_VALUES } from "../mock/devices";

const STATUS_OPTIONS = [
  { label: "Active", value: 1 },
  { label: "Deprecated", value: 0 },
  { label: "Tất cả", value: "" },
];

const MODEL_OPTIONS = [{ label: "Tất cả Model Code", value: "" }].concat(
  MODEL_CODE_VALUES.map((v) => ({ label: v, value: v }))
);

// Thanh lọc dùng chung: q + Model Code + Status. Gọi onSearch(params).
const FilterBar = ({ value, onChange, onSearch, searchPlaceholder = "Tìm kiếm..." }) => {
  const set = (patch) => onChange({ ...value, ...patch });

  return (
    <div className="filter-bar" style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
      <span className="p-input-icon-left">
        <i className="pi pi-search" />
        <InputText
          value={value.q || ""}
          placeholder={searchPlaceholder}
          onChange={(e) => set({ q: e.target.value })}
          onKeyDown={(e) => e.key === "Enter" && onSearch()}
          style={{ width: 280, fontSize: 13 }}
        />
      </span>
      <Dropdown
        value={value.vendor_code || ""}
        options={MODEL_OPTIONS}
        onChange={(e) => set({ vendor_code: e.value })}
        style={{ fontSize: 13 }}
      />
      <Dropdown
        value={value.status === undefined ? 1 : value.status}
        options={STATUS_OPTIONS}
        onChange={(e) => set({ status: e.value })}
        style={{ fontSize: 13 }}
      />
      <Button label="Tìm kiếm" icon="pi pi-search" className="btn-search" onClick={onSearch} />
    </div>
  );
};

export default FilterBar;
