import React, { useCallback, useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Button } from "primereact/button";
import { InputText } from "primereact/inputtext";
import { Dropdown } from "primereact/dropdown";
import { Tag } from "primereact/tag";
import { confirmDialog } from "primereact/confirmdialog";
import StatusBadge from "../../components/StatusBadge";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import {
  fetchDerivedRules,
  deleteDerivedRule,
  patchDerivedRule,
} from "../../redux/actions/derivedActions";
import {
  DERIVE_KIND,
  DERIVE_KIND_FILTER,
  AGG_FUNCTION,
  STATUS_FILTER,
} from "./derivedConstants";

const KIND_SEVERITY = { 0: "info", 1: "success", 2: "warning" };

const DerivedRulesList = ({ reloadToken, onAdd, onEdit, onReload }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const { list, loading } = useSelector((s) => s.derived.rules);
  const [filters, setFilters] = useState({ q: "", derive_kind: "", status: 1 });
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const load = useCallback(() => {
    const f = filtersRef.current;
    const params = { page_size: 200, sort: "-updated_at" };
    if (f.q) params.q = f.q;
    if (f.derive_kind !== "") params.derive_kind = f.derive_kind;
    params.status = f.status;
    dispatch(fetchDerivedRules(params));
  }, [dispatch]);

  useEffect(() => {
    load();
  }, [load, reloadToken]);

  const set = (patch) => setFilters((v) => ({ ...v, ...patch }));

  const sttBody = (row) => list.indexOf(row) + 1;

  const kindBody = (row) => (
    <Tag value={DERIVE_KIND[row.derive_kind]} severity={KIND_SEVERITY[row.derive_kind]} />
  );

  const detailBody = (row) => {
    if (row.derive_kind === 0) return <code>{row.expression}</code>;
    if (row.derive_kind === 1)
      return (
        <span>
          {AGG_FUNCTION[row.agg_function]} / {row.window_seconds}s
        </span>
      );
    return <span>×{row.delta_scale_factor} / Δt</span>;
  };

  const doDelete = useCallback(
    (row) =>
      dispatch(deleteDerivedRule(row.id))
        .then(() => {
          toast.success("Đã Deprecate (soft-delete) derived rule.");
          onReload();
        })
        .catch((rej) => toast.error(errInfo(rej).message)),
    [dispatch, toast, onReload]
  );

  const handleDelete = (row) =>
    confirmDialog({
      header: "Xác nhận Deprecate",
      message: `Deprecate derived rule "${row.output_metric}"? Flink ngừng emit ở chu kỳ refresh kế (≤60s).`,
      icon: "pi pi-info-circle",
      acceptLabel: "Deprecate",
      rejectLabel: "Hủy",
      accept: () => doDelete(row),
    });

  const toggleStatus = (row) =>
    dispatch(patchDerivedRule(row.id, { status: row.status === 1 ? 0 : 1 }))
      .then(() => {
        toast.success(row.status === 1 ? "Đã tạm dừng rule." : "Đã kích hoạt lại rule.");
        onReload();
      })
      .catch((rej) => toast.error(errInfo(rej).message));

  const actionBody = (row) => (
    <div className="row-actions">
      <Button
        icon={row.status === 1 ? "pi pi-pause" : "pi pi-play"}
        className="p-button-text p-button-sm"
        tooltip={row.status === 1 ? "Disable" : "Enable"}
        tooltipOptions={{ position: "top" }}
        style={{ color: row.status === 1 ? "#faad14" : "#52c41a" }}
        onClick={() => toggleStatus(row)}
      />
      <Button
        icon="pi pi-pencil"
        className="action-icon-btn edit p-button-text p-button-sm"
        tooltip="Sửa"
        tooltipOptions={{ position: "top" }}
        onClick={() => onEdit(row)}
      />
      <Button
        icon="pi pi-trash"
        className="action-icon-btn delete p-button-text p-button-sm"
        tooltip="Deprecate"
        tooltipOptions={{ position: "top" }}
        onClick={() => handleDelete(row)}
      />
    </div>
  );

  return (
    <div>
      <div className="page-header">
        <div className="filter-bar" style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <span className="p-input-icon-left">
            <i className="pi pi-search" />
            <InputText
              value={filters.q}
              placeholder="Tìm theo output_metric / tên rule..."
              onChange={(e) => set({ q: e.target.value })}
              onKeyDown={(e) => e.key === "Enter" && load()}
              style={{ width: 280, fontSize: 13 }}
            />
          </span>
          <Dropdown
            value={filters.derive_kind}
            options={DERIVE_KIND_FILTER}
            onChange={(e) => set({ derive_kind: e.value })}
            style={{ fontSize: 13 }}
          />
          <Dropdown
            value={filters.status}
            options={STATUS_FILTER}
            onChange={(e) => set({ status: e.value })}
            style={{ fontSize: 13 }}
          />
          <Button label="Tìm kiếm" icon="pi pi-search" className="btn-search" onClick={load} />
        </div>
        <div className="page-actions">
          <Button label="Thêm mới" icon="pi pi-plus" className="btn-add" onClick={onAdd} />
        </div>
      </div>

      <DataTable
        value={list}
        loading={loading}
        paginator
        rows={20}
        rowsPerPageOptions={[20, 50, 100]}
        dataKey="id"
        responsiveLayout="scroll"
        className="rule-table"
        emptyMessage="Không có dữ liệu"
        currentPageReportTemplate="Hiển thị {first} đến {last} trên tổng số {totalRecords} bản ghi"
        paginatorTemplate="CurrentPageReport FirstPageLink PrevPageLink PageLinks NextPageLink LastPageLink RowsPerPageDropdown"
      >
        <Column header="STT" body={sttBody} style={{ width: 56 }} />
        <Column
          header="Output Metric"
          field="output_metric"
          sortable
          style={{ width: 180 }}
          body={(row) => (
            <span className="cell-link" onClick={() => onEdit(row)}>
              {row.output_metric}
            </span>
          )}
        />
        <Column header="Unit" field="output_unit" style={{ width: 70 }} />
        <Column header="Loại" field="derive_kind" sortable body={kindBody} style={{ width: 120 }} />
        <Column
          header="Input"
          body={(row) => row.input_metrics.join(", ")}
          style={{ width: 200 }}
        />
        <Column header="Cấu hình" body={detailBody} style={{ width: 220 }} />
        <Column
          header="Trạng thái"
          field="status"
          sortable
          style={{ width: 120 }}
          body={(row) => <StatusBadge status={row.status} />}
        />
        <Column header="Hành động" body={actionBody} style={{ width: 132 }} />
      </DataTable>
    </div>
  );
};

export default DerivedRulesList;
