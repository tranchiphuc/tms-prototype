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
import SeverityBadge from "../../components/SeverityBadge";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import {
  fetchAlertRules,
  deleteAlertRule,
  patchAlertRule,
} from "../../redux/actions/alertActions";
import {
  RULE_KIND,
  RULE_KIND_FILTER,
  SEVERITY_FILTER,
  STATUS_FILTER,
  conditionLabel,
  metricLabel,
} from "./alertConstants";

const AlertRulesList = ({ reloadToken, onAdd, onEdit, onReload, onState }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const { list, loading } = useSelector((s) => s.alert.rules);
  const [filters, setFilters] = useState({ q: "", severity: "", rule_kind: "", status: 1 });
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const load = useCallback(() => {
    const f = filtersRef.current;
    const params = { page_size: 200, sort: "-updated_at" };
    if (f.q) params.q = f.q;
    if (f.severity !== "") params.severity = f.severity;
    if (f.rule_kind !== "") params.rule_kind = f.rule_kind;
    params.status = f.status;
    dispatch(fetchAlertRules(params));
  }, [dispatch]);

  useEffect(() => {
    load();
  }, [load, reloadToken]);

  const set = (patch) => setFilters((v) => ({ ...v, ...patch }));

  const sttBody = (row) => list.indexOf(row) + 1;

  // "Composite" → "Comp" để cột "Loại" hẹp lại (tooltip giữ tên đầy đủ).
  const kindBody = (row) => (
    <Tag
      value={Number(row.rule_kind) === 1 ? "Comp" : "Basic"}
      severity={Number(row.rule_kind) === 1 ? "info" : null}
      style={{ background: Number(row.rule_kind) === 1 ? undefined : "#8c8c8c" }}
      tooltip={RULE_KIND[row.rule_kind]}
    />
  );

  const doDelete = useCallback(
    (row, force) =>
      dispatch(deleteAlertRule(row.id, force))
        .then(() => {
          toast.success("Đã xóa rule (soft-delete: status=0). Engine emit RESOLVED cho instance đang FIRING.");
          onReload();
        })
        .catch((rej) => {
          const e = errInfo(rej);
          if (e.status === 409) {
            const comps = (e.details && e.details.composites) || [];
            confirmDialog({
              header: "Rule đang được composite tham chiếu",
              message: `${e.message}: ${comps.map((c) => `#${c.id} ${c.rule_name}`).join(", ")}. Vẫn xóa và vô hiệu các composite này?`,
              icon: "pi pi-exclamation-triangle",
              acceptLabel: "Xóa + vô hiệu composite (force)",
              rejectLabel: "Hủy",
              acceptClassName: "p-button-danger",
              accept: () => doDelete(row, true),
            });
          } else {
            toast.error(e.message);
          }
        }),
    [dispatch, toast, onReload]
  );

  const handleDelete = (row) =>
    confirmDialog({
      header: "Xác nhận xóa rule",
      message: `Xóa rule "${row.rule_name}"? Đây là soft-delete (đặt status=0), khác với nút Tắt/Bật (AL-08): xóa còn kiểm tra ràng buộc composite. Instance đang FIRING sẽ được RESOLVED.`,
      icon: "pi pi-exclamation-triangle",
      acceptLabel: "Xóa",
      rejectLabel: "Hủy",
      acceptClassName: "p-button-danger",
      accept: () => doDelete(row, false),
    });

  const toggleStatus = (row) =>
    dispatch(patchAlertRule(row.id, { status: row.status === 1 ? 0 : 1 }))
      .then(() => {
        toast.success(row.status === 1 ? "Đã tắt rule (Disabled)." : "Đã bật lại rule (Active).");
        onReload();
      })
      .catch((rej) => toast.error(errInfo(rej).message));

  const actionBody = (row) => (
    <div className="row-actions">
      <Button
        icon="pi pi-bell"
        className="p-button-text p-button-sm"
        tooltip="Firing state (AL-10)"
        tooltipOptions={{ position: "top" }}
        style={{ color: "#1890ff" }}
        onClick={() => onState(row)}
      />
      <Button
        icon={row.status === 1 ? "pi pi-pause" : "pi pi-play"}
        className="p-button-text p-button-sm"
        tooltip={row.status === 1 ? "Tắt (Disable)" : "Bật (Enable)"}
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
        tooltip="Xóa"
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
              placeholder="Tìm theo tên rule..."
              onChange={(e) => set({ q: e.target.value })}
              onKeyDown={(e) => e.key === "Enter" && load()}
              style={{ width: 240, fontSize: 13 }}
            />
          </span>
          <Dropdown
            value={filters.severity}
            options={SEVERITY_FILTER}
            onChange={(e) => set({ severity: e.value })}
            style={{ fontSize: 13 }}
          />
          <Dropdown
            value={filters.rule_kind}
            options={RULE_KIND_FILTER}
            onChange={(e) => set({ rule_kind: e.value })}
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
        <Column header="STT" body={sttBody} style={{ width: 40 }} />
        <Column
          header="Tên rule"
          field="rule_name"
          sortable
          style={{ width: 150 }}
          body={(row) => (
            <span className="cell-link" onClick={() => onEdit(row)}>
              {row.rule_name}
            </span>
          )}
        />
        <Column header="Loại" field="rule_kind" sortable body={kindBody} style={{ width: 62 }} />
        <Column header="Điều kiện" body={(row) => <code>{conditionLabel(row)}</code>} style={{ width: 170 }} />
        <Column header="Metric" body={(row) => metricLabel(row)} style={{ width: 130 }} />
        <Column
          header="entity_keys"
          body={(row) => (row.entity_keys || []).length ? row.entity_keys.join(", ") : <em>device-level</em>}
          style={{ width: 92 }}
        />
        <Column
          header="Severity"
          field="severity"
          sortable
          body={(row) => <SeverityBadge severity={row.severity} />}
          style={{ width: 78 }}
        />
        <Column header="Sustain/Dedup" body={(row) => `${row.sustain_samples} / ${row.dedup_seconds}s`} style={{ width: 88 }} />
        <Column
          header="Trạng thái"
          field="status"
          sortable
          body={(row) => <StatusBadge status={row.status} activeLabel="Active" inactiveLabel="Disabled" />}
          style={{ width: 80 }}
        />
        <Column header="Hành động" body={actionBody} style={{ width: 134 }} />
      </DataTable>
    </div>
  );
};

export default AlertRulesList;
