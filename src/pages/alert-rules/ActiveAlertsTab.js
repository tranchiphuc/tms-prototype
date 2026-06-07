import React, { useCallback, useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Button } from "primereact/button";
import { Dropdown } from "primereact/dropdown";
import { Tag } from "primereact/tag";
import SeverityBadge from "../../components/SeverityBadge";
import { fetchActiveAlerts } from "../../redux/actions/alertActions";
import { SEVERITY_FILTER } from "./alertConstants";
import { DEVICES } from "../../mock/devices";
import { fmtTime } from "./fmt";

const DEVICE_FILTER = [{ label: "Mọi thiết bị", value: "" }].concat(
  DEVICES.map((d) => ({ label: d.name, value: d.id }))
);

// AL-09 — alert đang FIRING (đọc tlm_alert_state mirror).
const ActiveAlertsTab = () => {
  const dispatch = useDispatch();
  const { list, loading } = useSelector((s) => s.alert.activeAlerts);
  const [filters, setFilters] = useState({ severity: "", device_id: "" });
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const load = useCallback(() => {
    const f = filtersRef.current;
    const params = {};
    if (f.severity !== "") params.severity = f.severity;
    if (f.device_id !== "") params.device_id = f.device_id;
    dispatch(fetchActiveAlerts(params));
  }, [dispatch]);

  useEffect(() => {
    load();
  }, [load]);

  const set = (patch) => {
    setFilters((v) => ({ ...v, ...patch }));
  };

  return (
    <div>
      <div className="filter-bar" style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
        <Dropdown
          value={filters.severity}
          options={SEVERITY_FILTER}
          onChange={(e) => set({ severity: e.value })}
          style={{ fontSize: 13 }}
        />
        <Dropdown
          value={filters.device_id}
          options={DEVICE_FILTER}
          onChange={(e) => set({ device_id: e.value })}
          style={{ fontSize: 13 }}
        />
        <Button label="Làm mới" icon="pi pi-refresh" className="btn-search" onClick={load} />
        <span style={{ color: "#8c8c8c", fontSize: 12 }}>
          Cập nhật gần realtime (engine streaming; đọc mirror + history).
        </span>
      </div>

      <DataTable
        value={list}
        loading={loading}
        paginator
        rows={20}
        dataKey="rule_id"
        responsiveLayout="scroll"
        className="rule-table"
        emptyMessage="Không có alert đang FIRING"
        currentPageReportTemplate="Hiển thị {first} đến {last} trên tổng số {totalRecords} bản ghi"
        paginatorTemplate="CurrentPageReport FirstPageLink PrevPageLink PageLinks NextPageLink LastPageLink"
      >
        <Column header="fired_at" body={(r) => fmtTime(r.fired_at)} style={{ width: 118 }} />
        <Column header="Rule" field="rule_name" style={{ width: 180 }} />
        <Column
          header="Severity"
          body={(r) => <SeverityBadge severity={r.severity} />}
          style={{ width: 84 }}
        />
        <Column header="Device" body={(r) => `${r.device_name} (#${r.device_id})`} style={{ width: 128 }} />
        <Column
          header="Entity"
          body={(r) => r.entity_fingerprint || <em>device-level</em>}
          style={{ width: 168 }}
        />
        <Column header="Trigger" body={(r) => (r.trigger_value == null ? "—" : r.trigger_value)} style={{ width: 96 }} />
        <Column header="Threshold" body={(r) => (r.threshold == null ? "—" : r.threshold)} style={{ width: 92 }} />
        <Column
          header="Dedup"
          body={(r) => (
            <Tag
              value={r.dedup_state}
              severity={r.dedup_state === "SUPPRESSED" ? "warning" : "danger"}
            />
          )}
          style={{ width: 116 }}
        />
      </DataTable>
    </div>
  );
};

export default ActiveAlertsTab;
