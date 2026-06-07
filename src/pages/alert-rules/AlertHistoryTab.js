import React, { useCallback, useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Button } from "primereact/button";
import { Dropdown } from "primereact/dropdown";
import { InputText } from "primereact/inputtext";
import { Tag } from "primereact/tag";
import SeverityBadge from "../../components/SeverityBadge";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { fetchAlertHistory, exportAlertHistory } from "../../redux/actions/alertActions";
import { toCSV, downloadBlob, fileStamp } from "../../utils/exportFile";
import {
  SEVERITY_FILTER,
  EVENT_TYPE_FILTER,
  TIME_RANGE_OPTIONS,
  SEVERITY,
  CONDITION_KIND,
} from "./alertConstants";
import { DEVICES } from "../../mock/devices";
import { fmtTime } from "./fmt";

const DEVICE_FILTER = [{ label: "Mọi thiết bị", value: "" }].concat(
  DEVICES.map((d) => ({ label: d.name, value: d.id }))
);

const EVENT_SEVERITY = { FIRED: "danger", REFIRED: "warning", RESOLVED: "success" };

const EXPORT_COLUMNS = [
  { header: "timestamp", value: (r) => r.timestamp },
  { header: "rule_id", value: (r) => r.rule_id },
  { header: "rule_name", value: (r) => r.rule_name },
  { header: "severity", value: (r) => SEVERITY[r.severity] },
  { header: "condition_kind", value: (r) => (r.condition_kind == null ? "" : CONDITION_KIND[r.condition_kind]) },
  { header: "device_name", value: (r) => r.device_name },
  { header: "entity_fingerprint", value: (r) => r.entity_fingerprint },
  { header: "trigger_value", value: (r) => r.trigger_value },
  { header: "threshold", value: (r) => r.threshold },
  { header: "event_type", value: (r) => r.event_type },
];

// AL-11 — alert history (FIRED/REFIRED/RESOLVED). Mặc định 24h, TTL 90 ngày.
const AlertHistoryTab = () => {
  const dispatch = useDispatch();
  const toast = useToast();
  const { list, loading } = useSelector((s) => s.alert.history);
  const [filters, setFilters] = useState({
    from_minutes: 1440,
    severity: "",
    device_id: "",
    event_type: "",
    q: "",
  });
  const [exporting, setExporting] = useState(false);
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const buildParams = () => {
    const f = filtersRef.current;
    const params = { from_minutes: f.from_minutes, page_size: 200 };
    if (f.severity !== "") params.severity = f.severity;
    if (f.device_id !== "") params.device_id = f.device_id;
    if (f.event_type) params.event_type = f.event_type;
    if (f.q) params.q = f.q;
    return params;
  };

  const load = useCallback(() => {
    dispatch(fetchAlertHistory(buildParams()));
  }, [dispatch]);

  useEffect(() => {
    load();
  }, [load]);

  const set = (patch) => setFilters((v) => ({ ...v, ...patch }));

  const handleExport = () => {
    setExporting(true);
    dispatch(exportAlertHistory({ ...buildParams(), page_size: 1000 }))
      .then((res) => {
        const rows = (res.payload.data || {}).items || [];
        if (!rows.length) {
          toast.warn("Không có sự kiện nào để xuất.");
          return;
        }
        downloadBlob(`alert-history-${fileStamp()}.csv`, toCSV(rows, EXPORT_COLUMNS));
        toast.success(`Đã xuất ${rows.length} sự kiện ra CSV.`);
      })
      .catch((rej) => toast.error(errInfo(rej).message))
      .finally(() => setExporting(false));
  };

  return (
    <div>
      <div className="filter-bar" style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 12 }}>
        <Dropdown
          value={filters.from_minutes}
          options={TIME_RANGE_OPTIONS}
          onChange={(e) => set({ from_minutes: e.value })}
          style={{ fontSize: 13 }}
        />
        <Dropdown value={filters.severity} options={SEVERITY_FILTER} onChange={(e) => set({ severity: e.value })} style={{ fontSize: 13 }} />
        <Dropdown value={filters.device_id} options={DEVICE_FILTER} onChange={(e) => set({ device_id: e.value })} style={{ fontSize: 13 }} />
        <Dropdown value={filters.event_type} options={EVENT_TYPE_FILTER} onChange={(e) => set({ event_type: e.value })} style={{ fontSize: 13 }} />
        <span className="p-input-icon-left">
          <i className="pi pi-search" />
          <InputText
            value={filters.q}
            placeholder="Tìm theo tên rule..."
            onChange={(e) => set({ q: e.target.value })}
            onKeyDown={(e) => e.key === "Enter" && load()}
            style={{ width: 200, fontSize: 13 }}
          />
        </span>
        <Button label="Lọc" icon="pi pi-filter" className="btn-search" onClick={load} />
        <Button label="Xuất CSV" icon="pi pi-cloud-download" className="btn-export p-button-sm" loading={exporting} onClick={handleExport} />
      </div>

      <DataTable
        value={list}
        loading={loading}
        paginator
        rows={20}
        rowsPerPageOptions={[20, 50, 100]}
        dataKey="timestamp"
        responsiveLayout="scroll"
        className="rule-table"
        emptyMessage="Không có sự kiện trong khoảng đã chọn"
        currentPageReportTemplate="Hiển thị {first} đến {last} trên tổng số {totalRecords} bản ghi"
        paginatorTemplate="CurrentPageReport FirstPageLink PrevPageLink PageLinks NextPageLink LastPageLink RowsPerPageDropdown"
      >
        <Column header="Thời điểm" body={(r) => fmtTime(r.timestamp)} sortable field="timestamp" style={{ width: 128 }} />
        <Column header="Rule" field="rule_name" style={{ width: 200 }} />
        <Column header="Severity" body={(r) => <SeverityBadge severity={r.severity} />} style={{ width: 84 }} />
        <Column header="Device" body={(r) => `${r.device_name} (#${r.device_id})`} style={{ width: 128 }} />
        <Column header="Entity" body={(r) => r.entity_fingerprint || <em>device</em>} style={{ width: 160 }} />
        <Column header="Trigger" body={(r) => (r.trigger_value == null ? "—" : r.trigger_value)} style={{ width: 100 }} />
        <Column
          header="Sự kiện"
          body={(r) => <Tag value={r.event_type} severity={EVENT_SEVERITY[r.event_type]} />}
          style={{ width: 100 }}
        />
      </DataTable>
    </div>
  );
};

export default AlertHistoryTab;
