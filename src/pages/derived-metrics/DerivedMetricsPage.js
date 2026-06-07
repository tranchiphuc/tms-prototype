import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch } from "react-redux";
import { Button } from "primereact/button";
import { ConfirmDialog } from "primereact/confirmdialog";
import DerivedRulesList from "./DerivedRulesList";
import DerivedRuleDialog from "./DerivedRuleDialog";
import ObservabilityDialog from "./ObservabilityDialog";
import { exportDerivedRules } from "../../redux/actions/derivedActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { toCSV, downloadBlob, fileStamp } from "../../utils/exportFile";
import { DERIVE_KIND, AGG_FUNCTION } from "./derivedConstants";
import "./DerivedMetricsPage.scss";

const EXPORT_COLUMNS = [
  { header: "id", value: (r) => r.id },
  { header: "rule_name", value: (r) => r.rule_name },
  { header: "output_metric", value: (r) => r.output_metric },
  { header: "output_unit", value: (r) => r.output_unit },
  { header: "derive_kind", value: (r) => DERIVE_KIND[r.derive_kind] },
  { header: "input_metrics", value: (r) => (r.input_metrics || []).join("|") },
  { header: "expression", value: (r) => r.expression },
  { header: "assembly_window_seconds", value: (r) => r.assembly_window_seconds },
  { header: "window_seconds", value: (r) => r.window_seconds },
  { header: "agg_function", value: (r) => (r.agg_function == null ? "" : AGG_FUNCTION[r.agg_function]) },
  { header: "delta_scale_factor", value: (r) => r.delta_scale_factor },
  { header: "delta_reset_threshold", value: (r) => r.delta_reset_threshold },
  { header: "static_constants", value: (r) => r.static_constants },
  { header: "scope_path_alias_id", value: (r) => r.scope_path_alias_id },
  { header: "scope_device_ids", value: (r) => (r.scope_device_ids == null ? "" : r.scope_device_ids.join("|")) },
  { header: "priority", value: (r) => r.priority },
  { header: "status", value: (r) => (Number(r.status) === 1 ? "Active" : "Deprecated") },
  { header: "updated_at", value: (r) => r.updated_at },
];

const DerivedMetricsPage = () => {
  const { t } = useTranslation("derived");
  const dispatch = useDispatch();
  const toast = useToast();
  const [reloadToken, setReloadToken] = useState(0);
  const [editing, setEditing] = useState(null);
  const [dialogVisible, setDialogVisible] = useState(false);
  const [obsOpen, setObsOpen] = useState(false);
  const [exporting, setExporting] = useState(false);

  const reload = useCallback(() => setReloadToken((x) => x + 1), []);

  // Export toàn bộ derived rule (mọi trạng thái) ra CSV.
  const handleExport = useCallback(() => {
    setExporting(true);
    dispatch(exportDerivedRules({ status: "", page_size: 200, sort: "-updated_at" }))
      .then((res) => {
        const rows = (res.payload.data || {}).items || [];
        if (!rows.length) {
          toast.warn("Không có derived rule nào để xuất.");
          return;
        }
        downloadBlob(`derived-metrics-${fileStamp()}.csv`, toCSV(rows, EXPORT_COLUMNS));
        toast.success(`Đã xuất ${rows.length} derived rule ra CSV.`);
      })
      .catch((rej) => toast.error(errInfo(rej).message))
      .finally(() => setExporting(false));
  }, [dispatch, toast]);

  const openAdd = useCallback(() => {
    setEditing(null);
    setDialogVisible(true);
  }, []);

  const openEdit = useCallback((row) => {
    setEditing(row);
    setDialogVisible(true);
  }, []);

  return (
    <div className="page-container derived-metrics-page">
      <ConfirmDialog />
      <div className="page-header">
        <h1 className="page-title">{t("title")}</h1>
        <div className="page-actions">
          <Button
            label="Xuất CSV"
            icon="pi pi-cloud-download"
            className="btn-export p-button-sm"
            loading={exporting}
            onClick={handleExport}
          />
          <Button
            label="Observability (DM-08)"
            icon="pi pi-chart-bar"
            className="p-button-outlined p-button-sm"
            onClick={() => setObsOpen(true)}
          />
        </div>
      </div>

      <DerivedRulesList
        reloadToken={reloadToken}
        onAdd={openAdd}
        onEdit={openEdit}
        onReload={reload}
      />

      <DerivedRuleDialog
        visible={dialogVisible}
        initial={editing}
        onHide={() => setDialogVisible(false)}
        onSaved={reload}
      />
      <ObservabilityDialog visible={obsOpen} onHide={() => setObsOpen(false)} />
    </div>
  );
};

export default DerivedMetricsPage;
