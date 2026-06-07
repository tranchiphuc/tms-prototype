import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch } from "react-redux";
import { TabView, TabPanel } from "primereact/tabview";
import { Button } from "primereact/button";
import { ConfirmDialog } from "primereact/confirmdialog";
import AlertRulesList from "./AlertRulesList";
import AlertRuleDialog from "./AlertRuleDialog";
import RuleStateDialog from "./RuleStateDialog";
import ActiveAlertsTab from "./ActiveAlertsTab";
import AlertHistoryTab from "./AlertHistoryTab";
import EngineStatusDialog from "./EngineStatusDialog";
import { exportAlertRules } from "../../redux/actions/alertActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { toCSV, downloadBlob, fileStamp } from "../../utils/exportFile";
import { RULE_KIND, CONDITION_KIND, COMPARATOR, SEVERITY, LOGICAL_OP, STATUS } from "./alertConstants";
import "./AlertRulesPage.scss";

const EXPORT_COLUMNS = [
  { header: "id", value: (r) => r.id },
  { header: "rule_name", value: (r) => r.rule_name },
  { header: "rule_kind", value: (r) => RULE_KIND[r.rule_kind] },
  { header: "condition_kind", value: (r) => (r.condition_kind == null ? "" : CONDITION_KIND[r.condition_kind]) },
  { header: "severity", value: (r) => SEVERITY[r.severity] },
  { header: "alias_metric", value: (r) => r.alias_metric },
  { header: "path_alias_id", value: (r) => r.path_alias_id },
  { header: "entity_keys", value: (r) => (r.entity_keys || []).join("|") },
  { header: "entity_filter", value: (r) => r.entity_filter },
  { header: "comparator", value: (r) => (r.comparator == null ? "" : COMPARATOR[r.comparator]) },
  { header: "threshold", value: (r) => r.threshold },
  { header: "pct_abs", value: (r) => r.pct_abs },
  { header: "no_data_seconds", value: (r) => r.no_data_seconds },
  { header: "logical_op", value: (r) => (r.logical_op == null ? "" : LOGICAL_OP[r.logical_op]) },
  { header: "child_rule_ids", value: (r) => (r.child_rule_ids == null ? "" : r.child_rule_ids.join("|")) },
  { header: "missing_as", value: (r) => r.missing_as },
  { header: "sustain_samples", value: (r) => r.sustain_samples },
  { header: "dedup_seconds", value: (r) => r.dedup_seconds },
  { header: "emit_independent", value: (r) => r.emit_independent },
  { header: "scope_device_ids", value: (r) => (r.scope_device_ids == null ? "" : r.scope_device_ids.join("|")) },
  { header: "status", value: (r) => STATUS[r.status] },
  { header: "updated_at", value: (r) => r.updated_at },
];

const AlertRulesPage = () => {
  const { t } = useTranslation("alert");
  const dispatch = useDispatch();
  const toast = useToast();
  const [activeIndex, setActiveIndex] = useState(0);
  const [reloadToken, setReloadToken] = useState(0);
  const [editing, setEditing] = useState(null);
  const [dialogVisible, setDialogVisible] = useState(false);
  const [stateRule, setStateRule] = useState(null);
  const [engineOpen, setEngineOpen] = useState(false);
  const [exporting, setExporting] = useState(false);

  const reload = useCallback(() => setReloadToken((x) => x + 1), []);

  const handleExport = useCallback(() => {
    setExporting(true);
    dispatch(exportAlertRules({ status: "all", page_size: 200, sort: "-updated_at" }))
      .then((res) => {
        const rows = (res.payload.data || {}).items || [];
        if (!rows.length) {
          toast.warn("Không có rule nào để xuất.");
          return;
        }
        downloadBlob(`alert-rules-${fileStamp()}.csv`, toCSV(rows, EXPORT_COLUMNS));
        toast.success(`Đã xuất ${rows.length} rule ra CSV.`);
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
    <div className="page-container alert-rules-page">
      <ConfirmDialog />
      <div className="page-header">
        <h1 className="page-title">{t("title")}</h1>
        <div className="page-actions">
          <Button
            label="Engine status (AL-13)"
            icon="pi pi-chart-line"
            className="p-button-outlined p-button-sm"
            onClick={() => setEngineOpen(true)}
          />
          {activeIndex === 0 && (
            <Button
              label="Xuất CSV"
              icon="pi pi-cloud-download"
              className="btn-export p-button-sm"
              loading={exporting}
              onClick={handleExport}
            />
          )}
        </div>
      </div>

      <TabView activeIndex={activeIndex} onTabChange={(e) => setActiveIndex(e.index)}>
        <TabPanel header={t("tab.ruleList")}>
          {activeIndex === 0 && (
            <AlertRulesList
              reloadToken={reloadToken}
              onAdd={openAdd}
              onEdit={openEdit}
              onReload={reload}
              onState={setStateRule}
            />
          )}
        </TabPanel>
        <TabPanel header={t("tab.activeAlerts")}>
          {activeIndex === 1 && <ActiveAlertsTab />}
        </TabPanel>
        <TabPanel header={t("tab.history")}>
          {activeIndex === 2 && <AlertHistoryTab />}
        </TabPanel>
      </TabView>

      <AlertRuleDialog
        visible={dialogVisible}
        initial={editing}
        onHide={() => setDialogVisible(false)}
        onSaved={reload}
      />
      <RuleStateDialog rule={stateRule} onHide={() => setStateRule(null)} />
      <EngineStatusDialog visible={engineOpen} onHide={() => setEngineOpen(false)} />
    </div>
  );
};

export default AlertRulesPage;
