import React from "react";
import RuleListShell from "../../../components/RuleListShell";
import MetricAliasDialog from "../dialogs/MetricAliasDialog";
import RulePreviewDialog from "../preview/RulePreviewDialog";
import FlinkExportButton from "../FlinkExportButton";
import { useRuleTab } from "../useRuleTab";

const TK_LABEL = { 0: "identity", 1: "linear", 2: "expression", 3: "enum_mapping" };

const MetricAliasTab = () => {
  const tab = useRuleTab("metricAliases");

  const columns = [
    { field: "vendor_code", header: "Model Code", style: { width: 100 } },
    { field: "alias_metric", header: "Alias Metric", isLink: true, onLinkClick: tab.openEdit, style: { width: 180 } },
    // cần width rõ ràng: dưới table-layout:fixed, cột không có width sẽ bị co
    // về ~0 khi các cột khác đã chiếm hết → text vỡ thành chữ dọc.
    { field: "original_name", header: "Original Name", style: { width: 220 } },
    {
      field: "path_alias_id",
      header: "Path scope",
      body: (r) => (r.path_alias_id == null ? <em style={{ color: "#8c8c8c" }}>any-path</em> : `#${r.path_alias_id}`),
      style: { width: 110 },
    },
    { field: "transform_kind", header: "Transform", body: (r) => TK_LABEL[r.transform_kind], style: { width: 120 } },
    { field: "priority", header: "Priority", style: { width: 90 } },
  ];

  return (
    <>
      <RuleListShell
        slice="metricAliases"
        searchPlaceholder="Tìm theo alias / original name..."
        columns={columns}
        onAdd={tab.openAdd}
        onEdit={tab.openEdit}
        onDelete={tab.handleDelete}
        onPreview={tab.openPreview}
        reloadToken={tab.reloadToken}
        toolbarExtra={<FlinkExportButton slice="metricAliases" />}
      />
      <MetricAliasDialog visible={tab.dialogVisible} initial={tab.editing} onHide={tab.closeDialog} onSaved={tab.reload} />
      <RulePreviewDialog visible={!!tab.previewRow} slice="metricAliases" rule={tab.previewRow} onHide={tab.closePreview} />
    </>
  );
};

export default MetricAliasTab;
