import React from "react";
import { Tag } from "primereact/tag";
import RuleListShell from "../../../components/RuleListShell";
import FilterRuleDialog from "../dialogs/FilterRuleDialog";
import RulePreviewDialog from "../preview/RulePreviewDialog";
import FlinkExportButton from "../FlinkExportButton";
import { useRuleTab } from "../useRuleTab";

const FilterRuleTab = () => {
  const tab = useRuleTab("filterRules");

  const columns = [
    { field: "vendor_code", header: "Model Code", style: { width: 100 } },
    {
      field: "filter_action",
      header: "Action",
      style: { width: 130 },
      body: (r) =>
        Number(r.filter_action) === 0 ? (
          <Tag value="EXCLUDE" severity="danger" />
        ) : (
          <Tag value="INCLUDE" severity="success" />
        ),
    },
    // width tường minh cho mọi cột để tổng > vùng nội dung → bật thanh cuộn ngang
    // (overflow-x:auto trên .rule-table) đồng nhất với Metric/Label Alias tab.
    { field: "match_path", header: "match_path", style: { width: 240 }, body: (r) => r.match_path || <em style={{ color: "#8c8c8c" }}>mọi path</em> },
    { field: "match_metric", header: "match_metric", style: { width: 200 }, body: (r) => r.match_metric || <em style={{ color: "#8c8c8c" }}>mọi metric</em> },
    { field: "filter_expression", header: "Expression", style: { width: 280 }, body: (r) => r.filter_expression || <em style={{ color: "#8c8c8c" }}>luôn TRUE</em> },
    { field: "priority", header: "Priority", style: { width: 90 } },
  ];

  return (
    <>
      <RuleListShell
        slice="filterRules"
        searchPlaceholder="Tìm theo path / metric / expression..."
        columns={columns}
        onAdd={tab.openAdd}
        onEdit={tab.openEdit}
        onDelete={tab.handleDelete}
        onPreview={tab.openPreview}
        reloadToken={tab.reloadToken}
        toolbarExtra={<FlinkExportButton slice="filterRules" />}
      />
      <FilterRuleDialog
        visible={tab.dialogVisible}
        initial={tab.editing}
        onHide={tab.closeDialog}
        onSaved={tab.reload}
        onPreview={(values) => tab.openPreview(values)}
      />
      <RulePreviewDialog visible={!!tab.previewRow} slice="filterRules" rule={tab.previewRow} onHide={tab.closePreview} />
    </>
  );
};

export default FilterRuleTab;
