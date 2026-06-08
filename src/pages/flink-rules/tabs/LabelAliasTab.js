import React from "react";
import RuleListShell from "../../../components/RuleListShell";
import LabelAliasDialog from "../dialogs/LabelAliasDialog";
import RulePreviewDialog from "../preview/RulePreviewDialog";
import FlinkExportButton from "../FlinkExportButton";
import { useRuleTab } from "../useRuleTab";

const LV_LABEL = { 0: "identity", 1: "regex", 2: "enum_mapping" };

const LabelAliasTab = () => {
  const tab = useRuleTab("labelAliases");

  const columns = [
    { field: "vendor_code", header: "Model Code", style: { width: 100 } },
    // width tường minh cho mọi cột: dưới table-layout:fixed, cột thiếu width
    // bị co về ~0 → header vỡ thành chữ dọc (xem PROGRESS Gotcha #3).
    { field: "original_key", header: "Original Key", style: { width: 220 } },
    { field: "alias_key", header: "Alias Key", isLink: true, onLinkClick: tab.openEdit, style: { width: 160 } },
    { field: "lv_kind", header: "lv_kind", body: (r) => LV_LABEL[r.lv_kind], style: { width: 130 } },
    {
      field: "lv_pattern",
      header: "Pattern / Mapping",
      style: { width: 260 },
      body: (r) =>
        r.lv_kind === 1
          ? <code style={{ fontSize: 12 }}>{r.lv_pattern} → {r.lv_replace}</code>
          : r.lv_kind === 2
          ? <code style={{ fontSize: 12 }}>{JSON.stringify(r.lv_mapping)}</code>
          : "—",
    },
    { field: "priority", header: "Priority", style: { width: 90 } },
  ];

  return (
    <>
      <RuleListShell
        slice="labelAliases"
        searchPlaceholder="Tìm theo key..."
        columns={columns}
        onAdd={tab.openAdd}
        onEdit={tab.openEdit}
        onDelete={tab.handleDelete}
        onPreview={tab.openPreview}
        reloadToken={tab.reloadToken}
        toolbarExtra={<FlinkExportButton slice="labelAliases" />}
      />
      <LabelAliasDialog visible={tab.dialogVisible} initial={tab.editing} onHide={tab.closeDialog} onSaved={tab.reload} />
      <RulePreviewDialog visible={!!tab.previewRow} slice="labelAliases" rule={tab.previewRow} onHide={tab.closePreview} />
    </>
  );
};

export default LabelAliasTab;
