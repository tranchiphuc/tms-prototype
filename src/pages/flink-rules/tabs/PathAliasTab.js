import React from "react";
import RuleListShell from "../../../components/RuleListShell";
import PathAliasDialog from "../dialogs/PathAliasDialog";
import RulePreviewDialog from "../preview/RulePreviewDialog";
import FlinkExportButton from "../FlinkExportButton";
import { useRuleTab } from "../useRuleTab";

const PathAliasTab = () => {
  const tab = useRuleTab("pathAliases");

  const columns = [
    { field: "vendor_code", header: "Model Code", style: { width: 110 } },
    {
      field: "original_path",
      header: "Original Path",
      // width tường minh để tổng width cột > vùng nội dung → bật thanh cuộn ngang
      // (overflow-x:auto trên .rule-table) đồng nhất với Metric/Label Alias tab.
      // chuỗi path dài vẫn tự xuống dòng (CSS .rule-table), không bị cắt ký tự.
      style: { width: 500 },
      body: (r) => <span title={r.original_path}>{r.original_path}</span>,
    },
    { field: "alias_path", header: "Alias Path", isLink: true, onLinkClick: tab.openEdit, style: { width: 260 } },
  ];

  return (
    <>
      <RuleListShell
        slice="pathAliases"
        searchPlaceholder="Tìm theo path / alias..."
        columns={columns}
        onAdd={tab.openAdd}
        onEdit={tab.openEdit}
        onDelete={tab.handleDelete}
        onPreview={tab.openPreview}
        reloadToken={tab.reloadToken}
        toolbarExtra={<FlinkExportButton slice="pathAliases" />}
      />
      <PathAliasDialog
        visible={tab.dialogVisible}
        initial={tab.editing}
        onHide={tab.closeDialog}
        onSaved={tab.reload}
      />
      <RulePreviewDialog visible={!!tab.previewRow} slice="pathAliases" rule={tab.previewRow} onHide={tab.closePreview} />
    </>
  );
};

export default PathAliasTab;
