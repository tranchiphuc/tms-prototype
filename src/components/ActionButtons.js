import React from "react";
import { Button } from "primereact/button";
import { useTranslation } from "react-i18next";

// Nhóm action button góc phải header (CLAUDE.md §7).
// Truyền các handler tùy chọn; chỉ render button nào có handler.
const ActionButtons = ({ onSearch, onAdd, onRescan, onExport, onImport }) => {
  const { t } = useTranslation("common");

  return (
    <div className="page-actions">
      {onSearch && (
        <Button
          label={t("action.search")}
          icon="pi pi-search"
          className="btn-search"
          onClick={onSearch}
        />
      )}
      {onAdd && (
        <Button
          label={t("action.add")}
          icon="pi pi-plus"
          className="btn-add"
          onClick={onAdd}
        />
      )}
      {onRescan && (
        <Button
          label={t("action.rescan")}
          icon="pi pi-refresh"
          className="btn-rescan"
          onClick={onRescan}
        />
      )}
      {onExport && (
        <Button
          label={t("action.export")}
          icon="pi pi-cloud-download"
          className="btn-export"
          onClick={onExport}
        />
      )}
      {onImport && (
        <Button
          label={t("action.import")}
          icon="pi pi-cloud-upload"
          className="btn-import"
          onClick={onImport}
        />
      )}
    </div>
  );
};

export default ActionButtons;
