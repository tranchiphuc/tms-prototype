import React from "react";
import { useTranslation } from "react-i18next";

const DataExplorerPage = () => {
  const { t } = useTranslation("explorer");
  return (
    <div className="page-container">
      <div className="page-header">
        <h1 className="page-title">{t("title")}</h1>
      </div>
      <p style={{ color: "#8c8c8c" }}>
        Phase 5 — Query nhanh ipms.tlm_metrics (raw vs derived) + raw payload.
      </p>
    </div>
  );
};

export default DataExplorerPage;
