import React from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import "./AppTopbar.scss";

// Map route → breadcrumb label key
const ROUTE_LABELS = {
  "/flink-rules": "menu.flinkRules",
  "/derived-metrics": "menu.derivedMetrics",
  "/alert-rules": "menu.alert",
  "/pipeline-monitor": "menu.pipeline",
  "/data-explorer": "menu.dataExplorer",
};

const AppTopbar = () => {
  const { t } = useTranslation("common");
  const location = useLocation();
  const labelKey = ROUTE_LABELS[location.pathname] || "app.title";

  return (
    <header className="app-topbar">
      <div className="topbar-breadcrumb">
        <span className="crumb-muted">{t("app.title")}</span>
        <i className="pi pi-angle-right" />
        <span>{t(labelKey)}</span>
      </div>
      <div className="topbar-user">
        <i className="pi pi-user" />
        <span>operator</span>
      </div>
    </header>
  );
};

export default AppTopbar;
