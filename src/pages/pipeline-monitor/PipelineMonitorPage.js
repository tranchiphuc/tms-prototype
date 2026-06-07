import React from "react";
import { useTranslation } from "react-i18next";

const PipelineMonitorPage = () => {
  const { t } = useTranslation("pipeline");
  return (
    <div className="page-container">
      <div className="page-header">
        <h1 className="page-title">{t("title")}</h1>
      </div>
      <p style={{ color: "#8c8c8c" }}>
        Phase 4 — Flink jobs / Kafka topics / ClickHouse sinks.
      </p>
    </div>
  );
};

export default PipelineMonitorPage;
