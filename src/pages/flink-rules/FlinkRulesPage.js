import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import { TabView, TabPanel } from "primereact/tabview";
import { Button } from "primereact/button";
import { ConfirmDialog } from "primereact/confirmdialog";
import PathAliasTab from "./tabs/PathAliasTab";
import MetricAliasTab from "./tabs/MetricAliasTab";
import LabelAliasTab from "./tabs/LabelAliasTab";
import FilterRuleTab from "./tabs/FilterRuleTab";
import FallthroughMonitor from "./FallthroughMonitor";
import RefreshStatusDialog from "./RefreshStatusDialog";
import "./FlinkRulesPage.scss";

const FlinkRulesPage = () => {
  const { t } = useTranslation("flink");
  const [activeIndex, setActiveIndex] = useState(0);
  const [fallthroughOpen, setFallthroughOpen] = useState(false);
  const [refreshOpen, setRefreshOpen] = useState(false);

  return (
    <div className="page-container flink-rules-page">
      <ConfirmDialog />
      <div className="page-header">
        <h1 className="page-title">{t("title")}</h1>
        <div className="page-actions">
          {/* refresh-status áp cho cả 4 bảng → đặt ở cấp trang, không trong tab */}
          <Button
            label="Refresh status"
            icon="pi pi-clock"
            className="btn-rescan p-button-sm"
            onClick={() => setRefreshOpen(true)}
          />
          <Button
            label="Fallthrough Monitor"
            icon="pi pi-search-plus"
            className="p-button-outlined p-button-sm"
            onClick={() => setFallthroughOpen(true)}
          />
        </div>
      </div>

      <TabView activeIndex={activeIndex} onTabChange={(e) => setActiveIndex(e.index)}>
        <TabPanel header={t("tab.pathAlias")}>
          {activeIndex === 0 && <PathAliasTab />}
        </TabPanel>
        <TabPanel header={t("tab.metricAlias")}>
          {activeIndex === 1 && <MetricAliasTab />}
        </TabPanel>
        <TabPanel header={t("tab.labelAlias")}>
          {activeIndex === 2 && <LabelAliasTab />}
        </TabPanel>
        <TabPanel header={t("tab.filterRule")}>
          {activeIndex === 3 && <FilterRuleTab />}
        </TabPanel>
      </TabView>

      <FallthroughMonitor visible={fallthroughOpen} onHide={() => setFallthroughOpen(false)} />
      <RefreshStatusDialog visible={refreshOpen} onHide={() => setRefreshOpen(false)} />
    </div>
  );
};

export default FlinkRulesPage;
