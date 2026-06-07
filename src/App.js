import React from "react";
import { Switch, Route, Redirect } from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ToastProvider } from "./components/ToastProvider";

import FlinkRulesPage from "./pages/flink-rules/FlinkRulesPage";
import DerivedMetricsPage from "./pages/derived-metrics/DerivedMetricsPage";
import AlertRulesPage from "./pages/alert-rules/AlertRulesPage";
import PipelineMonitorPage from "./pages/pipeline-monitor/PipelineMonitorPage";
import DataExplorerPage from "./pages/data-explorer/DataExplorerPage";

const App = () => {
  return (
    <ToastProvider>
      <AppLayout>
        <Switch>
        <Route exact path="/" render={() => <Redirect to="/flink-rules" />} />
        <Route path="/flink-rules" component={FlinkRulesPage} />
        <Route path="/derived-metrics" component={DerivedMetricsPage} />
        <Route path="/alert-rules" component={AlertRulesPage} />
        <Route path="/pipeline-monitor" component={PipelineMonitorPage} />
        <Route path="/data-explorer" component={DataExplorerPage} />
        <Route render={() => <Redirect to="/flink-rules" />} />
        </Switch>
      </AppLayout>
    </ToastProvider>
  );
};

export default App;
