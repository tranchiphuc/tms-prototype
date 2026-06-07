import React, { useCallback, useEffect, useState } from "react";
import AppSidebar from "./AppSidebar";
import AppTopbar from "./AppTopbar";
import "./AppLayout.scss";

const STORAGE_KEY = "sidebarCollapsed";

const AppLayout = ({ children }) => {
  const [collapsed, setCollapsed] = useState(() => localStorage.getItem(STORAGE_KEY) === "1");

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, collapsed ? "1" : "0");
  }, [collapsed]);

  const toggle = useCallback(() => setCollapsed((c) => !c), []);

  return (
    <div className={`app-layout${collapsed ? " collapsed" : ""}`}>
      <AppSidebar collapsed={collapsed} onToggle={toggle} />
      <div className="app-main">
        <AppTopbar />
        <div className="app-content">{children}</div>
      </div>
    </div>
  );
};

export default AppLayout;
