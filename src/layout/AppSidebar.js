import React from "react";
import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import "./AppSidebar.scss";

// Cấu trúc menu: nhóm → item (đồng bộ Phụ Lục B user-stories)
const MENU = [
  {
    group: "menu.config",
    icon: "pi pi-cog",
    items: [
      { to: "/flink-rules", label: "menu.flinkRules", icon: "pi pi-filter" },
      { to: "/derived-metrics", label: "menu.derivedMetrics", icon: "pi pi-sitemap" },
    ],
  },
  {
    group: "menu.alert",
    icon: "pi pi-bell",
    items: [{ to: "/alert-rules", label: "menu.alert", icon: "pi pi-bell" }],
  },
  {
    group: "menu.monitor",
    icon: "pi pi-desktop",
    items: [{ to: "/pipeline-monitor", label: "menu.pipeline", icon: "pi pi-desktop" }],
  },
  {
    group: "menu.data",
    icon: "pi pi-database",
    items: [{ to: "/data-explorer", label: "menu.dataExplorer", icon: "pi pi-search" }],
  },
];

const AppSidebar = ({ collapsed, onToggle }) => {
  const { t } = useTranslation("common");

  return (
    <aside className="app-sidebar">
      <div className="sidebar-brand" title={collapsed ? t("app.title") : undefined}>
        <i className="pi pi-server" />
        <span>{t("app.title")}</span>
      </div>
      <nav className="sidebar-nav">
        {MENU.map((section) => (
          <div className="sidebar-section" key={section.group}>
            <div className="sidebar-section-title">{t(section.group)}</div>
            {section.items.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className="sidebar-item"
                activeClassName="active"
                title={collapsed ? t(item.label) : undefined}
              >
                <i className={item.icon} />
                <span>{t(item.label)}</span>
              </NavLink>
            ))}
          </div>
        ))}
      </nav>
      <button
        type="button"
        className="sidebar-collapse-btn"
        onClick={onToggle}
        title={collapsed ? "Mở rộng menu" : "Thu nhỏ menu"}
        aria-label="Toggle sidebar"
      >
        <i className={collapsed ? "pi pi-angle-double-right" : "pi pi-angle-double-left"} />
        {!collapsed && <span>Thu nhỏ menu</span>}
      </button>
    </aside>
  );
};

export default AppSidebar;
