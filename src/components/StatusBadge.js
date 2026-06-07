import React from "react";
import { Tag } from "primereact/tag";

// status: 1 = Active, 0 = Inactive/Deprecated (CLAUDE.md §4.1)
const StatusBadge = ({ status, activeLabel = "Active", inactiveLabel = "Deprecated" }) => {
  const isActive = Number(status) === 1;
  return (
    <Tag
      value={isActive ? activeLabel : inactiveLabel}
      severity={isActive ? "success" : "warning"}
      style={{ fontSize: "12px" }}
    />
  );
};

export default StatusBadge;
