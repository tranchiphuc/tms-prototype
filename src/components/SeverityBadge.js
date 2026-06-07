import React from "react";

// Severity order (v6.2+): 0=info < 1=warning < 2=error < 3=critical
// critical là NẶNG NHẤT. Màu theo CLAUDE.md §7.
export const SEVERITY_META = {
  0: { label: "Info", color: "#1890ff" },
  1: { label: "Warning", color: "#faad14" },
  2: { label: "Error", color: "#ff7a45" },
  3: { label: "Critical", color: "#ff4d4f" },
};

const SeverityBadge = ({ severity }) => {
  const meta = SEVERITY_META[Number(severity)] || SEVERITY_META[0];
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: "10px",
        fontSize: "12px",
        fontWeight: 500,
        color: "#fff",
        background: meta.color,
      }}
    >
      {meta.label}
    </span>
  );
};

export default SeverityBadge;
