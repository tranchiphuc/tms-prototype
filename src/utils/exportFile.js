// ============================================================
// Tiện ích export — CSV + tải file ở trình duyệt.
// ============================================================

// Escape 1 ô CSV: bọc dấu " nếu chứa , " xuống dòng; "" cho null/undefined.
const cell = (v) => {
  if (v === null || v === undefined) return "";
  let s = typeof v === "object" ? JSON.stringify(v) : String(v);
  if (/[",\n]/.test(s)) s = `"${s.replace(/"/g, '""')}"`;
  return s;
};

// columns: [{ header, value: (row) => any }]
export const toCSV = (rows, columns) => {
  const head = columns.map((c) => cell(c.header)).join(",");
  const body = rows.map((r) => columns.map((c) => cell(c.value(r))).join(",")).join("\n");
  // BOM ﻿ để Excel mở UTF-8 đúng tiếng Việt
  return `﻿${head}\n${body}`;
};

export const downloadBlob = (filename, content, mime = "text/csv;charset=utf-8") => {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};

// YYYYMMDD-HHmm cho hậu tố tên file.
export const fileStamp = () => {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}`;
};
