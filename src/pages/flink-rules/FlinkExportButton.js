import React, { useState } from "react";
import { useDispatch } from "react-redux";
import { Button } from "primereact/button";
import { exportList } from "../../redux/actions/flinkActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { toCSV, downloadBlob, fileStamp } from "../../utils/exportFile";
import { EXPORT_CONFIG } from "./exportColumns";

// Nút "Xuất CSV" cho 1 group Flink Rule. Tải TOÀN BỘ rule của group
// (mọi trạng thái) — không phụ thuộc bộ lọc đang hiển thị.
const FlinkExportButton = ({ slice }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const [exporting, setExporting] = useState(false);
  const cfg = EXPORT_CONFIG[slice];

  const handleExport = () => {
    setExporting(true);
    dispatch(exportList(slice, { status: "", page_size: 200, sort: "-updated_at" }))
      .then((res) => {
        const rows = (res.payload.data || {}).items || [];
        if (!rows.length) {
          toast.warn("Không có bản ghi nào để xuất.");
          return;
        }
        downloadBlob(`${cfg.filenameBase}-${fileStamp()}.csv`, toCSV(rows, cfg.columns));
        toast.success(`Đã xuất ${rows.length} bản ghi ra CSV.`);
      })
      .catch((rej) => toast.error(errInfo(rej).message))
      .finally(() => setExporting(false));
  };

  return (
    <Button
      label="Xuất CSV"
      icon="pi pi-cloud-download"
      className="btn-export p-button-sm"
      loading={exporting}
      onClick={handleExport}
    />
  );
};

export default FlinkExportButton;
