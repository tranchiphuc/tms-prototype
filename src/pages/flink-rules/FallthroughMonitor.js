import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Button } from "primereact/button";
import { fetchFallthrough } from "../../redux/actions/flinkActions";

// FR-08 (N): Fallthrough Monitor — metric chưa khớp Path/Metric Alias
const FallthroughMonitor = ({ visible, onHide, onCreateAlias }) => {
  const dispatch = useDispatch();
  const data = useSelector((s) => s.flink.fallthrough);

  useEffect(() => {
    if (visible) dispatch(fetchFallthrough());
  }, [visible, dispatch]);

  return (
    <Dialog header="Fallthrough Monitor (metric chưa cấu hình alias)" visible={visible} style={{ width: 900 }} onHide={onHide} modal>
      <DataTable value={data.items || []} responsiveLayout="scroll" className="preview-table" style={{ fontSize: 13 }} paginator rows={10}>
        <Column field="vendor_code" header="Model Code" style={{ width: "10%" }} />
        <Column field="device_name" header="Thiết bị" style={{ width: "11%" }} />
        <Column field="raw_path" header="raw_path (auto-derived)" style={{ width: "27%" }} />
        <Column field="raw_metric_name" header="raw_metric_name (full leaf)" style={{ width: "30%" }} />
        <Column field="occurrences" header="Số lần" sortable style={{ width: "9%" }} />
        <Column
          header="Hành động"
          style={{ width: "13%" }}
          body={(r) => (
            <Button
              label="Tạo Alias"
              icon="pi pi-plus"
              className="p-button-sm p-button-outlined"
              onClick={() => onCreateAlias && onCreateAlias(r)}
            />
          )}
        />
      </DataTable>
      {data.note && <div className="warning-box" style={{ marginTop: 10 }}>{data.note}</div>}
    </Dialog>
  );
};

export default FallthroughMonitor;
