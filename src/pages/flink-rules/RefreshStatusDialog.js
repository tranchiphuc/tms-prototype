import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import moment from "moment";
import { fetchRefreshStatus } from "../../redux/actions/flinkActions";

// FR-07: trạng thái hiệu lực rule (refresh-status)
const fmt = (v) => (v ? moment(v).format("YYYY-MM-DD HH:mm:ss") : "—");

const RefreshStatusDialog = ({ visible, onHide }) => {
  const dispatch = useDispatch();
  const status = useSelector((s) => s.flink.refreshStatus);

  useEffect(() => {
    if (visible) dispatch(fetchRefreshStatus());
  }, [visible, dispatch]);

  const tables = (status && status.tables) || [];

  return (
    <Dialog header="Trạng thái hiệu lực Rule (refresh-status)" visible={visible} style={{ width: 760 }} onHide={onHide} modal>
      <DataTable value={tables} responsiveLayout="scroll" style={{ fontSize: 13 }}>
        <Column field="table_name" header="Bảng" />
        <Column header="max_updated_at" body={(r) => fmt(r.max_updated_at)} />
        <Column
          header="rows_pending_push"
          body={(r) => (
            <span style={{ color: r.rows_pending_push > 0 ? "#fa8c16" : "#52c41a", fontWeight: 600 }}>
              {r.rows_pending_push}
            </span>
          )}
        />
        <Column header="last_push_completed_at" body={(r) => fmt(r.last_push_completed_at)} />
      </DataTable>
      {status && status.note && <div className="warning-box" style={{ marginTop: 10 }}>{status.note}</div>}
    </Dialog>
  );
};

export default RefreshStatusDialog;
