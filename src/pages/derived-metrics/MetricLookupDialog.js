import React, { useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import { InputText } from "primereact/inputtext";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Message } from "primereact/message";
import { lookupMetricAlias } from "../../redux/actions/derivedActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";

// DM-07 — lookup alias_metric trải trên nhiều path để quyết định scope_path_alias_id.
const MetricLookupDialog = ({ visible, onHide, pathList }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const [q, setQ] = useState("");
  const [rows, setRows] = useState(null);

  const pathAlias = (id) => {
    const p = pathList.find((x) => x.id === Number(id));
    return p ? p.alias_path : "—";
  };

  const run = () => {
    if (!q) return;
    dispatch(lookupMetricAlias(q))
      .then((res) => setRows((res.payload.data || {}).items || []))
      .catch((rej) => toast.error(errInfo(rej).message));
  };

  const multiPath = rows && rows.length >= 2;

  return (
    <Dialog header="Lookup Metric Alias (DM-07)" visible={visible} style={{ width: 640 }} onHide={onHide} modal>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <InputText
          value={q}
          placeholder="Nhập alias_metric chính xác, vd: if_in_octets"
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
          style={{ flex: 1, fontSize: 13 }}
        />
        <Button label="Tra cứu" icon="pi pi-search" className="btn-search" onClick={run} />
      </div>

      {rows && (
        <>
          {multiPath ? (
            <Message
              severity="warn"
              style={{ display: "block", marginBottom: 8 }}
              text={`"${q}" xuất hiện ở ${rows.length} path. Cùng nghĩa (đa-vendor) → để scope_path_alias_id=null; khác nghĩa → đặt scope cụ thể.`}
            />
          ) : (
            <Message
              severity="info"
              style={{ display: "block", marginBottom: 8 }}
              text={rows.length ? `"${q}" chỉ ở 1 path — scope_path_alias_id=null là đủ.` : "Không tìm thấy alias_metric này."}
            />
          )}
          <DataTable value={rows} className="preview-table" emptyMessage="Không có kết quả" responsiveLayout="scroll">
            <Column header="alias_metric" field="alias_metric" />
            <Column header="Model Code" field="vendor_code" style={{ width: 110 }} />
            <Column
              header="path_alias_id"
              body={(r) => (r.path_alias_id == null ? "any-path" : `#${r.path_alias_id}`)}
              style={{ width: 110 }}
            />
            <Column header="alias_path" body={(r) => (r.path_alias_id == null ? "—" : pathAlias(r.path_alias_id))} />
          </DataTable>
        </>
      )}
    </Dialog>
  );
};

export default MetricLookupDialog;
