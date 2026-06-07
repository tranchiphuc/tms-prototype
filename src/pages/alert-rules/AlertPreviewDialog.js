import React, { useEffect, useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Tag } from "primereact/tag";
import { previewAlertRule } from "../../redux/actions/alertActions";
import { errInfo } from "../../utils/apiError";

// AL-07 — dry-run boolean tức thời trên dữ liệu mẫu.
const AlertPreviewDialog = ({ body, onHide }) => {
  const dispatch = useDispatch();
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!body) {
      setResult(null);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);
    dispatch(previewAlertRule(body))
      .then((res) => setResult(res.payload.data))
      .catch((rej) => setError(errInfo(rej).message))
      .finally(() => setLoading(false));
  }, [body, dispatch]);

  return (
    <Dialog header="Preview / dry-run (AL-07)" visible={!!body} style={{ width: 720 }} onHide={onHide} modal>
      {loading && <p>Đang đánh giá...</p>}
      {error && <div className="warning-box danger">{error}</div>}
      {result && !loading && (
        <div>
          <div style={{ display: "flex", gap: 16, marginBottom: 10 }}>
            <Tag value={`Đánh giá: ${result.evaluated_series}`} />
            <Tag value={`Khớp (would_fire): ${result.matching_series}`} severity="danger" />
            <Tag value={`Bỏ qua: ${result.skipped_series}`} severity="warning" />
          </div>
          {(result.warnings || []).map((w, i) => (
            <div key={i} className="warning-box">{w}</div>
          ))}
          <DataTable value={result.samples || []} className="preview-table" responsiveLayout="scroll" emptyMessage="Không có series mẫu">
            <Column header="Device" body={(r) => `${r.device_name} (#${r.device_id})`} />
            <Column header="Entity" body={(r) => r.entity || <em>device</em>} />
            <Column header="cur" field="cur_value" />
            <Column header="prev" body={(r) => (r.prev_value == null ? "—" : r.prev_value)} />
            <Column header="observed" body={(r) => (r.observed_value == null ? r.cur_value : r.observed_value)} />
            <Column
              header="would_fire"
              body={(r) => (
                <Tag value={r.would_fire ? "FIRE" : "—"} severity={r.would_fire ? "danger" : null} />
              )}
            />
          </DataTable>
          <p style={{ color: "#8c8c8c", fontSize: 12, marginTop: 8 }}>{result.note}</p>
        </div>
      )}
    </Dialog>
  );
};

export default AlertPreviewDialog;
