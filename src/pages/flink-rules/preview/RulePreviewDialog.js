import React, { useEffect, useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { ProgressSpinner } from "primereact/progressspinner";
import { previewRule } from "../../../redux/actions/flinkActions";

// FR-06: preview/dry-run một rule trên mẫu metric thực gần nhất.
const RulePreviewDialog = ({ visible, slice, rule, onHide }) => {
  const dispatch = useDispatch();
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (visible && rule) {
      setLoading(true);
      setResult(null);
      dispatch(previewRule(slice, rule))
        .then((a) => setResult(a.payload.data))
        .catch(() => setResult(null))
        .finally(() => setLoading(false));
    }
  }, [visible, rule, slice, dispatch]);

  return (
    <Dialog header="Preview / Dry-run" visible={visible} style={{ width: 760 }} onHide={onHide} modal>
      {loading && (
        <div style={{ textAlign: "center", padding: 20 }}>
          <ProgressSpinner style={{ width: 40, height: 40 }} />
        </div>
      )}
      {!loading && result && (
        <div>
          <div style={{ display: "flex", gap: 24, marginBottom: 12, fontSize: 13 }}>
            <span>
              Sampled: <strong>{result.sampled_records}</strong>
            </span>
            <span>
              Matched: <strong style={{ color: "#1890ff" }}>{result.matched_records}</strong>
            </span>
            {result.affected != null && (
              <span>
                {Number(result.filter_action) === 0 ? "Sẽ DROP" : "Sẽ GIỮ"}:{" "}
                <strong style={{ color: "#ff4d4f" }}>{result.affected}</strong>
              </span>
            )}
          </div>

          {result.note && <div className="warning-box">{result.note}</div>}

          {slice === "metricAliases" && (
            <DataTable value={result.samples} responsiveLayout="scroll" className="preview-table" style={{ fontSize: 13 }}>
              <Column field="raw_metric_name" header="raw_metric_name" style={{ width: "40%" }} />
              <Column header="before" body={(r) => `${r.before.metric_name} = ${r.before.value_number} ${r.before.unit}`} style={{ width: "30%" }} />
              <Column header="after" body={(r) => `${r.after.metric_name} = ${r.after.value_number} ${r.after.unit}`} style={{ width: "30%" }} />
            </DataTable>
          )}
          {slice === "filterRules" && (
            <DataTable value={result.samples} responsiveLayout="scroll" style={{ fontSize: 13 }}>
              <Column field="device_name" header="Thiết bị" />
              <Column field="metric_name" header="Metric" />
              <Column field="value_number" header="Value" />
              <Column header="Quyết định" body={(r) => (
                <span style={{ color: r.decision === "DROP" ? "#ff4d4f" : "#52c41a", fontWeight: 600 }}>{r.decision}</span>
              )} />
            </DataTable>
          )}
          {slice === "labelAliases" && (
            <DataTable value={result.samples} responsiveLayout="scroll" style={{ fontSize: 13 }}>
              <Column field="original_key" header="Original Key" />
              <Column field="original_value" header="Value gốc" />
              <Column field="alias_key" header="Alias Key" />
              <Column field="alias_value" header="Value sau transform" />
            </DataTable>
          )}
          {slice === "pathAliases" && (
            <DataTable value={result.samples} responsiveLayout="scroll" className="preview-table" style={{ fontSize: 13 }}>
              <Column field="raw_path" header="raw_path" style={{ width: "50%" }} />
              <Column field="alias_path" header="alias_path" style={{ width: "30%" }} />
              <Column field="path_id" header="path_id" style={{ width: "20%" }} />
            </DataTable>
          )}

          <p className="field-hint" style={{ marginTop: 10 }}>
            Preview phản ánh tác động <strong>một-rule, một-thời-điểm</strong> — không mô phỏng tương
            tác priority giữa nhiều rule.
          </p>
        </div>
      )}
    </Dialog>
  );
};

export default RulePreviewDialog;
