import React, { useEffect, useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { Tag } from "primereact/tag";
import { fetchEngineStatus } from "../../redux/actions/alertActions";
import { errInfo } from "../../utils/apiError";
import { fmtTime } from "./fmt";

// AL-13 — sức khoẻ Flink Job 3 (Alert Evaluator).
const EngineStatusDialog = ({ visible, onHide }) => {
  const dispatch = useDispatch();
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!visible) return;
    setError(null);
    dispatch(fetchEngineStatus())
      .then((res) => setData(res.payload.data))
      .catch((rej) => setError(errInfo(rej).message));
  }, [visible, dispatch]);

  const Row = ({ label, children }) => (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #f0f0f0" }}>
      <span style={{ color: "#595959", fontSize: 13 }}>{label}</span>
      <span style={{ fontSize: 13, fontWeight: 500 }}>{children}</span>
    </div>
  );

  return (
    <Dialog header="Trạng thái Alert Engine — Flink Job 3 (AL-13)" visible={visible} style={{ width: 540 }} onHide={onHide} modal>
      {error && <div className="warning-box danger">{error}</div>}
      {data && (
        <div>
          <Row label="Job">
            {data.job_name}{" "}
            <Tag value={data.status} severity={data.status === "Running" ? "success" : "danger"} />
          </Row>
          <Row label="SLO ≤ 5s">
            <Tag value={data.slo_ok ? "ĐẠT" : "RỦI RO"} severity={data.slo_ok ? "success" : "danger"} />
          </Row>
          <Row label="eval latency">{data.eval_latency_ms} ms</Row>
          <Row label="watermark lag">{data.watermark_lag_ms} ms</Row>
          <Row label="consumer lag (processed_metrics)">{data.consumer_lag.processed_metrics}</Row>
          <Row label="consumer lag (derived_metrics)">{data.consumer_lag.derived_metrics}</Row>
          <Row label="checkpoint gần nhất">{fmtTime(data.last_checkpoint_at)}</Row>
          <Row label="rule đang Active">{data.active_rules_count}</Row>
          <Row label="số fire trong 1h">{data.fires_last_hour}</Row>
          <p style={{ color: "#8c8c8c", fontSize: 12, marginTop: 10 }}>{data.note}</p>
        </div>
      )}
    </Dialog>
  );
};

export default EngineStatusDialog;
