import React, { useEffect, useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Tag } from "primereact/tag";
import { fetchRuleState } from "../../redux/actions/alertActions";
import { errInfo } from "../../utils/apiError";

// AL-10 — per-instance firing state (read-only, để debug vì sao bị suppress).
const RuleStateDialog = ({ rule, onHide }) => {
  const dispatch = useDispatch();
  const [instances, setInstances] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!rule) {
      setInstances([]);
      return;
    }
    setLoading(true);
    setError(null);
    dispatch(fetchRuleState(rule.id))
      .then((res) => setInstances((res.payload.data || {}).instances || []))
      .catch((rej) => setError(errInfo(rej).message))
      .finally(() => setLoading(false));
  }, [rule, dispatch]);

  return (
    <Dialog
      header={rule ? `Firing state — ${rule.rule_name} (AL-10)` : ""}
      visible={!!rule}
      style={{ width: 760 }}
      onHide={onHide}
      modal
    >
      {loading && <p>Đang tải...</p>}
      {error && <div className="warning-box danger">{error}</div>}
      {!loading && !error && (
        <DataTable value={instances} responsiveLayout="scroll" emptyMessage="Chưa có instance nào (rule chưa từng được đánh giá)">
          <Column header="Device" body={(r) => `${r.device_name} (#${r.device_id})`} />
          <Column header="entity_fingerprint" body={(r) => r.entity_fingerprint || <em>device-level</em>} />
          <Column
            header="last_state"
            body={(r) => (
              <Tag value={r.last_state} severity={r.last_state === "FIRING" ? "danger" : "success"} />
            )}
          />
          <Column header="last_fired_at" body={(r) => fmt(r.last_fired_at)} />
          <Column header="last_resolved_at" body={(r) => fmt(r.last_resolved_at)} />
          <Column
            header="dedup còn lại"
            body={(r) =>
              r.dedup_remaining_seconds > 0 ? (
                <Tag value={`${r.dedup_remaining_seconds}s (SUPPRESSED)`} severity="warning" />
              ) : (
                "0s"
              )
            }
          />
        </DataTable>
      )}
    </Dialog>
  );
};

const fmt = (iso) => {
  if (!iso) return "—";
  const d = new Date(iso);
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getDate())}/${p(d.getMonth() + 1)} ${p(d.getHours())}:${p(d.getMinutes())}`;
};

export default RuleStateDialog;
