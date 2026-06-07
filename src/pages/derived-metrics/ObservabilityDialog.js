import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Dialog } from "primereact/dialog";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Tag } from "primereact/tag";
import { fetchObservability } from "../../redux/actions/derivedActions";
import { DERIVE_KIND } from "./derivedConstants";

// DM-08 — observability Job 2: incomplete + emit counter per rule.
const ObservabilityDialog = ({ visible, onHide }) => {
  const dispatch = useDispatch();
  const obs = useSelector((s) => s.derived.observability);

  useEffect(() => {
    if (visible) dispatch(fetchObservability());
  }, [visible, dispatch]);

  const incompleteBody = (row) => (
    <span style={{ color: row.misconfig_suspect ? "#cf1322" : "#262626", fontWeight: row.misconfig_suspect ? 600 : 400 }}>
      {row.derived_incomplete_total}
      {row.misconfig_suspect && (
        <Tag value="nghi cấu hình sai" severity="danger" style={{ marginLeft: 8, fontSize: 11 }} />
      )}
    </span>
  );

  return (
    <Dialog header="Observability Job 2 (DM-08)" visible={visible} style={{ width: 960 }} onHide={onHide} modal>
      <DataTable
        value={(obs && obs.rules) || []}
        className="rule-table"
        emptyMessage="Không có dữ liệu"
        responsiveLayout="scroll"
        paginator
        rows={10}
      >
        <Column header="Output Metric" field="output_metric" style={{ width: 200 }} />
        <Column header="Tên rule" field="rule_name" style={{ width: 210 }} />
        <Column header="Loại" body={(r) => DERIVE_KIND[r.derive_kind]} style={{ width: 110 }} />
        <Column header="derived_incomplete_total" body={incompleteBody} style={{ width: 260 }} />
        <Column header="emit_total" field="emit_total" style={{ width: 110 }} />
      </DataTable>
      {obs && <p style={{ color: "#8c8c8c", fontSize: 12, marginTop: 8 }}>{obs.note}</p>}
    </Dialog>
  );
};

export default ObservabilityDialog;
