import React, { useEffect, useMemo, useState } from "react";
import { useDispatch } from "react-redux";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import { InputTextarea } from "primereact/inputtextarea";
import { Message } from "primereact/message";
import { previewDerivedRule } from "../../redux/actions/derivedActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";

// Gợi ý sample_inputs theo derive_kind để operator chỉnh nhanh.
const sampleTemplate = (body) => {
  if (!body) return "{}";
  const kind = Number(body.derive_kind);
  const inputs = body.input_metrics || [];
  if (kind === 0) {
    const obj = {};
    inputs.forEach((m, i) => (obj[m] = 1000 * (i + 1)));
    return JSON.stringify(obj, null, 2);
  }
  if (kind === 1) {
    const m = inputs[0] || "input";
    return JSON.stringify({ [m]: [10, 20, 30, 40] }, null, 2);
  }
  // delta
  const m = inputs[0] || "input";
  return JSON.stringify({ [m]: { prev: 1000000, curr: 1500000, dt: 60 } }, null, 2);
};

const DerivedPreviewDialog = ({ body, onHide }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const [sampleText, setSampleText] = useState("{}");
  const [result, setResult] = useState(null);

  const visible = !!body;
  const tpl = useMemo(() => sampleTemplate(body), [body]);

  useEffect(() => {
    if (visible) {
      setSampleText(tpl);
      setResult(null);
    }
  }, [visible, tpl]);

  const run = () => {
    let sample_inputs;
    try {
      sample_inputs = JSON.parse(sampleText || "{}");
    } catch (e) {
      toast.error("sample_inputs không phải JSON hợp lệ");
      return;
    }
    dispatch(previewDerivedRule({ ...body, sample_inputs }))
      .then((res) => setResult(res.payload.data))
      .catch((rej) => toast.error(errInfo(rej).message));
  };

  return (
    <Dialog
      header="Preview Derived (DM-06)"
      visible={visible}
      style={{ width: 620 }}
      onHide={onHide}
      modal
    >
      {body && (
        <>
          <div className="field-row">
            <label>sample_inputs (JSON)</label>
            <InputTextarea
              value={sampleText}
              onChange={(e) => setSampleText(e.target.value)}
              rows={6}
              autoResize
              style={{ width: "100%", fontFamily: "monospace", fontSize: 12 }}
            />
            <span className="field-hint">
              Computed: {`{alias_metric: value}`}. Aggregated: {`{input: [mảng giá trị]}`}. Delta:{" "}
              {`{input: {prev, curr, dt}}`}.
            </span>
          </div>

          <Button label="Tính preview" icon="pi pi-play" className="p-button-sm" onClick={run} />

          {result && (
            <div style={{ marginTop: 16 }}>
              <div
                style={{
                  background: "#fafafa",
                  border: "1px solid #f0f0f0",
                  borderRadius: 4,
                  padding: 12,
                }}
              >
                <div style={{ fontSize: 13 }}>
                  <strong>{result.output_metric}</strong> ={" "}
                  <span style={{ color: "#1890ff", fontWeight: 600 }}>
                    {result.result == null ? "—" : result.result}
                  </span>{" "}
                  {result.unit}
                </div>
              </div>
              {(result.warnings || []).map((w, i) => (
                <Message key={`w${i}`} severity="warn" text={w} style={{ display: "block", marginTop: 8 }} />
              ))}
              {(result.errors || []).map((e, i) => (
                <Message key={`e${i}`} severity="error" text={e} style={{ display: "block", marginTop: 8 }} />
              ))}
              <p style={{ color: "#8c8c8c", fontSize: 12, marginTop: 8 }}>{result.note}</p>
            </div>
          )}
        </>
      )}
    </Dialog>
  );
};

export default DerivedPreviewDialog;
