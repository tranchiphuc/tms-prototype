import React, { useEffect, useMemo, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Form } from "react-final-form";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import {
  TextField,
  TextAreaField,
  NumberField,
  DropdownField,
  MultiSelectField,
  required,
} from "../../components/FormFields";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { DEVICES } from "../../mock/devices";
import { fetchList } from "../../redux/actions/flinkActions";
import {
  createDerivedRule,
  updateDerivedRule,
} from "../../redux/actions/derivedActions";
import {
  DERIVE_KIND_OPTIONS,
  AGG_FUNCTION_OPTIONS,
} from "./derivedConstants";
import DerivedPreviewDialog from "./DerivedPreviewDialog";
import MetricLookupDialog from "./MetricLookupDialog";

const DEVICE_OPTIONS = DEVICES.map((d) => ({ label: `${d.name} (#${d.id})`, value: d.id }));

const DerivedRuleDialog = ({ visible, initial, onHide, onSaved }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;
  const metricList = useSelector((s) => s.flink.metricAliases.list);
  const pathList = useSelector((s) => s.flink.pathAliases.list);
  const [previewBody, setPreviewBody] = useState(null);
  const [lookupOpen, setLookupOpen] = useState(false);

  useEffect(() => {
    if (!visible) return;
    if (metricList.length === 0) dispatch(fetchList("metricAliases", { page_size: 200, status: "" }));
    if (pathList.length === 0) dispatch(fetchList("pathAliases", { page_size: 200, status: "" }));
  }, [visible, metricList.length, pathList.length, dispatch]);

  // alias_metric distinct → options cho input_metrics
  const metricOptions = useMemo(() => {
    const set = new Set(metricList.map((m) => m.alias_metric));
    return Array.from(set)
      .sort()
      .map((m) => ({ label: m, value: m }));
  }, [metricList]);

  const pathOptions = useMemo(
    () =>
      [{ label: "(mọi path · null)", value: null }].concat(
        pathList
          .filter((p) => Number(p.status) === 1)
          .map((p) => ({ label: `${p.alias_path} (#${p.id} · ${p.vendor_code})`, value: p.id }))
      ),
    [pathList]
  );

  const prepare = (init) => {
    if (!init)
      return {
        derive_kind: 0,
        input_metrics: [],
        scope_path_alias_id: null,
        scope_device_ids: [],
        priority: 100,
        status: 1,
        assembly_window_seconds: 90,
        delta_scale_factor: 1,
      };
    return {
      ...init,
      scope_device_ids: init.scope_device_ids || [],
      static_constants_text: init.static_constants
        ? JSON.stringify(init.static_constants, null, 2)
        : "",
    };
  };

  // Gom values → body API (chuẩn hoá theo derive_kind)
  const toBody = (values) => {
    const kind = Number(values.derive_kind);
    const body = {
      rule_name: values.rule_name,
      output_metric: values.output_metric,
      output_unit: values.output_unit || "",
      derive_kind: kind,
      input_metrics: values.input_metrics || [],
      scope_path_alias_id: values.scope_path_alias_id ?? null,
      scope_device_ids:
        values.scope_device_ids && values.scope_device_ids.length ? values.scope_device_ids : null,
      priority: Number(values.priority) || 100,
      status: values.status == null ? 1 : Number(values.status),
    };
    if (kind === 0) {
      body.expression = values.expression;
      body.assembly_window_seconds =
        values.assembly_window_seconds == null ? null : Number(values.assembly_window_seconds);
      if (values.static_constants_text && values.static_constants_text.trim()) {
        body.static_constants = JSON.parse(values.static_constants_text);
      }
    } else if (kind === 1) {
      body.window_seconds = Number(values.window_seconds);
      body.agg_function = Number(values.agg_function);
    } else if (kind === 2) {
      body.delta_scale_factor =
        values.delta_scale_factor == null ? 1 : Number(values.delta_scale_factor);
      body.delta_reset_threshold =
        values.delta_reset_threshold == null || values.delta_reset_threshold === ""
          ? null
          : Number(values.delta_reset_threshold);
    }
    return body;
  };

  const onSubmit = (values) => {
    let body;
    try {
      body = toBody(values);
    } catch (e) {
      toast.error("static_constants không phải JSON hợp lệ");
      return { static_constants_text: "JSON không hợp lệ" };
    }
    const req = isEdit
      ? dispatch(updateDerivedRule(initial.id, body))
      : dispatch(createDerivedRule(body));
    return req
      .then(() => {
        toast.success("Lưu derived rule thành công.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { [e.details?.[0]?.field || "output_metric"]: e.message };
      });
  };

  const openPreview = (values) => {
    try {
      setPreviewBody(toBody(values));
    } catch (e) {
      toast.error("static_constants không phải JSON hợp lệ");
    }
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Derived Rule" : "Thêm Derived Rule"}
      visible={visible}
      style={{ width: 820 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={prepare(initial)}
        render={({ handleSubmit, submitting, values }) => {
          const kind = Number(values.derive_kind);
          const deviceCount = (values.scope_device_ids || []).length;
          return (
            <form onSubmit={handleSubmit}>
              <div style={{ display: "flex", gap: 16 }}>
                {/* Cột trái — chung */}
                <div style={{ flex: 1 }}>
                  <TextField name="rule_name" label="Tên rule" required validate={required()} />
                  <TextField
                    name="output_metric"
                    label="Output Metric"
                    required
                    validate={required()}
                    placeholder="vd: memory_utilization_pct"
                    hint="Unique trong active; KHÔNG trùng alias_metric nào trong tlm_metric_aliases"
                  />
                  <TextField name="output_unit" label="Output Unit" placeholder="%, By, Mbps..." />
                  <DropdownField
                    name="derive_kind"
                    label="Loại (derive_kind)"
                    required
                    options={DERIVE_KIND_OPTIONS}
                    validate={required()}
                    disabled={isEdit}
                    hint={isEdit ? "Không đổi loại sau khi tạo" : undefined}
                  />
                  <div style={{ display: "flex", alignItems: "flex-end", gap: 8 }}>
                    <div style={{ flex: 1 }}>
                      <MultiSelectField
                        name="input_metrics"
                        label="Input metrics (alias_metric)"
                        required
                        options={metricOptions}
                        filter
                        placeholder="Chọn alias_metric..."
                        validate={(v) => (!v || v.length === 0 ? "Chọn ≥1 input" : undefined)}
                        hint={
                          kind === 0
                            ? "Computed: ≥1 input, đôi một khác nhau"
                            : "Aggregated/Delta: đúng 1 input"
                        }
                      />
                    </div>
                    <Button
                      type="button"
                      icon="pi pi-search"
                      className="p-button-outlined p-button-sm"
                      tooltip="Lookup alias trải trên nhiều path (DM-07)"
                      tooltipOptions={{ position: "top" }}
                      style={{ marginBottom: 18 }}
                      onClick={() => setLookupOpen(true)}
                    />
                  </div>
                </div>

                {/* Cột phải — theo loại + scope */}
                <div style={{ flex: 1 }}>
                  {kind === 0 && (
                    <>
                      <TextField
                        name="expression"
                        label="Expression"
                        required
                        validate={required()}
                        placeholder="memory_used_bytes / (memory_used_bytes + memory_free_bytes) * 100"
                        hint="Biến = alias_metric của input (và key của static_constants)"
                      />
                      <NumberField
                        name="assembly_window_seconds"
                        label="assembly_window_seconds"
                        hint="Cửa sổ chờ gom đủ input (~1.5× sample ≈ 90s). State TTL đặt > giá trị này."
                      />
                      <TextAreaField
                        name="static_constants_text"
                        label="static_constants (JSON Map<alias_metric, Double>)"
                        rows={3}
                        placeholder='{"link_capacity_bps": 10000000000}'
                      />
                    </>
                  )}
                  {kind === 1 && (
                    <>
                      <NumberField
                        name="window_seconds"
                        label="window_seconds"
                        required
                        validate={required()}
                        hint="Tumbling window; độ trễ ≈ window × 1.25"
                      />
                      <DropdownField
                        name="agg_function"
                        label="agg_function"
                        required
                        options={AGG_FUNCTION_OPTIONS}
                        validate={required()}
                      />
                    </>
                  )}
                  {kind === 2 && (
                    <>
                      <NumberField
                        name="delta_scale_factor"
                        label="delta_scale_factor"
                        maxFractionDigits={12}
                        hint="vd 8/1_000_000 = 0.000008 (bytes → Mbps). Công thức: (curr−prev)×scale / Δt"
                      />
                      <NumberField
                        name="delta_reset_threshold"
                        label="delta_reset_threshold"
                        hint="Null = không xử lý counter reset. vd 4294967295 cho counter32."
                      />
                    </>
                  )}

                  <DropdownField
                    name="scope_path_alias_id"
                    label="scope_path_alias_id"
                    options={pathOptions}
                    hint="Null = mọi path. Đặt path cụ thể để khử nhập nhằng input matching (DM-05)."
                  />
                  <MultiSelectField
                    name="scope_device_ids"
                    label={`scope_device_ids (${deviceCount}/20 thiết bị)`}
                    options={DEVICE_OPTIONS}
                    placeholder="Để trống = áp tất cả thiết bị"
                    validate={(v) => (v && v.length > 20 ? "Tối đa 20 thiết bị" : undefined)}
                    hint="Để trống = áp tất cả thiết bị (tối đa 20)"
                  />
                  <NumberField name="priority" label="priority" />
                </div>
              </div>

              {kind === 0 && (values.input_metrics || []).length < 1 && (
                <div className="warning-box">
                  Computed cần ≥1 input. Input vắng mặt sẽ thay bằng static_constants (nếu có), nếu
                  không thì bỏ qua chu kỳ (đếm vào derived_incomplete_total — xem Observability DM-08).
                </div>
              )}

              <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8 }}>
                <Button
                  type="button"
                  label="Preview (DM-06)"
                  icon="pi pi-eye"
                  className="p-button-outlined p-button-sm"
                  onClick={() => openPreview(values)}
                />
                <div>
                  <Button label="Hủy" className="p-button-text" type="button" onClick={onHide} />
                  <Button label="Lưu" icon="pi pi-check" type="submit" loading={submitting} />
                </div>
              </div>
            </form>
          );
        }}
      />

      <DerivedPreviewDialog body={previewBody} onHide={() => setPreviewBody(null)} />
      <MetricLookupDialog
        visible={lookupOpen}
        onHide={() => setLookupOpen(false)}
        pathList={pathList}
      />
    </Dialog>
  );
};

export default DerivedRuleDialog;
