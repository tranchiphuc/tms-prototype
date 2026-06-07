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
  CheckboxField,
  required,
} from "../../components/FormFields";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";
import { DEVICES } from "../../mock/devices";
import { fetchList } from "../../redux/actions/flinkActions";
import {
  createAlertRule,
  updateAlertRule,
  exportAlertRules,
} from "../../redux/actions/alertActions";
import {
  RULE_KIND_OPTIONS,
  CONDITION_KIND_OPTIONS,
  COMPARATOR_OPTIONS,
  LOGICAL_OP_OPTIONS,
  MISSING_AS_OPTIONS,
  SEVERITY_OPTIONS,
  ENTITY_KEY_OPTIONS,
} from "./alertConstants";
import AlertPreviewDialog from "./AlertPreviewDialog";

const DEVICE_OPTIONS = DEVICES.map((d) => ({ label: `${d.name} (#${d.id})`, value: d.id }));

const AlertRuleDialog = ({ visible, initial, onHide, onSaved }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;
  const metricList = useSelector((s) => s.flink.metricAliases.list);
  const pathList = useSelector((s) => s.flink.pathAliases.list);
  const [basicRules, setBasicRules] = useState([]); // cho child_rule_ids (composite)
  const [previewBody, setPreviewBody] = useState(null);

  useEffect(() => {
    if (!visible) return;
    if (metricList.length === 0) dispatch(fetchList("metricAliases", { page_size: 200, status: "" }));
    if (pathList.length === 0) dispatch(fetchList("pathAliases", { page_size: 200, status: "" }));
    // tải danh sách basic Active để chọn child (không ghi vào store bảng)
    dispatch(exportAlertRules({ rule_kind: 0, status: 1, page_size: 200 }))
      .then((res) => setBasicRules((res.payload.data || {}).items || []))
      .catch(() => setBasicRules([]));
  }, [visible, metricList.length, pathList.length, dispatch]);

  const metricOptions = useMemo(() => {
    const set = new Set(metricList.map((m) => m.alias_metric));
    // bổ sung derived metrics hay dùng (path_alias_id=0)
    ["cpu_util_avg_5m", "if_in_mbps", "if_out_mbps", "if_total_octets"].forEach((m) => set.add(m));
    return Array.from(set).sort().map((m) => ({ label: m, value: m }));
  }, [metricList]);

  const pathOptions = useMemo(
    () =>
      [
        { label: "(mọi path · null)", value: null },
        { label: "(derived metric · 0)", value: 0 },
      ].concat(
        pathList
          .filter((p) => Number(p.status) === 1)
          .map((p) => ({ label: `${p.alias_path} (#${p.id} · ${p.vendor_code})`, value: p.id }))
      ),
    [pathList]
  );

  const childOptions = useMemo(
    () =>
      basicRules
        .filter((r) => !isEdit || r.id !== initial.id)
        .map((r) => ({
          label: `#${r.id} ${r.rule_name} [${(r.entity_keys || []).join(",") || "device"}]`,
          value: r.id,
        })),
    [basicRules, isEdit, initial]
  );

  const prepare = (init) => {
    if (!init)
      return {
        rule_kind: 0,
        condition_kind: 0,
        severity: 1,
        entity_keys: [],
        scope_device_ids: [],
        path_alias_id: null,
        pct_abs: false,
        sustain_samples: 1,
        dedup_seconds: 1800,
        emit_independent: true,
        missing_as: 0,
        logical_op: 0,
        child_rule_ids: [],
        status: 1,
      };
    return {
      ...init,
      entity_keys: init.entity_keys || [],
      scope_device_ids: init.scope_device_ids || [],
      child_rule_ids: init.child_rule_ids || [],
      pct_abs: !!init.pct_abs,
      emit_independent: init.emit_independent == null ? true : !!init.emit_independent,
      entity_filter_text: init.entity_filter ? JSON.stringify(init.entity_filter, null, 2) : "",
    };
  };

  const toBody = (values) => {
    const kind = Number(values.rule_kind);
    const body = {
      rule_name: values.rule_name,
      rule_kind: kind,
      severity: Number(values.severity),
      entity_keys: values.entity_keys || [],
      scope_device_ids:
        values.scope_device_ids && values.scope_device_ids.length ? values.scope_device_ids : null,
      sustain_samples: Number(values.sustain_samples) || 1,
      dedup_seconds: values.dedup_seconds == null ? 0 : Number(values.dedup_seconds),
      status: values.status == null ? 1 : Number(values.status),
    };
    if (kind === 0) {
      const ck = Number(values.condition_kind);
      body.condition_kind = ck;
      body.alias_metric = values.alias_metric;
      body.path_alias_id = values.path_alias_id ?? null;
      body.emit_independent = values.emit_independent ? 1 : 0;
      if (values.entity_filter_text && values.entity_filter_text.trim()) {
        body.entity_filter = JSON.parse(values.entity_filter_text);
      } else {
        body.entity_filter = null;
      }
      if (ck === 2) {
        body.no_data_seconds = Number(values.no_data_seconds);
      } else {
        body.comparator = Number(values.comparator);
        body.threshold = Number(values.threshold);
        if (ck === 1 || ck === 3) body.pct_abs = values.pct_abs ? 1 : 0;
      }
    } else {
      body.logical_op = Number(values.logical_op);
      body.child_rule_ids = values.child_rule_ids || [];
      body.missing_as = Number(values.missing_as) ? 1 : 0;
    }
    return body;
  };

  const onSubmit = (values) => {
    let body;
    try {
      body = toBody(values);
    } catch (e) {
      toast.error("entity_filter không phải JSON hợp lệ");
      return { entity_filter_text: "JSON không hợp lệ" };
    }
    const req = isEdit
      ? dispatch(updateAlertRule(initial.id, body))
      : dispatch(createAlertRule(body));
    return req
      .then(() => {
        toast.success("Lưu alert rule thành công.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { [e.details?.[0]?.field || "rule_name"]: e.message };
      });
  };

  const openPreview = (values) => {
    try {
      setPreviewBody(toBody(values));
    } catch (e) {
      toast.error("entity_filter không phải JSON hợp lệ");
    }
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Alert Rule" : "Thêm Alert Rule"}
      visible={visible}
      style={{ width: 880 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={prepare(initial)}
        render={({ handleSubmit, submitting, values }) => {
          const kind = Number(values.rule_kind);
          const ck = Number(values.condition_kind);
          const needsThreshold = kind === 0 && ck !== 2;
          const isPct = kind === 0 && (ck === 1 || ck === 3);
          return (
            <form onSubmit={handleSubmit}>
              <div style={{ display: "flex", gap: 16 }}>
                {/* Cột trái */}
                <div style={{ flex: 1 }}>
                  <TextField name="rule_name" label="Tên rule" required validate={required()} />
                  <DropdownField
                    name="rule_kind"
                    label="Loại rule (rule_kind)"
                    required
                    options={RULE_KIND_OPTIONS}
                    disabled={isEdit}
                    hint={isEdit ? "Không đổi loại sau khi tạo" : undefined}
                  />
                  <DropdownField name="severity" label="Severity" required options={SEVERITY_OPTIONS} />
                  <MultiSelectField
                    name="entity_keys"
                    label="entity_keys"
                    options={ENTITY_KEY_OPTIONS}
                    placeholder="Để trống = device-level"
                    hint={
                      kind === 1
                        ? "Composite: mọi child phải CÙNG entity_keys này"
                        : "Để trống = device-level; vd if_name"
                    }
                  />

                  {kind === 0 && (
                    <>
                      <DropdownField
                        name="condition_kind"
                        label="condition_kind"
                        required
                        options={CONDITION_KIND_OPTIONS}
                      />
                      <DropdownField
                        name="alias_metric"
                        label="alias_metric"
                        required
                        options={metricOptions}
                        placeholder="Chọn metric..."
                        validate={required()}
                      />
                      <DropdownField
                        name="path_alias_id"
                        label="path_alias_id"
                        options={pathOptions}
                        hint="null=any-path; 0=derived metric (Job 2)"
                      />
                    </>
                  )}

                  {kind === 1 && (
                    <>
                      <DropdownField
                        name="logical_op"
                        label="logical_op"
                        required
                        options={LOGICAL_OP_OPTIONS}
                      />
                      <MultiSelectField
                        name="child_rule_ids"
                        label="child_rule_ids (basic Active cùng entity_keys)"
                        required
                        filter
                        options={childOptions}
                        placeholder="Chọn child rules..."
                        validate={(v) => (!v || v.length === 0 ? "Chọn ≥1 child" : undefined)}
                        hint="Child phải là basic, Active, cùng entity_keys; không lồng composite"
                      />
                      <DropdownField
                        name="missing_as"
                        label="missing_as (child thiếu data)"
                        options={MISSING_AS_OPTIONS}
                      />
                    </>
                  )}
                </div>

                {/* Cột phải */}
                <div style={{ flex: 1 }}>
                  {needsThreshold && (
                    <>
                      <DropdownField
                        name="comparator"
                        label="comparator"
                        required
                        options={COMPARATOR_OPTIONS}
                        validate={required()}
                      />
                      <NumberField
                        name="threshold"
                        label={`threshold${ck === 1 ? " (%)" : ""}`}
                        required
                        validate={required()}
                        maxFractionDigits={6}
                      />
                      {isPct && (
                        <CheckboxField
                          name="pct_abs"
                          label="pct_abs — dùng trị tuyệt đối |.|"
                          hint={ck === 1 ? "|%| thay đổi" : "|cur−prev|"}
                        />
                      )}
                    </>
                  )}

                  {kind === 0 && ck === 2 && (
                    <NumberField
                      name="no_data_seconds"
                      label="no_data_seconds"
                      required
                      validate={required()}
                      hint="> 0. Chỉ FIRED 1 lần/đợt im lặng (không refire chu kỳ — AL-04/AL-15)."
                    />
                  )}

                  <NumberField
                    name="sustain_samples"
                    label="sustain_samples"
                    required
                    validate={(v) => (v == null || Number(v) < 1 ? "≥ 1" : undefined)}
                    hint="Số lần evaluate liên tiếp vi phạm trước khi FIRE"
                  />
                  <NumberField
                    name="dedup_seconds"
                    label="dedup_seconds"
                    hint="≥ 0. Chống refire trong cửa sổ này."
                  />

                  {kind === 0 && (
                    <>
                      <TextAreaField
                        name="entity_filter_text"
                        label="entity_filter (JSON, optional)"
                        rows={2}
                        placeholder='{"if_name": ["Gi0/0/0/0"]}'
                      />
                      <CheckboxField
                        name="emit_independent"
                        label="emit_independent — fire độc lập"
                        hint="Mặc định bật. Tắt nếu chỉ muốn dùng làm child của composite."
                      />
                    </>
                  )}

                  <MultiSelectField
                    name="scope_device_ids"
                    label="scope_device_ids"
                    options={DEVICE_OPTIONS}
                    placeholder="Để trống = mọi thiết bị"
                    hint="Để trống = áp mọi thiết bị"
                  />
                </div>
              </div>

              {kind === 0 && ck === 2 && (
                <div className="warning-box">
                  no_data chỉ fire cho series <b>từng có</b> dữ liệu rồi ngừng báo. Mỗi đợt im lặng
                  FIRED đúng một lần; tự RESOLVED khi series báo lại. comparator/threshold không dùng.
                </div>
              )}
              {(ck === 1 || ck === 3) && kind === 0 && (
                <div className="info-box">
                  "previous" = đúng 1 sample liền trước (cần ≥2 sample ≈ 2 chu kỳ ~120s). prev=0 → skip
                  (tránh chia 0), không resolve nhầm.
                </div>
              )}

              <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8 }}>
                <Button
                  type="button"
                  label="Preview / dry-run (AL-07)"
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

      <AlertPreviewDialog body={previewBody} onHide={() => setPreviewBody(null)} />
    </Dialog>
  );
};

export default AlertRuleDialog;
