import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Form } from "react-final-form";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import {
  TextField,
  TextAreaField,
  NumberField,
  DropdownField,
  required,
} from "../../../components/FormFields";
import { MODEL_CODE_VALUES } from "../../../mock/devices";
import { createRule, updateRule, fetchList } from "../../../redux/actions/flinkActions";
import { useToast } from "../../../components/ToastProvider";
import { errInfo } from "../../../utils/apiError";

const MODEL_OPTIONS = MODEL_CODE_VALUES.map((v) => ({ label: v, value: v }));
const VALUE_TYPE_OPTIONS = [
  { label: "(Không override)", value: null },
  { label: "1 - number", value: 1 },
  { label: "2 - string", value: 2 },
  { label: "3 - bool", value: 3 },
];
const TRANSFORM_OPTIONS = [
  { label: "0 - identity", value: 0 },
  { label: "1 - linear (scale·x + offset)", value: 1 },
  { label: "2 - expression", value: 2 },
  { label: "3 - enum_mapping", value: 3 },
];

const MetricAliasDialog = ({ visible, initial, onHide, onSaved }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;
  const pathList = useSelector((s) => s.flink.pathAliases.list);

  useEffect(() => {
    if (visible && pathList.length === 0) {
      dispatch(fetchList("pathAliases", { page_size: 200, status: "" }));
    }
  }, [visible, pathList.length, dispatch]);

  const pathOptions = [{ label: "(any-path · null)", value: null }].concat(
    pathList
      .filter((p) => Number(p.status) === 1)
      .map((p) => ({ label: `${p.alias_path} (#${p.id} · ${p.vendor_code})`, value: p.id }))
  );

  const prepare = (initVals) => {
    if (!initVals) return { vendor_code: "Cisco", path_alias_id: null, transform_kind: 0, value_type_override: null, scale_factor: 1, offset_value: 0, priority: 100 };
    return {
      ...initVals,
      enum_mapping_text: initVals.enum_mapping ? JSON.stringify(initVals.enum_mapping, null, 2) : "",
    };
  };

  const onSubmit = (values) => {
    const tk = Number(values.transform_kind);
    let enum_mapping = null;
    if (tk === 3) {
      try {
        enum_mapping = JSON.parse(values.enum_mapping_text || "{}");
      } catch (e) {
        toast.error("enum_mapping không phải JSON hợp lệ");
        return { enum_mapping_text: "JSON không hợp lệ" };
      }
    }
    const body = {
      vendor_code: values.vendor_code,
      path_alias_id: values.path_alias_id ?? null,
      original_name: values.original_name,
      alias_metric: values.alias_metric,
      value_type_override: values.value_type_override ?? null,
      source_unit: values.source_unit || null,
      target_unit: values.target_unit || null,
      transform_kind: tk,
      scale_factor: values.scale_factor == null ? 1 : Number(values.scale_factor),
      offset_value: values.offset_value == null ? 0 : Number(values.offset_value),
      transform_expression: tk === 2 ? values.transform_expression : null,
      enum_mapping,
      priority: Number(values.priority) || 50,
    };
    const req = isEdit
      ? dispatch(updateRule("metricAliases", initial.id, body))
      : dispatch(createRule("metricAliases", body));
    return req
      .then(() => {
        toast.success("Lưu Metric Alias thành công.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { [e.details?.[0]?.field || "alias_metric"]: e.message };
      });
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Metric Alias" : "Thêm Metric Alias"}
      visible={visible}
      style={{ width: 720 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={prepare(initial)}
        render={({ handleSubmit, submitting, values }) => {
          const tk = Number(values.transform_kind);
          const isStrBool = values.value_type_override === 2 || values.value_type_override === 3;
          return (
            <form onSubmit={handleSubmit}>
              <div className="p-grid" style={{ display: "flex", gap: 16 }}>
                <div style={{ flex: 1 }}>
                  <DropdownField name="vendor_code" label="Model Code" required options={MODEL_OPTIONS} validate={required()} />
                  <DropdownField
                    name="path_alias_id"
                    label="Path scope"
                    options={pathOptions}
                    hint="null = any-path (priority 50); chọn path = path-scoped (priority 100)"
                  />
                  <TextField name="original_name" label="Original Name (leaf)" required validate={required()} />
                  <TextField name="alias_metric" label="Alias Metric" required validate={required()} placeholder="vd: node_free_memory" />
                  <NumberField name="priority" label="Priority" hint="path=100, any-path=50, OpenConfig=30, All=10" />
                </div>
                <div style={{ flex: 1 }}>
                  <DropdownField name="value_type_override" label="value_type_override" options={VALUE_TYPE_OPTIONS} />
                  <DropdownField name="transform_kind" label="transform_kind" required options={TRANSFORM_OPTIONS} validate={required()} />
                  {!isStrBool && (
                    <>
                      <TextField name="source_unit" label="source_unit" placeholder="By" />
                      <TextField name="target_unit" label="target_unit" placeholder="MBy" />
                    </>
                  )}
                  {tk === 1 && (
                    <>
                      <NumberField name="scale_factor" label="scale_factor" maxFractionDigits={12} />
                      <NumberField name="offset_value" label="offset_value" maxFractionDigits={6} />
                    </>
                  )}
                  {tk === 2 && (
                    <TextField name="transform_expression" label="transform_expression" required validate={required()} placeholder="x / 1000000" />
                  )}
                  {tk === 3 && (
                    <TextAreaField name="enum_mapping_text" label="enum_mapping (JSON)" rows={4} placeholder='{"0":"false","6":"true"}' />
                  )}
                </div>
              </div>

              {isStrBool && (
                <div className="warning-box">
                  value_type ∈ {"{string, bool}"}: source_unit/target_unit phải rỗng, scale_factor=1,
                  offset_value=0, transform_kind ∈ {"{0,3}"} (validate 422).
                </div>
              )}

              <div style={{ textAlign: "right", marginTop: 8 }}>
                <Button label="Hủy" className="p-button-text" type="button" onClick={onHide} />
                <Button label="Lưu" icon="pi pi-check" type="submit" loading={submitting} />
              </div>
            </form>
          );
        }}
      />
    </Dialog>
  );
};

export default MetricAliasDialog;
